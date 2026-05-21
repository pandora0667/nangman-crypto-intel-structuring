use crate::config::ProcessingConfig;
use crate::error::{AppError, AppResult};
use crate::hash::{sha256_prefixed, stable_short_id};
use crate::market::reader::MarketL1Reader;
use crate::models::constants::{CONTEXT_FLAG_SCHEMA_VERSION, STRUCTURED_PACKET_SCHEMA_VERSION};
use crate::models::output::{
    IntelL1IndexPointer, OutputObjectRef, PacketRevisionIndex, QuarantineEvent, S3ObjectPointer,
    StructuredPointer,
};
use crate::models::raw::{RawIntelEvent, RawIntelEventCreatedPointer};
use crate::nats::consumer::RawIntelMessage;
use crate::nats::publisher::StructuredPublisher;
use crate::observability::{ProcessingMetric, emit_processing_metric};
use crate::storage::object_store::ObjectStore;
use crate::structuring::packet::{ManifestBuildInput, build_manifest, build_packet_set};
use crate::structuring::router::{ModelRouter, force_rule_evidence_floor};
use crate::structuring::story::StoryMergeManager;
use crate::structuring::validation::{redact_forbidden_output_terms, validate_no_forbidden_output};
use crate::time::{now_ms, run_id};
use crate::workflow::keys;

pub struct IntelStructuringProcessor<P>
where
    P: crate::ai::contract::ModelProvider,
{
    rustfs_store: ObjectStore,
    output_store: ObjectStore,
    market_reader: MarketL1Reader,
    router: ModelRouter<P>,
    publisher: StructuredPublisher,
    config: ProcessingConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckDecision {
    Ack,
    DoNotAck,
}

impl AckDecision {
    pub fn should_ack(self) -> bool {
        matches!(self, Self::Ack)
    }
}

impl<P> IntelStructuringProcessor<P>
where
    P: crate::ai::contract::ModelProvider,
{
    pub fn new(
        rustfs_store: ObjectStore,
        output_store: ObjectStore,
        market_reader: MarketL1Reader,
        router: ModelRouter<P>,
        publisher: StructuredPublisher,
        config: ProcessingConfig,
    ) -> Self {
        Self {
            rustfs_store,
            output_store,
            market_reader,
            router,
            publisher,
            config,
        }
    }

    pub async fn process_nats_message(&self, message: &RawIntelMessage) -> AppResult<AckDecision> {
        let pointer = match RawIntelEventCreatedPointer::parse(message.payload()) {
            Ok(pointer) => pointer,
            Err(error) => {
                self.write_quarantine(None, "invalid_pointer", false, error.to_string())
                    .await?;
                return Ok(AckDecision::Ack);
            }
        };

        if self
            .output_store
            .object_exists(&keys::index_key(
                &pointer.event_id,
                &self.config.structuring_policy_version,
            ))
            .await?
        {
            return Ok(AckDecision::Ack);
        }

        match self.process_pointer(pointer.clone()).await {
            Ok(()) => Ok(AckDecision::Ack),
            Err(error) if is_permanent_failure(&error) => {
                self.write_quarantine(
                    Some(pointer.event_id.as_str()),
                    "permanent_input_failure",
                    false,
                    error.to_string(),
                )
                .await?;
                Ok(AckDecision::Ack)
            }
            Err(error) => {
                eprintln!(
                    "{{\"level\":\"error\",\"raw_event_id\":\"{}\",\"ack\":\"no\",\"error\":{}}}",
                    pointer.event_id,
                    serde_json::to_string(&error.to_string())?
                );
                Ok(AckDecision::DoNotAck)
            }
        }
    }

    async fn process_pointer(&self, pointer: RawIntelEventCreatedPointer) -> AppResult<()> {
        if pointer.storage_ref.bucket != self.rustfs_store.bucket() {
            return Err(AppError::validation(format!(
                "raw pointer bucket mismatch pointer={} configured={}",
                pointer.storage_ref.bucket,
                self.rustfs_store.bucket()
            )));
        }
        let raw_bytes = self
            .rustfs_store
            .get_byte_range(
                &pointer.storage_ref.key,
                pointer.storage_ref.byte_offset,
                pointer.storage_ref.byte_length,
            )
            .await?;
        let raw_event = RawIntelEvent::parse_verified(&raw_bytes, &pointer)?;
        let market_context = self
            .market_reader
            .context_for(
                raw_event.published_at_ms,
                raw_event.fetched_at_ms,
                &raw_event.symbol_candidates,
            )
            .await;
        let mut decision = self.router.decide(&raw_event, &market_context).await?;
        force_rule_evidence_floor(&raw_event, &mut decision);
        let observed_at_ms = raw_event.observed_at_ms;
        let mut packet_set = build_packet_set(
            &raw_event,
            &decision,
            market_context,
            &self.config.structuring_policy_version,
            observed_at_ms,
            self.config.market_context_retry_interval_ms,
            self.config.market_context_expire_after_ms,
        );
        let story_merge = StoryMergeManager::new(
            self.output_store.clone(),
            self.config.story_member_scan_limit,
        )
        .merge_current_event(
            &raw_event,
            &mut packet_set,
            &self.config.structuring_policy_version,
            observed_at_ms,
        )
        .await?;
        validate_no_forbidden_output(&packet_set.story_cluster)?;
        validate_no_forbidden_output(&packet_set.structured_packet)?;
        if let Some(context_flag_packet) = &packet_set.context_flag_packet {
            validate_no_forbidden_output(context_flag_packet)?;
        }

        let run_id = run_id("intel-l1", observed_at_ms);
        let structured_key = keys::structured_packet_key(
            observed_at_ms,
            &raw_event.event_id,
            &packet_set.structured_packet.packet_id,
        );
        let flag_key = packet_set
            .context_flag_packet
            .as_ref()
            .map(|context_flag_packet| {
                keys::context_flag_key(
                    observed_at_ms,
                    &raw_event.event_id,
                    &context_flag_packet.flag_packet_id,
                )
            });
        let story_key = keys::story_cluster_key(
            observed_at_ms,
            &raw_event.event_id,
            &packet_set.story_cluster.cluster_id,
        );
        let health_key = keys::health_key(
            observed_at_ms,
            &raw_event.event_id,
            &packet_set.health_event.health_event_id,
        );

        self.output_store
            .put_bytes_idempotent(
                &story_merge.story_member_key,
                story_merge.story_member_bytes.clone(),
                "application/json",
            )
            .await?;
        let story_bytes = self
            .output_store
            .put_jsonl_idempotent(&story_key, std::slice::from_ref(&packet_set.story_cluster))
            .await?;
        let structured_bytes = self
            .output_store
            .put_jsonl_idempotent(
                &structured_key,
                std::slice::from_ref(&packet_set.structured_packet),
            )
            .await?;
        let revision_index = PacketRevisionIndex {
            schema_version: PacketRevisionIndex::schema(),
            packet_family_id: packet_set.structured_packet.packet_family_id.clone(),
            raw_event_id: raw_event.event_id.clone(),
            latest_revision: packet_set.structured_packet.revision,
            latest_packet_id: packet_set.structured_packet.packet_id.clone(),
            latest_structured_key: structured_key.clone(),
            market_context_status: packet_set.structured_packet.market_context_status.clone(),
            updated_at_ms: observed_at_ms,
        };
        self.output_store
            .put_json_idempotent(
                &keys::packet_revision_index_key(
                    &packet_set.structured_packet.packet_family_id,
                    packet_set.structured_packet.revision,
                ),
                &revision_index,
            )
            .await?;
        let flag_bytes = if let (Some(flag_key), Some(context_flag_packet)) =
            (&flag_key, &packet_set.context_flag_packet)
        {
            Some(
                self.output_store
                    .put_jsonl_idempotent(flag_key, std::slice::from_ref(context_flag_packet))
                    .await?,
            )
        } else {
            None
        };
        let health_bytes = self
            .output_store
            .put_jsonl_idempotent(&health_key, std::slice::from_ref(&packet_set.health_event))
            .await?;

        let mut output_objects = vec![
            object_ref(
                "story_member",
                &story_merge.story_member_key,
                1,
                &story_merge.story_member_bytes,
            ),
            object_ref("story_cluster", &story_key, 1, &story_bytes),
            object_ref(
                "structured_intel_packet",
                &structured_key,
                1,
                &structured_bytes,
            ),
            object_ref("structuring_health_event", &health_key, 1, &health_bytes),
        ];
        if let (Some(flag_key), Some(flag_bytes)) = (&flag_key, &flag_bytes) {
            output_objects.push(object_ref("context_flag_packet", flag_key, 1, flag_bytes));
        }
        let finished_at_ms = observed_at_ms;
        let manifest = build_manifest(
            ManifestBuildInput {
                run_id: run_id.clone(),
                raw_event_id: raw_event.event_id.clone(),
                status: "success".to_owned(),
                started_at_ms: observed_at_ms,
                finished_at_ms,
                policy_version: self.config.structuring_policy_version.clone(),
                output_objects,
            },
            &packet_set,
        );
        let manifest_key = keys::manifest_key(observed_at_ms, &raw_event.event_id, &run_id);
        let manifest_bytes = self
            .output_store
            .put_json_idempotent(&manifest_key, &manifest)
            .await?;
        let index_input = IndexBuildInput {
            packet_id: &packet_set.structured_packet.packet_id,
            raw_event_id: &raw_event.event_id,
            manifest_key: &manifest_key,
            structured_key: &structured_key,
            flag_key: flag_key.as_deref(),
            finished_at_ms,
            policy_version: &self.config.structuring_policy_version,
        };
        let prepared_index = build_index("prepared", &index_input);
        self.output_store
            .put_json_idempotent(
                &keys::prepared_index_key(
                    &raw_event.event_id,
                    &self.config.structuring_policy_version,
                ),
                &prepared_index,
            )
            .await?;

        let structured_pointer = StructuredPointer {
            schema_version: STRUCTURED_PACKET_SCHEMA_VERSION.to_owned(),
            packet_id: packet_set.structured_packet.packet_id.clone(),
            raw_event_id: raw_event.event_id.clone(),
            terminal_decision: packet_set.structured_packet.terminal_decision.clone(),
            storage_ref: S3ObjectPointer {
                bucket: self.output_store.bucket().to_owned(),
                key: structured_key.clone(),
                content_sha256: sha256_prefixed(&structured_bytes),
                schema_version: STRUCTURED_PACKET_SCHEMA_VERSION.to_owned(),
            },
            manifest_key: manifest_key.clone(),
            created_at_ms: finished_at_ms,
        };
        let flag_pointer = if let (Some(context_flag_packet), Some(flag_key), Some(flag_bytes)) =
            (&packet_set.context_flag_packet, &flag_key, &flag_bytes)
        {
            Some(StructuredPointer {
                schema_version: CONTEXT_FLAG_SCHEMA_VERSION.to_owned(),
                packet_id: context_flag_packet.flag_packet_id.clone(),
                raw_event_id: raw_event.event_id.clone(),
                terminal_decision: packet_set.structured_packet.terminal_decision.clone(),
                storage_ref: S3ObjectPointer {
                    bucket: self.output_store.bucket().to_owned(),
                    key: flag_key.clone(),
                    content_sha256: sha256_prefixed(flag_bytes),
                    schema_version: CONTEXT_FLAG_SCHEMA_VERSION.to_owned(),
                },
                manifest_key: manifest_key.clone(),
                created_at_ms: finished_at_ms,
            })
        } else {
            None
        };

        self.publisher
            .publish_structured_pointer(&packet_set.structured_packet, &structured_pointer)
            .await?;
        if let (Some(context_flag_packet), Some(flag_pointer)) =
            (&packet_set.context_flag_packet, &flag_pointer)
        {
            self.publisher
                .publish_context_flag_pointer(context_flag_packet, flag_pointer)
                .await?;
        }
        self.publisher
            .publish_health(&packet_set.health_event)
            .await?;
        self.publisher.flush().await?;

        let index = build_index("success", &index_input);
        self.output_store
            .put_json_idempotent(
                &keys::index_key(&raw_event.event_id, &self.config.structuring_policy_version),
                &index,
            )
            .await?;

        emit_processing_metric(&ProcessingMetric {
            raw_event_id: raw_event.event_id.clone(),
            packet_id: packet_set.structured_packet.packet_id.clone(),
            model_tier_used: packet_set.structured_packet.model_tier_used.clone(),
            terminal_decision: packet_set.structured_packet.terminal_decision.clone(),
            market_context_status: packet_set.structured_packet.market_context_status.clone(),
            ack_ready: true,
            fallback_count: packet_set.health_event.fallback_count,
            conflict_count: packet_set.structured_packet.contradiction_flags.len(),
            haiku_invocation_count: packet_set.health_event.model_l0_invocations,
            sonnet_invocation_count: packet_set.health_event.model_l1_invocations,
            numeric_snapshot_count: usize::from(is_numeric_market_snapshot(&raw_event)),
            stale_market_context_count: usize::from(
                packet_set
                    .structured_packet
                    .market_context_status
                    .is_stale_but_usable(),
            ),
            sonnet_on_numeric_snapshot_count: usize::from(
                is_numeric_market_snapshot(&raw_event)
                    && packet_set.health_event.model_l1_invocations > 0,
            ),
        })?;
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "raw_event_id": raw_event.event_id,
                "packet_id": packet_set.structured_packet.packet_id,
                "terminal_decision": packet_set.structured_packet.terminal_decision,
                "model_tier_used": packet_set.structured_packet.model_tier_used,
                "market_context_status": packet_set.structured_packet.market_context_status,
                "evidence_quality_reasons": packet_set.structured_packet.evidence_quality_reasons,
                "haiku_invocations": packet_set.health_event.model_l0_invocations,
                "sonnet_invocations": packet_set.health_event.model_l1_invocations,
                "manifest_sha256": sha256_prefixed(&manifest_bytes),
                "ack_ready": true
            }))?
        );
        Ok(())
    }

    async fn write_quarantine(
        &self,
        raw_event_id: Option<&str>,
        failure_class: &str,
        retryable: bool,
        reason: String,
    ) -> AppResult<()> {
        let observed_at_ms = now_ms();
        let sanitized_reason = redact_forbidden_output_terms(&reason);
        let quarantine_id = stable_short_id(
            "intel_l1_quarantine",
            &[
                raw_event_id.unwrap_or("unknown"),
                failure_class,
                &sanitized_reason,
            ],
        );
        let event = QuarantineEvent::new(
            quarantine_id.clone(),
            raw_event_id.map(ToOwned::to_owned),
            observed_at_ms,
            failure_class,
            retryable,
            sanitized_reason,
        );
        validate_no_forbidden_output(&event)?;
        self.output_store
            .put_json_idempotent(
                &keys::quarantine_key(observed_at_ms, raw_event_id, &quarantine_id),
                &event,
            )
            .await?;
        Ok(())
    }
}

fn object_ref(family: &str, key: &str, record_count: usize, bytes: &[u8]) -> OutputObjectRef {
    OutputObjectRef {
        object_family: family.to_owned(),
        key: key.to_owned(),
        record_count,
        byte_count: bytes.len(),
    }
}

struct IndexBuildInput<'a> {
    packet_id: &'a str,
    raw_event_id: &'a str,
    manifest_key: &'a str,
    structured_key: &'a str,
    flag_key: Option<&'a str>,
    finished_at_ms: i64,
    policy_version: &'a str,
}

fn build_index(status: &str, input: &IndexBuildInput<'_>) -> IntelL1IndexPointer {
    IntelL1IndexPointer {
        schema_version: crate::models::constants::INDEX_POINTER_SCHEMA_VERSION.to_owned(),
        packet_id: input.packet_id.to_owned(),
        raw_event_id: input.raw_event_id.to_owned(),
        status: status.to_owned(),
        manifest_key: input.manifest_key.to_owned(),
        structured_packet_keys: vec![input.structured_key.to_owned()],
        context_flag_keys: input
            .flag_key
            .map(|key| vec![key.to_owned()])
            .unwrap_or_default(),
        finished_at_ms: input.finished_at_ms,
        structuring_policy_version: input.policy_version.to_owned(),
    }
}

fn is_permanent_failure(error: &AppError) -> bool {
    matches!(error, AppError::Validation(_))
}

fn is_numeric_market_snapshot(event: &RawIntelEvent) -> bool {
    event.source_quality_or_unknown() == "market_snapshot"
        || event.content_quality_or_unknown() == "numeric_observation"
}
