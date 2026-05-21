use crate::config::ProcessingConfig;
use crate::error::AppResult;
use crate::hash::sha256_prefixed;
use crate::market::reader::MarketL1Reader;
use crate::models::constants::{MANIFEST_SCHEMA_VERSION, STRUCTURED_PACKET_SCHEMA_VERSION};
use crate::models::market::{MarketContextSnapshot, MarketContextStatus};
use crate::models::output::{
    IntelL1Manifest, OutputObjectRef, PacketRevisionIndex, S3ObjectPointer, StructuredIntelPacket,
    StructuredPointer,
};
use crate::nats::publisher::StructuredPublisher;
use crate::storage::object_store::ObjectStore;
use crate::structuring::packet::{market_context_ref, revised_packet_id};
use crate::time::{now_ms, run_id};
use crate::workflow::keys;

const STRUCTURED_PACKET_PREFIX: &str = "structured-intel-packet/schema=structured_intel_packet_v1/";
const REVISION_INDEX_MAX_KEYS: usize = 256;

pub struct PendingMarketContextRehydrator {
    output_store: ObjectStore,
    market_reader: MarketL1Reader,
    publisher: StructuredPublisher,
    config: ProcessingConfig,
}

impl PendingMarketContextRehydrator {
    pub fn new(
        output_store: ObjectStore,
        market_reader: MarketL1Reader,
        publisher: StructuredPublisher,
        config: ProcessingConfig,
    ) -> Self {
        Self {
            output_store,
            market_reader,
            publisher,
            config,
        }
    }

    pub async fn run_once(&self, max_packets: usize) -> AppResult<usize> {
        let keys = self
            .output_store
            .list_keys(STRUCTURED_PACKET_PREFIX, max_packets)
            .await?;
        let mut published = 0usize;
        for key in keys {
            if self.try_rehydrate_key(&key).await? {
                published += 1;
            }
        }
        Ok(published)
    }

    async fn try_rehydrate_key(&self, key: &str) -> AppResult<bool> {
        let bytes = self.output_store.get_bytes(key).await?;
        let packet: StructuredIntelPacket = serde_json::from_slice(&bytes)?;
        if packet.market_context_status != MarketContextStatus::Pending {
            return Ok(false);
        }
        if packet.market_context_terminal_reason.is_some() {
            return Ok(false);
        }
        if packet
            .market_context_retry_after_ms
            .is_some_and(|retry_after_ms| retry_after_ms > now_ms())
        {
            return Ok(false);
        }
        if self.is_not_latest_revision(&packet).await? {
            return Ok(false);
        }

        let refreshed_context = self
            .market_reader
            .context_for(
                packet.published_at_ms,
                packet.fetched_at_ms,
                &packet.normalized_symbols,
            )
            .await;
        if refreshed_context.status.is_any_available() {
            self.publish_revision(packet, refreshed_context, None)
                .await?;
            return Ok(true);
        }
        if packet
            .market_context_expire_at_ms
            .or_else(|| {
                Some(
                    packet
                        .decision_available_at_ms
                        .saturating_add(self.config.market_context_expire_after_ms),
                )
            })
            .is_some_and(|expire_at_ms| expire_at_ms <= now_ms())
        {
            let basis_kind = if packet.published_at_ms.is_some() {
                "published_at_ms"
            } else {
                "fetched_at_ms"
            };
            let terminal_context =
                MarketContextSnapshot::unavailable("terminal_missing_market_context", basis_kind);
            self.publish_revision(
                packet,
                terminal_context,
                Some("terminal_missing_market_context".to_owned()),
            )
            .await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn is_not_latest_revision(&self, packet: &StructuredIntelPacket) -> AppResult<bool> {
        let Some(index) = self
            .latest_revision_index(effective_packet_family_id(packet))
            .await?
        else {
            return Ok(false);
        };
        Ok(packet.revision < index.latest_revision)
    }

    async fn latest_revision_index(
        &self,
        packet_family_id: &str,
    ) -> AppResult<Option<PacketRevisionIndex>> {
        let mut latest: Option<(u32, String)> = None;
        for key in self
            .output_store
            .list_keys(
                &keys::packet_revision_index_prefix(packet_family_id),
                REVISION_INDEX_MAX_KEYS,
            )
            .await?
        {
            let Some(revision) = parse_revision_from_key(&key) else {
                continue;
            };
            let replace = latest
                .as_ref()
                .is_none_or(|(current_revision, _)| revision > *current_revision);
            if replace {
                latest = Some((revision, key));
            }
        }
        let Some((_, key)) = latest else {
            return Ok(None);
        };
        self.output_store.get_json(&key).await.map(Some)
    }

    async fn publish_revision(
        &self,
        packet: StructuredIntelPacket,
        market_context: MarketContextSnapshot,
        terminal_reason: Option<String>,
    ) -> AppResult<()> {
        let revision = packet.revision.saturating_add(1);
        let created_at_ms = now_ms();
        let packet_family_id = effective_packet_family_id(&packet).to_owned();
        let raw_event_id = effective_raw_event_id(&packet).to_owned();
        let packet_id = revised_packet_id(&packet_family_id, revision);
        let mut revised_packet = packet.clone();
        revised_packet.packet_family_id = packet_family_id.clone();
        revised_packet.raw_event_id = raw_event_id.clone();
        revised_packet.packet_id = packet_id.clone();
        revised_packet.revision = revision;
        revised_packet.supersedes_packet_id = Some(packet.packet_id.clone());
        revised_packet.market_context_status = market_context.status.clone();
        revised_packet.market_context = market_context.clone();
        revised_packet.market_context_ref = market_context_ref(&market_context);
        revised_packet.market_context_retry_after_ms = None;
        revised_packet.market_context_expire_at_ms = None;
        revised_packet.market_context_terminal_reason = terminal_reason.clone();
        if market_context.status.is_any_available() {
            revised_packet.evidence_quality_reasons.retain(|reason| {
                !matches!(
                    reason,
                    crate::models::output::EvidenceQualityReason::MarketContextMissing
                )
            });
        }

        let structured_key = keys::structured_packet_key(created_at_ms, &raw_event_id, &packet_id);
        let structured_bytes = self
            .output_store
            .put_jsonl_idempotent(&structured_key, std::slice::from_ref(&revised_packet))
            .await?;

        let manifest = IntelL1Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION.to_owned(),
            run_id: run_id("intel-l1-rehydration", created_at_ms),
            raw_event_id: raw_event_id.clone(),
            status: if terminal_reason.is_some() {
                "terminal_missing_market_context".to_owned()
            } else {
                "rehydrated_market_context".to_owned()
            },
            started_at_ms: created_at_ms,
            finished_at_ms: created_at_ms,
            structuring_policy_version: self.config.structuring_policy_version.clone(),
            output_object_count: 1,
            output_objects: vec![OutputObjectRef {
                object_family: "structured_intel_packet".to_owned(),
                key: structured_key.clone(),
                record_count: 1,
                byte_count: structured_bytes.len(),
            }],
            structured_packet_count: 1,
            context_flag_packet_count: 0,
            story_cluster_count: 0,
            health_event_count: 0,
        };
        let manifest_key = keys::manifest_key(created_at_ms, &raw_event_id, &manifest.run_id);
        self.output_store
            .put_json_idempotent(&manifest_key, &manifest)
            .await?;

        let revision_index = PacketRevisionIndex {
            schema_version: PacketRevisionIndex::schema(),
            packet_family_id: packet_family_id.clone(),
            raw_event_id: raw_event_id.clone(),
            latest_revision: revision,
            latest_packet_id: packet_id.clone(),
            latest_structured_key: structured_key.clone(),
            market_context_status: revised_packet.market_context_status.clone(),
            updated_at_ms: created_at_ms,
        };
        self.output_store
            .put_json_idempotent(
                &keys::packet_revision_index_key(&packet_family_id, revision),
                &revision_index,
            )
            .await?;

        let pointer = StructuredPointer {
            schema_version: STRUCTURED_PACKET_SCHEMA_VERSION.to_owned(),
            packet_id,
            raw_event_id,
            terminal_decision: revised_packet.terminal_decision.clone(),
            storage_ref: S3ObjectPointer {
                bucket: self.output_store.bucket().to_owned(),
                key: structured_key,
                content_sha256: sha256_prefixed(&structured_bytes),
                schema_version: STRUCTURED_PACKET_SCHEMA_VERSION.to_owned(),
            },
            manifest_key,
            created_at_ms,
        };
        self.publisher
            .publish_structured_pointer(&revised_packet, &pointer)
            .await?;
        self.publisher.flush().await?;
        Ok(())
    }
}

fn parse_revision_from_key(key: &str) -> Option<u32> {
    key.strip_suffix(".json")?
        .rsplit_once("revision=")?
        .1
        .parse()
        .ok()
}

fn effective_packet_family_id(packet: &StructuredIntelPacket) -> &str {
    if !packet.packet_family_id.trim().is_empty() {
        packet.packet_family_id.as_str()
    } else if !packet.raw_event_id.trim().is_empty() {
        packet.raw_event_id.as_str()
    } else if let Some(source_event_id) = packet.source_event_ids.first() {
        source_event_id.as_str()
    } else {
        packet.packet_id.as_str()
    }
}

fn effective_raw_event_id(packet: &StructuredIntelPacket) -> &str {
    if !packet.raw_event_id.trim().is_empty() {
        packet.raw_event_id.as_str()
    } else if let Some(source_event_id) = packet.source_event_ids.first() {
        source_event_id.as_str()
    } else {
        packet.packet_id.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::parse_revision_from_key;

    #[test]
    fn parses_revision_index_key() {
        assert_eq!(
            parse_revision_from_key(
                "packet-revision-index/schema=packet_revision_index_v1/packet_family_id=family_1/revision=0000000007.json"
            ),
            Some(7)
        );
    }
}
