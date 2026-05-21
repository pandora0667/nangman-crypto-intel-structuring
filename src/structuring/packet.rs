use crate::hash::stable_short_id;
use crate::models::constants::{
    CONTEXT_FLAG_SCHEMA_VERSION, HEALTH_EVENT_SCHEMA_VERSION, STORY_CLUSTER_SCHEMA_VERSION,
    STRUCTURED_PACKET_SCHEMA_VERSION,
};
use crate::models::market::MarketContextSnapshot;
use crate::models::output::{
    ConfidenceBand, ConflictLevel, ContextFlagPacket, EvidenceQualityReason, HealthLevel,
    IntelL1Manifest, MarketContextRef, MetricEvidence, OutputObjectRef, SourceIndependenceSummary,
    StoryCluster, StructuredIntelPacket, StructuringHealthEvent, SymbolResolutionTrace,
    TerminalDecision, TextEvidence, TimeRelevanceWindow,
};
use crate::models::raw::RawIntelEvent;
use crate::structuring::router::StructuringDecision;
use crate::structuring::story::{story_cluster_id, story_hint_key};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub struct PacketSet {
    pub story_cluster: StoryCluster,
    pub structured_packet: StructuredIntelPacket,
    pub context_flag_packet: Option<ContextFlagPacket>,
    pub health_event: StructuringHealthEvent,
}

#[derive(Debug, Clone)]
pub struct ManifestBuildInput {
    pub run_id: String,
    pub raw_event_id: String,
    pub status: String,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub policy_version: String,
    pub output_objects: Vec<OutputObjectRef>,
}

pub fn build_packet_set(
    event: &RawIntelEvent,
    decision: &StructuringDecision,
    market_context: MarketContextSnapshot,
    policy_version: &str,
    observed_at_ms: i64,
    market_context_retry_interval_ms: i64,
    market_context_expire_after_ms: i64,
) -> PacketSet {
    let model = decision.model_response.as_ref();
    let packet_family_id = stable_short_id(
        "intel_pkt_family",
        &[
            &event.event_id,
            STRUCTURED_PACKET_SCHEMA_VERSION,
            policy_version,
        ],
    );
    let packet_id = stable_short_id(
        "intel_pkt",
        &[
            &event.event_id,
            STRUCTURED_PACKET_SCHEMA_VERSION,
            policy_version,
        ],
    );
    let flag_packet_id = stable_short_id(
        "intel_flag",
        &[&packet_id, CONTEXT_FLAG_SCHEMA_VERSION, policy_version],
    );
    let normalized_symbols = model
        .map(|value| value.normalized_symbols.clone())
        .unwrap_or_else(|| decision.rule.normalized_symbols.clone());
    let confidence_band = model
        .map(|value| value.confidence_band.clone())
        .unwrap_or_else(|| decision.rule.confidence_band.clone());
    let event_type = model
        .map(|value| value.event_type.clone())
        .unwrap_or_else(|| decision.rule.event_type.clone());
    let story_hint_key = story_hint_key(event, &event_type, &normalized_symbols);
    let cluster_id = story_cluster_id(&story_hint_key, policy_version);
    let contradiction_flags = model
        .map(|value| value.contradiction_flags.clone())
        .unwrap_or_else(|| decision.rule.contradiction_flags.clone());
    let terminal_decision = model
        .map(|value| value.terminal_decision.clone())
        .unwrap_or_else(|| decision.rule.terminal_decision.clone());
    let evidence_sentences = model
        .map(|value| value.evidence_sentences.clone())
        .unwrap_or_else(|| decision.rule.evidence_sentences.clone());
    let structured_at_ms = observed_at_ms;
    let decision_available_at_ms = decision_available_at_ms(event, structured_at_ms);
    let event_timestamp_ms = event.published_at_ms.unwrap_or(event.fetched_at_ms);
    let (market_context_retry_after_ms, market_context_expire_at_ms) =
        pending_market_context_schedule(
            &market_context,
            decision_available_at_ms,
            market_context_retry_interval_ms,
            market_context_expire_after_ms,
        );
    let time_relevance_window = time_window(
        event.published_at_ms.unwrap_or(event.fetched_at_ms),
        model
            .map(|value| value.relevance_decay_hint.clone())
            .unwrap_or_else(|| decision.rule.relevance_decay_hint.clone()),
    );

    let story_cluster = StoryCluster {
        cluster_id: cluster_id.clone(),
        source_event_ids: vec![event.event_id.clone()],
        story_hint_key,
        primary_topic: format!("{:?}", event_type),
        secondary_topics: Vec::new(),
        related_symbols: normalized_symbols.clone(),
        source_count: 1,
        trust_mix: event.trust_tier.clone(),
        first_published_at_ms: event.published_at_ms,
        last_updated_at_ms: observed_at_ms,
        novelty_score: model
            .map(|value| value.novelty_score)
            .unwrap_or(decision.rule.novelty_score),
        conflict_level: if contradiction_flags.is_empty() {
            ConflictLevel::None
        } else {
            ConflictLevel::Medium
        },
        conflicting_source_ids: Vec::new(),
        resolution_summary: "single source story".to_owned(),
        schema_version: STORY_CLUSTER_SCHEMA_VERSION.to_owned(),
    };

    let structured_packet = StructuredIntelPacket {
        packet_id: packet_id.clone(),
        packet_family_id: packet_family_id.clone(),
        raw_event_id: event.event_id.clone(),
        event_timestamp_ms,
        revision: 0,
        supersedes_packet_id: None,
        cluster_id: cluster_id.clone(),
        source_event_ids: vec![event.event_id.clone()],
        published_at_ms: event.published_at_ms,
        fetched_at_ms: event.fetched_at_ms,
        structured_at_ms,
        decision_available_at_ms,
        normalized_symbols: normalized_symbols.clone(),
        symbol_confidence_band: model
            .map(|value| value.symbol_confidence_band.clone())
            .unwrap_or_else(|| decision.rule.symbol_confidence_band.clone()),
        symbol_resolution_trace: symbol_resolution_trace(
            event,
            &normalized_symbols,
            model
                .map(|value| &value.symbol_confidence_band)
                .unwrap_or(&decision.rule.symbol_confidence_band),
        ),
        event_type,
        topic_summary: model
            .map(|value| value.topic_summary.clone())
            .unwrap_or_else(|| decision.rule.topic_summary.clone()),
        stance_summary: model
            .map(|value| value.stance_summary.clone())
            .unwrap_or_else(|| decision.rule.stance_summary.clone()),
        risk_summary: model
            .map(|value| value.risk_summary.clone())
            .unwrap_or_else(|| decision.rule.risk_summary.clone()),
        regime_hint: model
            .map(|value| value.regime_hint.clone())
            .unwrap_or_else(|| decision.rule.regime_hint.clone()),
        scenario_hint: model
            .map(|value| value.scenario_hint.clone())
            .unwrap_or_else(|| decision.rule.scenario_hint.clone()),
        confidence_band: confidence_band.clone(),
        novelty_score: model
            .map(|value| value.novelty_score)
            .unwrap_or(decision.rule.novelty_score),
        time_relevance_window: time_relevance_window.clone(),
        contradiction_flags,
        source_quality_summary: source_quality_summary(event, observed_at_ms),
        source_independence_summary: source_independence_summary(event),
        text_evidence: text_evidence(event, &evidence_sentences),
        metric_evidence: metric_evidence(event, &normalized_symbols),
        evidence_quality_reasons: evidence_quality_reasons(
            event,
            &normalized_symbols,
            &market_context,
        ),
        market_context_status: market_context.status.clone(),
        market_context_retry_after_ms,
        market_context_expire_at_ms,
        market_context_terminal_reason: None,
        market_context_ref: market_context_ref(&market_context),
        model_tier_used: decision.model_tier_used.clone(),
        terminal_decision,
        evidence_sentences,
        market_context,
        schema_version: STRUCTURED_PACKET_SCHEMA_VERSION.to_owned(),
    };

    let context_flag_packet =
        should_emit_context_flag(&structured_packet).then(|| ContextFlagPacket {
            flag_packet_id,
            packet_id: packet_id.clone(),
            cluster_id: cluster_id.clone(),
            normalized_symbols: structured_packet.normalized_symbols.clone(),
            observe_only: true,
            block_new_entries: false,
            reduce_only: false,
            paper_only: true,
            context_flag: context_flag(&structured_packet),
            risk_flag: risk_flag(&structured_packet),
            regime_flag: structured_packet.regime_hint.clone(),
            scenario_flag: structured_packet.scenario_hint.clone(),
            time_relevance_window,
            flag_confidence_band: flag_confidence(&confidence_band),
            reason_summary: structured_packet.risk_summary.clone(),
            model_tier_used: decision.model_tier_used.clone(),
            schema_version: CONTEXT_FLAG_SCHEMA_VERSION.to_owned(),
        });

    let health_event = StructuringHealthEvent {
        health_event_id: stable_short_id("intel_l1_health", &[&event.event_id, policy_version]),
        observed_at_ms,
        input_event_count: 1,
        cluster_count: 1,
        structured_packet_count: 1,
        flag_packet_count: usize::from(context_flag_packet.is_some()),
        model_l0_invocations: decision.haiku_invocations,
        model_l1_invocations: decision.sonnet_invocations,
        fallback_count: decision.fallback_count,
        conflict_high_count: usize::from(story_cluster.conflict_level == ConflictLevel::High),
        health_level: if decision.fallback_count > 0 {
            HealthLevel::FallbackOnly
        } else {
            HealthLevel::Healthy
        },
        reason: None,
        schema_version: HEALTH_EVENT_SCHEMA_VERSION.to_owned(),
    };

    PacketSet {
        story_cluster,
        structured_packet,
        context_flag_packet,
        health_event,
    }
}

pub fn revised_packet_id(packet_family_id: &str, revision: u32) -> String {
    stable_short_id(
        "intel_pkt",
        &[packet_family_id, "revision", &revision.to_string()],
    )
}

fn pending_market_context_schedule(
    market_context: &MarketContextSnapshot,
    decision_available_at_ms: i64,
    retry_interval_ms: i64,
    expire_after_ms: i64,
) -> (Option<i64>, Option<i64>) {
    if market_context.status.is_pending_or_unavailable() {
        (
            Some(decision_available_at_ms.saturating_add(retry_interval_ms.max(1))),
            Some(decision_available_at_ms.saturating_add(expire_after_ms.max(1))),
        )
    } else {
        (None, None)
    }
}

pub fn build_manifest(input: ManifestBuildInput, packet_set: &PacketSet) -> IntelL1Manifest {
    IntelL1Manifest {
        schema_version: crate::models::constants::MANIFEST_SCHEMA_VERSION.to_owned(),
        run_id: input.run_id,
        raw_event_id: input.raw_event_id,
        status: input.status,
        started_at_ms: input.started_at_ms,
        finished_at_ms: input.finished_at_ms,
        structuring_policy_version: input.policy_version,
        output_object_count: input.output_objects.len(),
        output_objects: input.output_objects,
        structured_packet_count: 1,
        context_flag_packet_count: usize::from(packet_set.context_flag_packet.is_some()),
        story_cluster_count: 1,
        health_event_count: usize::from(!packet_set.health_event.health_event_id.is_empty()),
    }
}

fn decision_available_at_ms(event: &RawIntelEvent, structured_at_ms: i64) -> i64 {
    [
        event.published_at_ms,
        Some(event.fetched_at_ms),
        Some(event.observed_at_ms),
        Some(structured_at_ms),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(structured_at_ms)
}

fn time_window(
    start_basis_ms: i64,
    decay: crate::models::output::RelevanceDecayHint,
) -> TimeRelevanceWindow {
    let width_ms = match decay {
        crate::models::output::RelevanceDecayHint::Minutes => 30 * 60 * 1000,
        crate::models::output::RelevanceDecayHint::Hours => 6 * 60 * 60 * 1000,
        crate::models::output::RelevanceDecayHint::Day => 24 * 60 * 60 * 1000,
        crate::models::output::RelevanceDecayHint::MultiDay => 3 * 24 * 60 * 60 * 1000,
        crate::models::output::RelevanceDecayHint::Structural => 14 * 24 * 60 * 60 * 1000,
    };
    TimeRelevanceWindow {
        start_ms: start_basis_ms,
        end_ms: start_basis_ms.saturating_add(width_ms),
        relevance_decay_hint: decay,
    }
}

fn symbol_resolution_trace(
    event: &RawIntelEvent,
    normalized_symbols: &[String],
    mapping_confidence: &ConfidenceBand,
) -> Vec<SymbolResolutionTrace> {
    let raw_mentions = event
        .symbol_candidates
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if normalized_symbols.is_empty() {
        return vec![SymbolResolutionTrace {
            raw_mentions,
            resolved_project: None,
            resolved_asset: None,
            canonical_symbol: None,
            venue_symbols: Vec::new(),
            mapping_confidence: ConfidenceBand::Weak,
            ambiguity_reason: Some("no_resolved_symbol".to_owned()),
        }];
    }
    let ambiguity_reason = if normalized_symbols.len() > 1 {
        Some("multiple_candidate_symbols".to_owned())
    } else if matches!(
        mapping_confidence,
        ConfidenceBand::Weak | ConfidenceBand::Low
    ) {
        Some("weak_mapping_confidence".to_owned())
    } else {
        None
    };
    normalized_symbols
        .iter()
        .map(|symbol| SymbolResolutionTrace {
            raw_mentions: raw_mentions.clone(),
            resolved_project: Some(symbol.clone()),
            resolved_asset: Some(symbol.clone()),
            canonical_symbol: Some(symbol.clone()),
            venue_symbols: venue_symbols(symbol),
            mapping_confidence: mapping_confidence.clone(),
            ambiguity_reason: ambiguity_reason.clone(),
        })
        .collect()
}

fn venue_symbols(symbol: &str) -> Vec<String> {
    vec![
        format!("{symbol}USDT"),
        format!("{symbol}USD"),
        format!("KRW-{symbol}"),
    ]
}

fn source_independence_summary(event: &RawIntelEvent) -> SourceIndependenceSummary {
    let source_quality = event.source_quality_or_unknown();
    let duplicate_content_hashes =
        if source_quality.contains("duplicate") || source_quality.contains("syndicated") {
            vec![event.content_hash.clone()]
        } else {
            Vec::new()
        };
    let syndicated_from = source_quality
        .contains("syndicated")
        .then(|| event.source_id.clone());
    SourceIndependenceSummary {
        source_event_count: 1,
        independent_source_count: 1,
        official_source_present: official_source_present(event),
        duplicate_content_hashes,
        syndicated_from,
        original_source_ids: vec![event.source_id.clone()],
    }
}

fn official_source_present(event: &RawIntelEvent) -> bool {
    let source_text = format!(
        "{} {} {}",
        event.source_id,
        event.source_category,
        event.source_quality_or_unknown()
    )
    .to_ascii_lowercase();
    source_text.contains("official")
        || source_text.contains("exchange")
        || source_text.contains("project")
        || source_text.contains("notice")
}

fn text_evidence(event: &RawIntelEvent, evidence_sentences: &[String]) -> Vec<TextEvidence> {
    evidence_sentences
        .iter()
        .filter(|sentence| !sentence.trim().is_empty())
        .map(|sentence| TextEvidence {
            evidence_text: sentence.clone(),
            source_event_id: event.event_id.clone(),
            source_id: event.source_id.clone(),
            published_at_ms: event.published_at_ms,
            evidence_kind: "source_sentence".to_owned(),
        })
        .collect()
}

fn metric_evidence(event: &RawIntelEvent, normalized_symbols: &[String]) -> Vec<MetricEvidence> {
    if event.source_quality_or_unknown() != "market_snapshot"
        && event.content_quality_or_unknown() != "numeric_observation"
    {
        return Vec::new();
    }
    let symbols = if normalized_symbols.is_empty() {
        vec![None]
    } else {
        normalized_symbols
            .iter()
            .map(|symbol| Some(symbol.clone()))
            .collect()
    };
    symbols
        .into_iter()
        .map(|symbol| MetricEvidence {
            metric_name: event
                .event_category_hint
                .clone()
                .unwrap_or_else(|| event.content_kind_or_unknown().to_owned()),
            symbol,
            venue: Some(event.source_id.clone()),
            value: None,
            previous_value: None,
            delta_pct: None,
            window_ms: None,
            observed_at_ms: event.observed_at_ms,
            source_event_id: event.event_id.clone(),
        })
        .collect()
}

fn evidence_quality_reasons(
    event: &RawIntelEvent,
    normalized_symbols: &[String],
    market_context: &MarketContextSnapshot,
) -> Vec<EvidenceQualityReason> {
    let mut reasons = BTreeSet::new();
    reasons.insert(EvidenceQualityReason::SingleSourceOnly);
    if matches!(event.content_quality.as_deref(), Some("title_only")) {
        reasons.insert(EvidenceQualityReason::TitleOnly);
    }
    if normalized_symbols.len() > 1 || event.symbol_candidates.len() > 1 {
        reasons.insert(EvidenceQualityReason::SymbolAmbiguous);
    }
    if !market_context.status.is_symbol_usable() {
        reasons.insert(EvidenceQualityReason::MarketContextMissing);
    }
    if event.source_quality_or_unknown() == "market_snapshot"
        || event.content_quality_or_unknown() == "numeric_observation"
    {
        reasons.insert(EvidenceQualityReason::SingleNumericSnapshot);
        reasons.insert(EvidenceQualityReason::BaselineMissing);
    }
    if event.source_quality_or_unknown().contains("syndicated")
        || event.source_quality_or_unknown().contains("duplicate")
    {
        reasons.insert(EvidenceQualityReason::DuplicateOrSyndicatedSource);
    }
    reasons.into_iter().collect()
}

pub fn market_context_ref(market_context: &MarketContextSnapshot) -> MarketContextRef {
    MarketContextRef {
        status: market_context.status.clone(),
        basis_timestamp_ms: market_context.basis_timestamp_ms,
        basis_kind: market_context.basis_kind.clone(),
        window_start_ms: market_context.window_start_ms,
        window_end_ms: market_context.window_end_ms,
        manifest_key: market_context.manifest_key.clone(),
        output_object_keys: market_context.output_object_keys.clone(),
        market_data_quality_summary_key: market_context.market_data_quality_summary_key.clone(),
        market_feature_delta_key: market_context.market_feature_delta_key.clone(),
        market_feature_delta_summary_key: market_context.market_feature_delta_summary_key.clone(),
        market_regime_context_key: market_context.market_regime_context_key.clone(),
        symbol_universe_snapshot_key: market_context.symbol_universe_snapshot_key.clone(),
    }
}

fn source_quality_summary(event: &RawIntelEvent, observed_at_ms: i64) -> String {
    format!(
        "{} source {} freshness_ms={} content_quality={} score={} source_quality={} relevance_scope={}",
        event.trust_tier,
        event.source_id,
        observed_at_ms.saturating_sub(event.fetched_at_ms),
        event.content_quality_or_unknown(),
        event.content_quality_score_label(),
        event.source_quality_or_unknown(),
        event.source_relevance_scope_or_unknown()
    )
}

fn context_flag(packet: &StructuredIntelPacket) -> String {
    match packet.event_type {
        crate::models::output::EventType::MacroEvent => "macro_uncertainty",
        crate::models::output::EventType::SocialBacklash
        | crate::models::output::EventType::SocialHype => "social_attention_spike",
        crate::models::output::EventType::FundingShift => "funding_stress",
        crate::models::output::EventType::Other => "project_event",
        _ => "exchange_operational_event",
    }
    .to_owned()
}

fn risk_flag(packet: &StructuredIntelPacket) -> String {
    match packet.event_type {
        crate::models::output::EventType::Incident => "operational_risk",
        crate::models::output::EventType::Delisting
        | crate::models::output::EventType::Regulatory => "headline_risk",
        crate::models::output::EventType::FundingShift => "volatility_risk",
        _ => "rumor_risk",
    }
    .to_owned()
}

fn should_emit_context_flag(packet: &StructuredIntelPacket) -> bool {
    if packet.normalized_symbols.is_empty() {
        return false;
    }
    if matches!(
        packet.symbol_confidence_band,
        ConfidenceBand::Weak | ConfidenceBand::Low
    ) {
        return false;
    }
    if !matches!(
        packet.terminal_decision,
        TerminalDecision::HighConfidenceStructured | TerminalDecision::GeneralMarketContext
    ) {
        return false;
    }
    if matches!(
        packet.event_type,
        crate::models::output::EventType::FundingShift
    ) && !packet.market_context.status.is_symbol_usable()
    {
        return false;
    }
    true
}

fn flag_confidence(confidence: &ConfidenceBand) -> ConfidenceBand {
    match confidence {
        ConfidenceBand::High | ConfidenceBand::Strong => ConfidenceBand::High,
        ConfidenceBand::Medium | ConfidenceBand::Moderate => ConfidenceBand::Medium,
        _ => ConfidenceBand::Low,
    }
}
