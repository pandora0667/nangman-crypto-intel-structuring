use crate::ai::contract::{
    ModelProvider, ModelStage, ModelStructuringRequest, ModelStructuringResponse,
};
use crate::config::ModelPolicyConfig;
use crate::error::AppResult;
use crate::models::constants::{DEFAULT_ESCALATION_MODEL_ID, DEFAULT_PRIMARY_MODEL_ID};
use crate::models::market::{MarketContextSnapshot, MarketContextStatus};
use crate::models::output::{
    ConfidenceBand, ContradictionFlag, EventType, EvidenceQualityReason, ModelTierUsed,
    RelevanceDecayHint, TerminalDecision,
};
use crate::models::raw::RawIntelEvent;
use crate::structuring::packet::build_packet_set;
use crate::structuring::router::ModelRouter;
use crate::structuring::router::StructuringDecision;
use crate::structuring::rule::assess;
use async_trait::async_trait;

struct ScriptedProvider {
    haiku: Option<ModelStructuringResponse>,
    haiku_repair: Option<ModelStructuringResponse>,
    sonnet: Option<ModelStructuringResponse>,
}

#[async_trait]
impl ModelProvider for ScriptedProvider {
    async fn structure(
        &self,
        stage: ModelStage,
        _request: &ModelStructuringRequest,
    ) -> AppResult<ModelStructuringResponse> {
        match stage {
            ModelStage::Haiku => self
                .haiku
                .clone()
                .ok_or_else(|| crate::error::AppError::bedrock("haiku unavailable")),
            ModelStage::HaikuRepair => self
                .haiku_repair
                .clone()
                .ok_or_else(|| crate::error::AppError::bedrock("haiku repair unavailable")),
            ModelStage::Sonnet => self
                .sonnet
                .clone()
                .ok_or_else(|| crate::error::AppError::bedrock("sonnet unavailable")),
        }
    }
}

#[tokio::test]
async fn escalates_from_weak_haiku_to_sonnet() {
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(response(0.4, TerminalDecision::UnsupportedOrWeak)),
            haiku_repair: None,
            sonnet: Some(response(0.9, TerminalDecision::HighConfidenceStructured)),
        },
        policy(true),
    );
    let event = event();
    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(
        decision.model_tier_used,
        crate::models::output::ModelTierUsed::Sonnet
    );
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 1);
}

#[tokio::test]
async fn falls_back_when_models_disabled() {
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: None,
            haiku_repair: None,
            sonnet: None,
        },
        policy(false),
    );
    let decision = router.decide(&event(), &market_context()).await.unwrap();

    assert_eq!(
        decision.model_tier_used,
        crate::models::output::ModelTierUsed::FallbackOnly
    );
    assert_eq!(decision.fallback_count, 1);
}

#[tokio::test]
async fn drops_unsupported_sonnet_output_instead_of_using_weak_model_content() {
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(response(0.4, TerminalDecision::UnsupportedOrWeak)),
            haiku_repair: None,
            sonnet: Some(response_with_evidence(
                0.9,
                TerminalDecision::HighConfidenceStructured,
                "Unsupported sentence from another source",
            )),
        },
        policy(true),
    );
    let decision = router.decide(&event(), &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::FallbackOnly);
    assert!(decision.model_response.is_none());
    assert_eq!(decision.fallback_count, 1);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 1);
}

#[tokio::test]
async fn sonnet_failure_does_not_reuse_weak_haiku_content() {
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(response(0.4, TerminalDecision::UnsupportedOrWeak)),
            haiku_repair: None,
            sonnet: None,
        },
        policy(true),
    );
    let decision = router.decide(&event(), &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::FallbackOnly);
    assert!(decision.model_response.is_none());
    assert_eq!(decision.fallback_count, 1);
}

#[tokio::test]
async fn accepts_haiku_for_low_value_unsupported_outputs() {
    let mut event = event();
    event.source_category = "news".to_owned();
    event.symbol_candidates.clear();
    event.title = "General market commentary continues".to_owned();
    event.body = "General market commentary continues without a specific coin catalyst.".to_owned();
    let mut haiku = response_with_evidence(
        0.5,
        TerminalDecision::UnsupportedOrWeak,
        "General market commentary continues without a specific coin catalyst",
    );
    haiku.event_type = EventType::Other;
    haiku.normalized_symbols.clear();
    haiku.symbol_confidence_band = ConfidenceBand::Weak;
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(haiku),
            haiku_repair: None,
            sonnet: Some(response(0.9, TerminalDecision::HighConfidenceStructured)),
        },
        policy(true),
    );

    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::Haiku);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn community_reaction_uses_haiku_without_sonnet_when_evidence_is_direct() {
    let mut event = event();
    event.source_id = "social_hackernews_solana_rss".to_owned();
    event.source_category = "social".to_owned();
    event.title = "SOL developer discussion gains attention".to_owned();
    event.body = "SOL developer discussion gains attention from the community.".to_owned();
    event.symbol_candidates = vec!["SOL".to_owned()];
    event.event_category_hint = Some("community_reaction".to_owned());
    event.content_kind = Some("community_reaction".to_owned());
    event.content_quality = Some("full_text".to_owned());
    event.content_quality_score = Some(80);
    event.source_quality = Some("community_reaction".to_owned());
    event.source_relevance_scope = Some("direct_asset".to_owned());
    event.direct_asset_count = Some(1);
    event.matched_asset_count = Some(1);
    let mut haiku = response_with_evidence(
        0.86,
        TerminalDecision::HighConfidenceStructured,
        "SOL developer discussion gains attention from the community",
    );
    haiku.event_type = EventType::SocialHype;
    haiku.normalized_symbols = vec!["SOL".to_owned()];
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(haiku),
            haiku_repair: None,
            sonnet: Some(response(0.95, TerminalDecision::HighConfidenceStructured)),
        },
        policy(true),
    );

    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::Haiku);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn weak_global_symbol_scan_claim_stays_on_haiku() {
    let mut event = event();
    event.title = "ABC market headline circulates".to_owned();
    event.body = "ABC market headline circulates in a broad market digest.".to_owned();
    event.content_quality = Some("title_only".to_owned());
    event.content_quality_score = Some(30);
    event.source_quality = Some("global_symbol_scan".to_owned());
    event.source_relevance_scope = Some("global_symbol_scan".to_owned());
    event.direct_asset_count = Some(0);
    event.matched_asset_count = Some(1);
    let haiku = response_with_evidence(
        0.88,
        TerminalDecision::HighConfidenceStructured,
        "ABC market headline circulates in a broad market digest",
    );
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(haiku),
            haiku_repair: None,
            sonnet: Some(response_with_evidence(
                0.82,
                TerminalDecision::LowConfidenceStructured,
                "ABC market headline circulates in a broad market digest",
            )),
        },
        policy(true),
    );

    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::Haiku);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn numeric_market_snapshot_with_pending_context_bypasses_models() {
    let event = numeric_snapshot_event();
    let mut haiku = response_with_evidence(
        0.62,
        TerminalDecision::LowConfidenceStructured,
        r#"{"symbol":"BTCUSDT","open_interest":"1042","event_time_ms":1}"#,
    );
    haiku.event_type = EventType::FundingShift;
    haiku.normalized_symbols = vec!["BTC".to_owned()];
    haiku.symbol_confidence_band = ConfidenceBand::Moderate;
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(haiku),
            haiku_repair: None,
            sonnet: Some(response(0.95, TerminalDecision::HighConfidenceStructured)),
        },
        policy(true),
    );

    let decision = router
        .decide(&event, &pending_market_context())
        .await
        .unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::RuleOnly);
    assert_eq!(decision.haiku_invocations, 0);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn numeric_market_snapshot_with_stale_context_stays_on_haiku() {
    let event = numeric_snapshot_event();
    let mut haiku = response_with_evidence(
        0.62,
        TerminalDecision::LowConfidenceStructured,
        r#"{"symbol":"BTCUSDT","open_interest":"1042","event_time_ms":1}"#,
    );
    haiku.event_type = EventType::FundingShift;
    haiku.normalized_symbols = vec!["BTC".to_owned()];
    haiku.symbol_confidence_band = ConfidenceBand::Moderate;
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(haiku),
            haiku_repair: None,
            sonnet: Some(response(0.95, TerminalDecision::HighConfidenceStructured)),
        },
        policy(true),
    );

    let decision = router
        .decide(&event, &stale_market_context())
        .await
        .unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::Haiku);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn single_numeric_funding_snapshot_never_uses_sonnet_even_with_context() {
    let event = numeric_snapshot_event();
    let mut haiku = response_with_evidence(
        0.62,
        TerminalDecision::LowConfidenceStructured,
        r#"{"symbol":"BTCUSDT","open_interest":"1042","event_time_ms":1}"#,
    );
    haiku.event_type = EventType::FundingShift;
    haiku.normalized_symbols = vec!["BTC".to_owned()];
    haiku.symbol_confidence_band = ConfidenceBand::Moderate;
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(haiku),
            haiku_repair: None,
            sonnet: Some(response(0.95, TerminalDecision::HighConfidenceStructured)),
        },
        policy(true),
    );

    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::Haiku);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn noncritical_high_impact_escalation_respects_sonnet_budget() {
    let mut event = event();
    event.title = "ABC listing expands to a new venue".to_owned();
    event.body = "ABC listing expands to a new venue with limited supporting detail.".to_owned();
    event.symbol_candidates.clear();
    let mut haiku = response_with_evidence(
        0.5,
        TerminalDecision::LowConfidenceStructured,
        "ABC listing expands to a new venue with limited supporting detail",
    );
    haiku.event_type = EventType::Listing;
    haiku.normalized_symbols = vec!["ABC".to_owned()];
    haiku.symbol_confidence_band = ConfidenceBand::Moderate;
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(haiku),
            haiku_repair: None,
            sonnet: Some(response(0.95, TerminalDecision::HighConfidenceStructured)),
        },
        policy_with_sonnet_budget(true, 0.0),
    );

    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::Haiku);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn noncritical_sonnet_fallback_respects_sonnet_budget() {
    let mut event = event();
    event.title = "ABC listing expands to a new venue".to_owned();
    event.body = "ABC listing expands to a new venue with limited supporting detail.".to_owned();
    event.symbol_candidates.clear();
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: None,
            haiku_repair: None,
            sonnet: Some(response(0.95, TerminalDecision::HighConfidenceStructured)),
        },
        policy_with_sonnet_budget(true, 0.0),
    );

    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::FallbackOnly);
    assert_eq!(decision.fallback_count, 1);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[tokio::test]
async fn unsupported_low_quality_broad_scan_does_not_use_sonnet() {
    let mut event = event();
    event.title = "ABC market headline circulates".to_owned();
    event.body = "ABC market headline circulates in a broad market digest.".to_owned();
    event.content_quality = Some("title_only".to_owned());
    event.content_quality_score = Some(30);
    event.source_quality = Some("global_symbol_scan".to_owned());
    event.source_relevance_scope = Some("global_symbol_scan".to_owned());
    event.direct_asset_count = Some(0);
    event.matched_asset_count = Some(1);
    let router = ModelRouter::new(
        ScriptedProvider {
            haiku: Some(response_with_evidence(
                0.35,
                TerminalDecision::UnsupportedOrWeak,
                "ABC market headline circulates in a broad market digest",
            )),
            haiku_repair: None,
            sonnet: Some(response(0.95, TerminalDecision::HighConfidenceStructured)),
        },
        policy(true),
    );

    let decision = router.decide(&event, &market_context()).await.unwrap();

    assert_eq!(decision.model_tier_used, ModelTierUsed::Haiku);
    assert_eq!(decision.haiku_invocations, 1);
    assert_eq!(decision.sonnet_invocations, 0);
}

#[test]
fn packet_set_is_deterministic_for_redelivery_inputs() {
    let event = event();
    let decision = StructuringDecision {
        rule: assess(&event),
        model_response: None,
        model_tier_used: ModelTierUsed::RuleOnly,
        fallback_count: 0,
        haiku_invocations: 0,
        sonnet_invocations: 0,
    };

    let first = build_packet_set(
        &event,
        &decision,
        market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );
    let second = build_packet_set(
        &event,
        &decision,
        market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );

    assert_eq!(first, second);
    assert_eq!(
        first.structured_packet.source_quality_summary,
        "T1 source news_test freshness_ms=1233 content_quality=full_text score=80 source_quality=trusted_symbol_match relevance_scope=symbol_alias_match"
    );
    assert_eq!(first.structured_packet.published_at_ms, Some(1));
    assert_eq!(first.structured_packet.fetched_at_ms, 1);
    assert_eq!(first.structured_packet.raw_event_id, "intel_evt_test");
    assert_eq!(first.structured_packet.event_timestamp_ms, 1);
    assert_eq!(first.structured_packet.revision, 0);
    assert_eq!(first.structured_packet.structured_at_ms, 1234);
    assert_eq!(first.structured_packet.decision_available_at_ms, 1234);
    assert_eq!(
        first.structured_packet.market_context_status,
        MarketContextStatus::AvailableSymbolContext
    );
    assert_eq!(
        first
            .structured_packet
            .source_independence_summary
            .independent_source_count,
        1
    );
    assert_eq!(
        first.structured_packet.symbol_resolution_trace[0].canonical_symbol,
        Some("ABC".to_owned())
    );
    assert!(!first.structured_packet.text_evidence.is_empty());
    assert!(
        first
            .structured_packet
            .evidence_quality_reasons
            .contains(&EvidenceQualityReason::SingleSourceOnly)
    );
    assert_eq!(first.context_flag_packet, second.context_flag_packet);
}

#[test]
fn numeric_snapshot_records_metric_guard_reasons() {
    let mut event = event();
    event.source_id = "derivatives_binance_usdm_open_interest_rest".to_owned();
    event.source_category = "funding".to_owned();
    event.content_quality = Some("numeric_observation".to_owned());
    event.source_quality = Some("market_snapshot".to_owned());
    event.event_category_hint = Some("open_interest_snapshot".to_owned());
    let mut model = response(0.7, TerminalDecision::LowConfidenceStructured);
    model.event_type = EventType::FundingShift;
    model.normalized_symbols = vec!["ABC".to_owned()];
    let decision = StructuringDecision {
        rule: assess(&event),
        model_response: Some(model),
        model_tier_used: ModelTierUsed::Haiku,
        fallback_count: 0,
        haiku_invocations: 1,
        sonnet_invocations: 0,
    };

    let packet_set = build_packet_set(
        &event,
        &decision,
        pending_market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );

    assert_eq!(packet_set.structured_packet.metric_evidence.len(), 1);
    assert!(
        packet_set
            .structured_packet
            .evidence_quality_reasons
            .contains(&EvidenceQualityReason::SingleNumericSnapshot)
    );
    assert!(
        packet_set
            .structured_packet
            .evidence_quality_reasons
            .contains(&EvidenceQualityReason::BaselineMissing)
    );
    assert!(
        packet_set
            .structured_packet
            .evidence_quality_reasons
            .contains(&EvidenceQualityReason::MarketContextMissing)
    );
    assert_eq!(
        packet_set.structured_packet.market_context_retry_after_ms,
        Some(301_234)
    );
    assert_eq!(
        packet_set.structured_packet.market_context_expire_at_ms,
        Some(21_601_234)
    );
}

#[test]
fn duplicate_or_syndicated_source_records_content_hash_guard() {
    let mut event = event();
    event.source_quality = Some("syndicated_duplicate".to_owned());
    let decision = StructuringDecision {
        rule: assess(&event),
        model_response: None,
        model_tier_used: ModelTierUsed::RuleOnly,
        fallback_count: 0,
        haiku_invocations: 0,
        sonnet_invocations: 0,
    };

    let packet_set = build_packet_set(
        &event,
        &decision,
        market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );

    assert_eq!(
        packet_set
            .structured_packet
            .source_independence_summary
            .duplicate_content_hashes,
        vec![event.content_hash]
    );
    assert!(
        packet_set
            .structured_packet
            .evidence_quality_reasons
            .contains(&EvidenceQualityReason::DuplicateOrSyndicatedSource)
    );
}

#[test]
fn high_confidence_structured_packet_emits_context_flag() {
    let event = event();
    let decision = StructuringDecision {
        rule: assess(&event),
        model_response: Some(response(0.9, TerminalDecision::HighConfidenceStructured)),
        model_tier_used: ModelTierUsed::Haiku,
        fallback_count: 0,
        haiku_invocations: 1,
        sonnet_invocations: 0,
    };

    let packet_set = build_packet_set(
        &event,
        &decision,
        market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );

    assert!(packet_set.context_flag_packet.is_some());
    assert_eq!(packet_set.health_event.flag_packet_count, 1);
}

#[test]
fn low_confidence_structured_packet_does_not_emit_context_flag() {
    let event = event();
    let decision = StructuringDecision {
        rule: assess(&event),
        model_response: Some(response(0.7, TerminalDecision::LowConfidenceStructured)),
        model_tier_used: ModelTierUsed::Haiku,
        fallback_count: 0,
        haiku_invocations: 1,
        sonnet_invocations: 0,
    };

    let packet_set = build_packet_set(
        &event,
        &decision,
        market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );

    assert!(packet_set.context_flag_packet.is_none());
    assert_eq!(packet_set.health_event.flag_packet_count, 0);
}

#[test]
fn funding_shift_without_available_market_context_does_not_emit_context_flag() {
    let mut event = event();
    event.source_category = "funding".to_owned();
    event.content_quality = Some("numeric_observation".to_owned());
    event.source_quality = Some("market_snapshot".to_owned());
    let mut model = response(0.9, TerminalDecision::HighConfidenceStructured);
    model.event_type = EventType::FundingShift;
    model.normalized_symbols = vec!["ABC".to_owned()];
    let decision = StructuringDecision {
        rule: assess(&event),
        model_response: Some(model),
        model_tier_used: ModelTierUsed::Sonnet,
        fallback_count: 0,
        haiku_invocations: 1,
        sonnet_invocations: 1,
    };

    let packet_set = build_packet_set(
        &event,
        &decision,
        pending_market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );

    assert!(packet_set.context_flag_packet.is_none());
    assert_eq!(packet_set.health_event.flag_packet_count, 0);
}

#[test]
fn weak_symbol_packet_does_not_emit_context_flag() {
    let event = event();
    let mut weak_model = response(0.7, TerminalDecision::LowConfidenceStructured);
    weak_model.symbol_confidence_band = ConfidenceBand::Weak;
    let decision = StructuringDecision {
        rule: assess(&event),
        model_response: Some(weak_model),
        model_tier_used: ModelTierUsed::Haiku,
        fallback_count: 0,
        haiku_invocations: 1,
        sonnet_invocations: 0,
    };

    let packet_set = build_packet_set(
        &event,
        &decision,
        market_context(),
        "policy-v1",
        1234,
        300_000,
        21_600_000,
    );

    assert!(packet_set.context_flag_packet.is_none());
    assert_eq!(packet_set.health_event.flag_packet_count, 0);
}

fn policy(enable_bedrock: bool) -> ModelPolicyConfig {
    policy_with_sonnet_budget(enable_bedrock, 0.15)
}

fn policy_with_sonnet_budget(enable_bedrock: bool, sonnet_budget_ratio: f64) -> ModelPolicyConfig {
    ModelPolicyConfig {
        primary_model_id: DEFAULT_PRIMARY_MODEL_ID.to_owned(),
        escalation_model_id: DEFAULT_ESCALATION_MODEL_ID.to_owned(),
        escalate_if_confidence_below: 0.65,
        sonnet_budget_ratio,
        enable_bedrock,
    }
}

fn event() -> RawIntelEvent {
    RawIntelEvent {
        event_id: "intel_evt_test".to_owned(),
        source_id: "news_test".to_owned(),
        source_category: "news".to_owned(),
        source_name: "News".to_owned(),
        fetched_at_ms: 1,
        published_at_ms: Some(1),
        observed_at_ms: 1,
        language: "en".to_owned(),
        title: "Protocol exploit investigation expands".to_owned(),
        body: "Protocol exploit investigation expands after the team confirmed the incident."
            .to_owned(),
        url: "https://example.com".to_owned(),
        author_or_channel: None,
        trust_tier: "T1".to_owned(),
        cadence_tier: "low".to_owned(),
        content_hash: "h".to_owned(),
        dedup_key: "d".to_owned(),
        symbol_candidates: vec!["ABC".to_owned()],
        event_category_hint: None,
        top50_relevance: "relevant".to_owned(),
        content_kind: Some("news_article".to_owned()),
        content_quality: Some("full_text".to_owned()),
        content_quality_score: Some(80),
        source_quality: Some("trusted_symbol_match".to_owned()),
        source_relevance_scope: Some("symbol_alias_match".to_owned()),
        direct_asset_count: Some(0),
        matched_asset_count: Some(1),
        historical_source_depth: None,
        backfill_window_start_ms: None,
        backfill_window_end_ms: None,
        source_time_range_verified: None,
        schema_version: "raw_intel_event_v1".to_owned(),
    }
}

fn numeric_snapshot_event() -> RawIntelEvent {
    let mut event = event();
    event.source_id = "derivatives_binance_usdm_open_interest_rest".to_owned();
    event.source_category = "funding".to_owned();
    event.title = "Binance USD-M open interest BTCUSDT".to_owned();
    event.body = r#"{"symbol":"BTCUSDT","open_interest":"1042","event_time_ms":1}"#.to_owned();
    event.symbol_candidates = vec!["BTC".to_owned()];
    event.event_category_hint = Some("open_interest_snapshot".to_owned());
    event.content_kind = Some("derivatives_snapshot".to_owned());
    event.content_quality = Some("numeric_observation".to_owned());
    event.content_quality_score = Some(63);
    event.source_quality = Some("market_snapshot".to_owned());
    event.source_relevance_scope = Some("symbol_alias_match".to_owned());
    event
}

fn market_context() -> MarketContextSnapshot {
    MarketContextSnapshot {
        status: MarketContextStatus::AvailableSymbolContext,
        basis_timestamp_ms: Some(1),
        basis_kind: "published_at_ms".to_owned(),
        window_start_ms: Some(0),
        window_end_ms: Some(1000),
        manifest_key: Some("m".to_owned()),
        output_object_keys: vec!["o".to_owned()],
        market_data_quality_summary_key: Some("q".to_owned()),
        market_feature_delta_key: Some("d".to_owned()),
        market_feature_delta_summary_key: Some("ds".to_owned()),
        market_regime_context_key: Some("r".to_owned()),
        symbol_universe_snapshot_key: Some("u".to_owned()),
        symbol_summaries: Vec::new(),
        unavailable_reason: None,
    }
}

fn pending_market_context() -> MarketContextSnapshot {
    MarketContextSnapshot {
        status: MarketContextStatus::Pending,
        basis_timestamp_ms: Some(1),
        basis_kind: "published_at_ms".to_owned(),
        window_start_ms: None,
        window_end_ms: None,
        manifest_key: None,
        output_object_keys: Vec::new(),
        market_data_quality_summary_key: None,
        market_feature_delta_key: None,
        market_feature_delta_summary_key: None,
        market_regime_context_key: None,
        symbol_universe_snapshot_key: None,
        symbol_summaries: Vec::new(),
        unavailable_reason: Some("fixture pending".to_owned()),
    }
}

fn stale_market_context() -> MarketContextSnapshot {
    let mut context = market_context();
    context.status = MarketContextStatus::StaleButUsable;
    context.window_start_ms = Some(0);
    context.window_end_ms = Some(1000);
    context
}

fn response(confidence: f64, terminal_decision: TerminalDecision) -> ModelStructuringResponse {
    response_with_evidence(
        confidence,
        terminal_decision,
        "Protocol exploit investigation expands after the team confirmed the incident",
    )
}

fn response_with_evidence(
    confidence: f64,
    terminal_decision: TerminalDecision,
    evidence_sentence: &str,
) -> ModelStructuringResponse {
    ModelStructuringResponse {
        event_type: EventType::Incident,
        normalized_symbols: vec!["ABC".to_owned()],
        symbol_confidence_band: ConfidenceBand::Strong,
        topic_summary: "Incident confirmed".to_owned(),
        stance_summary: "Evidence supports an incident classification".to_owned(),
        risk_summary: "Operational risk is present".to_owned(),
        regime_hint: "event_driven".to_owned(),
        scenario_hint: "watch_only".to_owned(),
        confidence_band: if confidence >= 0.8 {
            ConfidenceBand::High
        } else {
            ConfidenceBand::Low
        },
        confidence_score: confidence,
        novelty_score: 0.8,
        relevance_decay_hint: RelevanceDecayHint::MultiDay,
        contradiction_flags: Vec::<ContradictionFlag>::new(),
        evidence_ids: Vec::new(),
        evidence_sentences: vec![evidence_sentence.to_owned()],
        terminal_decision,
    }
}
