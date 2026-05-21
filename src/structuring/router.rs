use crate::ai::contract::{
    ModelProvider, ModelStage, ModelStructuringRequest, ModelStructuringResponse,
};
use crate::ai::evidence::build_evidence_pack;
use crate::config::ModelPolicyConfig;
use crate::error::AppResult;
use crate::hash::sha256_hex;
use crate::models::market::MarketContextSnapshot;
use crate::models::output::{ConfidenceBand, ModelTierUsed, TerminalDecision};
use crate::models::raw::RawIntelEvent;
use crate::structuring::nli::{verify_model_response, verify_rule_evidence};
use crate::structuring::rule::{RuleAssessment, assess};
use serde_json::json;
use std::collections::BTreeSet;

pub struct ModelRouter<P: ModelProvider> {
    provider: P,
    policy: ModelPolicyConfig,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructuringDecision {
    pub rule: RuleAssessment,
    pub model_response: Option<ModelStructuringResponse>,
    pub model_tier_used: ModelTierUsed,
    pub fallback_count: usize,
    pub haiku_invocations: usize,
    pub sonnet_invocations: usize,
}

impl<P: ModelProvider> ModelRouter<P> {
    pub fn new(provider: P, policy: ModelPolicyConfig) -> Self {
        Self { provider, policy }
    }

    pub async fn decide(
        &self,
        event: &RawIntelEvent,
        market_context: &MarketContextSnapshot,
    ) -> AppResult<StructuringDecision> {
        let rule = assess(event);
        if self.rule_is_sufficient(event, &rule, market_context) {
            return Ok(StructuringDecision {
                rule,
                model_response: None,
                model_tier_used: ModelTierUsed::RuleOnly,
                fallback_count: 0,
                haiku_invocations: 0,
                sonnet_invocations: 0,
            });
        }

        if should_bypass_models_for_cost(event, market_context, &rule) {
            return Ok(StructuringDecision {
                rule,
                model_response: None,
                model_tier_used: ModelTierUsed::RuleOnly,
                fallback_count: 0,
                haiku_invocations: 0,
                sonnet_invocations: 0,
            });
        }

        if !self.policy.enable_bedrock {
            return Ok(StructuringDecision {
                rule,
                model_response: None,
                model_tier_used: ModelTierUsed::FallbackOnly,
                fallback_count: 1,
                haiku_invocations: 0,
                sonnet_invocations: 0,
            });
        }

        let request = model_request(event, market_context, &rule);
        let haiku = self.provider.structure(ModelStage::Haiku, &request).await;
        let Ok(haiku_response) = haiku else {
            return self
                .try_sonnet_or_fallback(event, market_context, rule, 1)
                .await;
        };
        let mut haiku_invocations = 1;
        let haiku_gate = verify_model_response(event, &haiku_response);
        if haiku_gate.supported
            && !self.should_escalate_from_model(event, market_context, &rule, &haiku_response)
        {
            return Ok(StructuringDecision {
                rule,
                model_response: Some(haiku_response),
                model_tier_used: ModelTierUsed::Haiku,
                fallback_count: 0,
                haiku_invocations: 1,
                sonnet_invocations: 0,
            });
        }

        if !haiku_gate.supported && !rule.high_risk {
            haiku_invocations += 1;
            let repair_request = model_repair_request(request, &haiku_response, &haiku_gate);
            if let Ok(repaired_response) = self
                .provider
                .structure(ModelStage::HaikuRepair, &repair_request)
                .await
            {
                let repaired_gate = verify_model_response(event, &repaired_response);
                if repaired_gate.supported
                    && !self.should_escalate_from_model(
                        event,
                        market_context,
                        &rule,
                        &repaired_response,
                    )
                {
                    return Ok(StructuringDecision {
                        rule,
                        model_response: Some(repaired_response),
                        model_tier_used: ModelTierUsed::Haiku,
                        fallback_count: 0,
                        haiku_invocations: 2,
                        sonnet_invocations: 0,
                    });
                }
            }
        }

        self.try_sonnet_or_fallback(event, market_context, rule, haiku_invocations)
            .await
    }

    fn rule_is_sufficient(
        &self,
        event: &RawIntelEvent,
        rule: &RuleAssessment,
        market_context: &MarketContextSnapshot,
    ) -> bool {
        if raw_quality_requires_model(event) {
            return false;
        }
        let gate_supported = rule
            .evidence_sentences
            .iter()
            .all(|sentence| !sentence.trim().is_empty());
        rule.confidence_score >= 0.82
            && gate_supported
            && !rule.high_risk
            && !matches!(
                rule.symbol_confidence_band,
                ConfidenceBand::Weak | ConfidenceBand::Low
            )
            && !market_context.status.is_pending_or_unavailable()
    }

    fn should_escalate_from_model(
        &self,
        event: &RawIntelEvent,
        market_context: &MarketContextSnapshot,
        rule: &RuleAssessment,
        response: &ModelStructuringResponse,
    ) -> bool {
        if !sonnet_admission_allows(event, market_context, rule, Some(response)) {
            return false;
        }
        if rule.high_risk || matches!(response.terminal_decision, TerminalDecision::Conflicted) {
            return true;
        }
        if numeric_snapshot_can_stop_at_primary(event, market_context, response) {
            return false;
        }
        let high_impact = is_high_impact_event(&rule.event_type)
            || is_high_impact_event(&response.event_type)
            || event.source_category.contains("exchange");
        let weak_raw_claim = raw_quality_requires_sonnet(event)
            && !matches!(
                response.terminal_decision,
                TerminalDecision::UnsupportedOrWeak
                    | TerminalDecision::IrrelevantOrNoise
                    | TerminalDecision::GeneralMarketContext
            );
        let weak_model_signal = response.confidence_score
            < self.policy.escalate_if_confidence_below
            || matches!(
                response.terminal_decision,
                TerminalDecision::UnsupportedOrWeak
            )
            || matches!(
                response.confidence_band,
                ConfidenceBand::Low | ConfidenceBand::Weak
            );
        let safety_escalation = high_impact && weak_model_signal;
        let audit_escalation =
            response.confidence_score < (self.policy.escalate_if_confidence_below + 0.10).min(1.0);
        let noncritical_escalation = weak_raw_claim || safety_escalation || audit_escalation;
        noncritical_escalation
            && within_sonnet_budget(&event.event_id, self.policy.sonnet_budget_ratio)
    }

    async fn try_sonnet_or_fallback(
        &self,
        event: &RawIntelEvent,
        market_context: &MarketContextSnapshot,
        rule: RuleAssessment,
        haiku_invocations: usize,
    ) -> AppResult<StructuringDecision> {
        if !sonnet_admission_allows(event, market_context, &rule, None) {
            return Ok(StructuringDecision {
                rule,
                model_response: None,
                model_tier_used: ModelTierUsed::FallbackOnly,
                fallback_count: 1,
                haiku_invocations,
                sonnet_invocations: 0,
            });
        }
        if !critical_rule_sonnet_path(&rule)
            && !within_sonnet_budget(&event.event_id, self.policy.sonnet_budget_ratio)
        {
            return Ok(StructuringDecision {
                rule,
                model_response: None,
                model_tier_used: ModelTierUsed::FallbackOnly,
                fallback_count: 1,
                haiku_invocations,
                sonnet_invocations: 0,
            });
        }

        let request = model_request(event, market_context, &rule);
        match self.provider.structure(ModelStage::Sonnet, &request).await {
            Ok(sonnet_response) => {
                let response = sonnet_response;
                let gate = verify_model_response(event, &response);
                if !gate.supported {
                    Ok(StructuringDecision {
                        rule,
                        model_response: None,
                        model_tier_used: ModelTierUsed::FallbackOnly,
                        fallback_count: 1,
                        haiku_invocations,
                        sonnet_invocations: 1,
                    })
                } else {
                    Ok(StructuringDecision {
                        rule,
                        model_response: Some(response),
                        model_tier_used: ModelTierUsed::Sonnet,
                        fallback_count: 0,
                        haiku_invocations,
                        sonnet_invocations: 1,
                    })
                }
            }
            Err(_) => Ok(StructuringDecision {
                rule,
                model_response: None,
                model_tier_used: ModelTierUsed::FallbackOnly,
                fallback_count: 1,
                haiku_invocations,
                sonnet_invocations: 1,
            }),
        }
    }
}

fn is_high_impact_event(event_type: &crate::models::output::EventType) -> bool {
    matches!(
        event_type,
        crate::models::output::EventType::Listing
            | crate::models::output::EventType::Delisting
            | crate::models::output::EventType::DepositWithdrawal
            | crate::models::output::EventType::Incident
            | crate::models::output::EventType::TokenUnlock
            | crate::models::output::EventType::FundingShift
            | crate::models::output::EventType::Regulatory
    )
}

fn critical_rule_sonnet_path(rule: &RuleAssessment) -> bool {
    rule.high_risk
}

fn within_sonnet_budget(raw_event_id: &str, ratio: f64) -> bool {
    if ratio <= 0.0 {
        return false;
    }
    if ratio >= 1.0 {
        return true;
    }
    let digest = sha256_hex(raw_event_id.as_bytes());
    let Some(prefix) = digest.get(..8) else {
        return false;
    };
    let Ok(value) = u32::from_str_radix(prefix, 16) else {
        return false;
    };
    let normalized = value as f64 / u32::MAX as f64;
    normalized < ratio
}

fn should_bypass_models_for_cost(
    event: &RawIntelEvent,
    market_context: &MarketContextSnapshot,
    rule: &RuleAssessment,
) -> bool {
    if rule.high_risk || is_official_or_trusted_notice(event) {
        return false;
    }

    if is_numeric_market_snapshot(event) && market_context.status.is_pending_or_unavailable() {
        return true;
    }

    let weak_general_item = rule.normalized_symbols.is_empty()
        && matches!(
            rule.terminal_decision,
            TerminalDecision::GeneralMarketContext
                | TerminalDecision::IrrelevantOrNoise
                | TerminalDecision::UnsupportedOrWeak
        );
    weak_general_item && is_low_quality_broad_scan(event)
}

fn sonnet_admission_allows(
    event: &RawIntelEvent,
    market_context: &MarketContextSnapshot,
    rule: &RuleAssessment,
    primary_response: Option<&ModelStructuringResponse>,
) -> bool {
    if is_single_numeric_funding_snapshot(event, rule, primary_response) {
        return false;
    }

    if is_numeric_market_snapshot(event) && market_context.status.is_pending_or_unavailable() {
        return false;
    }

    if is_numeric_market_snapshot(event)
        && !market_context.status.supports_numeric_snapshot_escalation()
    {
        return false;
    }

    if is_low_quality_broad_scan(event) && !rule.high_risk && !is_official_or_trusted_notice(event)
    {
        return false;
    }

    if let Some(response) = primary_response
        && is_low_value_terminal(response)
        && !rule.high_risk
        && !is_official_or_trusted_notice(event)
    {
        return false;
    }

    true
}

fn is_numeric_market_snapshot(event: &RawIntelEvent) -> bool {
    event.source_quality_or_unknown() == "market_snapshot"
        || event.content_quality_or_unknown() == "numeric_observation"
}

fn is_single_numeric_funding_snapshot(
    event: &RawIntelEvent,
    rule: &RuleAssessment,
    primary_response: Option<&ModelStructuringResponse>,
) -> bool {
    is_numeric_market_snapshot(event)
        && (matches!(
            rule.event_type,
            crate::models::output::EventType::FundingShift
        ) || primary_response.is_some_and(|response| {
            matches!(
                response.event_type,
                crate::models::output::EventType::FundingShift
            )
        }) || event
            .event_category_hint
            .as_deref()
            .is_some_and(is_derivatives_snapshot_hint)
            || is_derivatives_snapshot_source(event))
}

fn is_derivatives_snapshot_hint(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    value.contains("funding")
        || value.contains("open_interest")
        || value.contains("open interest")
        || value.contains("liquidation")
}

fn is_derivatives_snapshot_source(event: &RawIntelEvent) -> bool {
    let source_id = event.source_id.to_ascii_lowercase();
    let content_kind = event.content_kind_or_unknown().to_ascii_lowercase();
    source_id.contains("funding")
        || source_id.contains("open_interest")
        || source_id.contains("open-interest")
        || source_id.contains("liquidation")
        || content_kind.contains("derivatives")
}

fn is_single_numeric_funding_snapshot_response(
    event: &RawIntelEvent,
    response: &ModelStructuringResponse,
) -> bool {
    is_numeric_market_snapshot(event)
        && (matches!(
            response.event_type,
            crate::models::output::EventType::FundingShift
        ) || event
            .event_category_hint
            .as_deref()
            .is_some_and(is_derivatives_snapshot_hint)
            || is_derivatives_snapshot_source(event))
}

fn is_low_quality_broad_scan(event: &RawIntelEvent) -> bool {
    matches!(
        event.content_quality_or_unknown(),
        "title_only" | "metadata_fallback"
    ) || matches!(
        event.source_quality_or_unknown(),
        "global_symbol_scan" | "metadata_fallback"
    ) || matches!(
        event.source_relevance_scope.as_deref(),
        Some("global_symbol_scan")
    )
}

fn is_official_or_trusted_notice(event: &RawIntelEvent) -> bool {
    event.trust_tier == "T0"
        || event.source_id.contains("official")
        || event.source_id.contains("exchange")
        || event.source_category.contains("official")
        || event.source_category.contains("exchange")
        || event.source_category.contains("project")
        || matches!(
            event.source_quality.as_deref(),
            Some("official_source")
                | Some("official_notice")
                | Some("exchange_notice")
                | Some("project_notice")
                | Some("trusted_symbol_match")
        )
}

fn is_low_value_terminal(response: &ModelStructuringResponse) -> bool {
    matches!(
        response.terminal_decision,
        TerminalDecision::GeneralMarketContext
            | TerminalDecision::IrrelevantOrNoise
            | TerminalDecision::UnsupportedOrWeak
    )
}

fn model_request(
    event: &RawIntelEvent,
    market_context: &MarketContextSnapshot,
    rule: &RuleAssessment,
) -> ModelStructuringRequest {
    ModelStructuringRequest {
        raw_event_id: event.event_id.clone(),
        source_id: event.source_id.clone(),
        source_category: event.source_category.clone(),
        title: event.title.clone(),
        body: event.body.clone(),
        url: event.url.clone(),
        symbol_candidates: event.symbol_candidates.clone(),
        event_category_hint: event.event_category_hint.clone(),
        top50_relevance: event.top50_relevance.clone(),
        content_kind: event.content_kind_or_unknown().to_owned(),
        content_quality: event.content_quality_or_unknown().to_owned(),
        content_quality_score: event.content_quality_score_label(),
        source_quality: event.source_quality_or_unknown().to_owned(),
        source_relevance_scope: event.source_relevance_scope_or_unknown().to_owned(),
        rule_event_type: rule.event_type.clone(),
        rule_confidence: rule.confidence_score,
        evidence_candidates: rule.evidence_sentences.clone(),
        evidence_pack: build_evidence_pack(event),
        market_context_status: format!("{:?}", market_context.status),
        market_context_summary: market_context_summary(market_context, &event.symbol_candidates),
        repair_context: None,
    }
}

fn market_context_summary(market_context: &MarketContextSnapshot, symbols: &[String]) -> String {
    if !market_context.status.is_any_available() {
        return format!(
            "status={:?}; reason={}",
            market_context.status,
            market_context
                .unavailable_reason
                .as_deref()
                .unwrap_or("not_available")
        );
    }
    if market_context.symbol_summaries.is_empty() {
        return format!("status={:?}; symbol_summaries=empty", market_context.status);
    }

    let wanted = symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect::<BTreeSet<_>>();
    let mut summaries = Vec::new();
    for summary in &market_context.symbol_summaries {
        if !wanted.is_empty() && !wanted.contains(&summary.symbol.to_ascii_uppercase()) {
            continue;
        }
        summaries.push(format!(
            "symbol={} venue={} window={}..{} mid={} spread_bps={} trades={} volume={} completeness={}",
            summary.symbol,
            summary.venue,
            summary.window_start_ms,
            summary.window_end_ms,
            optional_f64(summary.mid_price),
            optional_f64(summary.spread_bps),
            summary.trade_count,
            summary.trade_volume,
            summary.slice_completeness
        ));
        if summaries.len() >= 8 {
            break;
        }
    }
    if summaries.is_empty() {
        return format!(
            "status={:?}; requested_symbols_missing_from_market_context",
            market_context.status
        );
    }
    summaries.join("; ")
}

fn optional_f64(value: Option<f64>) -> String {
    value
        .map(|number| format!("{number:.8}"))
        .unwrap_or_else(|| "null".to_owned())
}

fn numeric_snapshot_can_stop_at_primary(
    event: &RawIntelEvent,
    market_context: &MarketContextSnapshot,
    response: &ModelStructuringResponse,
) -> bool {
    if !is_numeric_market_snapshot(event) {
        return false;
    }
    if is_single_numeric_funding_snapshot_response(event, response) {
        return true;
    }
    if market_context.status.is_any_available() {
        return false;
    }
    matches!(
        response.terminal_decision,
        TerminalDecision::LowConfidenceStructured
            | TerminalDecision::UnsupportedOrWeak
            | TerminalDecision::GeneralMarketContext
            | TerminalDecision::IrrelevantOrNoise
    ) && response.confidence_score <= 0.75
}

fn raw_quality_requires_model(event: &RawIntelEvent) -> bool {
    matches!(
        event.source_quality_or_unknown(),
        "community_reaction" | "market_snapshot"
    ) || matches!(
        event.content_quality_or_unknown(),
        "title_only" | "metadata_fallback"
    )
}

fn raw_quality_requires_sonnet(event: &RawIntelEvent) -> bool {
    event.content_quality_score.is_some_and(|score| score < 45)
        || matches!(
            event.source_relevance_scope.as_deref(),
            Some("global_symbol_scan")
        )
}

fn model_repair_request(
    mut request: ModelStructuringRequest,
    response: &ModelStructuringResponse,
    gate: &crate::structuring::nli::EvidenceGateResult,
) -> ModelStructuringRequest {
    request.repair_context = Some(
        json!({
            "previous_response": response,
            "gate_supported": gate.supported,
            "contradiction_flags": gate.contradiction_flags
        })
        .to_string(),
    );
    request
}

pub fn force_rule_evidence_floor(event: &RawIntelEvent, decision: &mut StructuringDecision) {
    if decision.model_response.is_none() {
        let gate = verify_rule_evidence(event, &decision.rule.evidence_sentences);
        if !gate.supported && matches!(decision.rule.confidence_band, ConfidenceBand::High) {
            decision.rule.confidence_band = ConfidenceBand::Medium;
        }
    }
}
