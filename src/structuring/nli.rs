use crate::ai::contract::ModelStructuringResponse;
use crate::models::output::{ConfidenceBand, ContradictionFlag, EventType, TerminalDecision};
use crate::models::raw::RawIntelEvent;

#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceGateResult {
    pub supported: bool,
    pub contradiction_flags: Vec<ContradictionFlag>,
}

pub fn verify_rule_evidence(
    event: &RawIntelEvent,
    evidence_sentences: &[String],
) -> EvidenceGateResult {
    let source = event.evidence_text(50_000).to_ascii_lowercase();
    let mut flags = Vec::new();
    for sentence in evidence_sentences {
        if !source.contains(&sentence.to_ascii_lowercase()) {
            flags.push(ContradictionFlag::EvidenceWeak);
        }
    }
    EvidenceGateResult {
        supported: flags.is_empty(),
        contradiction_flags: flags,
    }
}

pub fn verify_model_response(
    event: &RawIntelEvent,
    response: &ModelStructuringResponse,
) -> EvidenceGateResult {
    let mut result = verify_rule_evidence(event, &response.evidence_sentences);
    if matches!(
        response.confidence_band,
        ConfidenceBand::High | ConfidenceBand::Strong
    ) && response.evidence_sentences.is_empty()
    {
        result.supported = false;
        result
            .contradiction_flags
            .push(ContradictionFlag::EvidenceWeak);
    }
    if response.normalized_symbols.is_empty()
        && matches!(response.symbol_confidence_band, ConfidenceBand::Strong)
    {
        result.supported = false;
        result
            .contradiction_flags
            .push(ContradictionFlag::SymbolAmbiguity);
    }
    let source = event.evidence_text(50_000).to_ascii_uppercase();
    let candidates = event
        .symbol_candidates
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect::<std::collections::BTreeSet<_>>();
    for symbol in &response.normalized_symbols {
        let normalized = symbol.trim().to_ascii_uppercase();
        if !candidates.contains(&normalized) && !source.contains(&normalized) {
            result.supported = false;
            result
                .contradiction_flags
                .push(ContradictionFlag::SymbolAmbiguity);
        }
    }
    if is_single_numeric_snapshot(event)
        && matches!(response.event_type, EventType::FundingShift)
        && matches!(
            response.terminal_decision,
            TerminalDecision::HighConfidenceStructured | TerminalDecision::Conflicted
        )
    {
        result.supported = false;
        result
            .contradiction_flags
            .push(ContradictionFlag::EvidenceWeak);
    }
    result
}

fn is_single_numeric_snapshot(event: &RawIntelEvent) -> bool {
    event.source_quality_or_unknown() == "market_snapshot"
        || event.content_quality_or_unknown() == "numeric_observation"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::output::{ConfidenceBand, EventType, RelevanceDecayHint, TerminalDecision};

    #[test]
    fn detects_unsupported_evidence_sentence() {
        let event = event();
        let result = verify_rule_evidence(&event, &["Missing sentence".to_owned()]);
        assert!(!result.supported);
    }

    #[test]
    fn detects_symbol_not_supported_by_candidates_or_source_text() {
        let event = event();
        let response = ModelStructuringResponse {
            event_type: EventType::Incident,
            normalized_symbols: vec!["XYZ".to_owned()],
            symbol_confidence_band: ConfidenceBand::Strong,
            topic_summary: "Incident confirmed".to_owned(),
            stance_summary: "Evidence supports an incident classification".to_owned(),
            risk_summary: "Operational risk is present".to_owned(),
            regime_hint: "event_driven".to_owned(),
            scenario_hint: "watch_only".to_owned(),
            confidence_band: ConfidenceBand::High,
            confidence_score: 0.9,
            novelty_score: 0.8,
            relevance_decay_hint: RelevanceDecayHint::MultiDay,
            contradiction_flags: Vec::new(),
            evidence_ids: Vec::new(),
            evidence_sentences: vec!["Real sentence".to_owned()],
            terminal_decision: TerminalDecision::HighConfidenceStructured,
        };

        let result = verify_model_response(&event, &response);

        assert!(!result.supported);
        assert!(
            result
                .contradiction_flags
                .contains(&ContradictionFlag::SymbolAmbiguity)
        );
    }

    #[test]
    fn rejects_high_confidence_funding_shift_from_single_numeric_snapshot() {
        let mut event = event();
        event.source_quality = Some("market_snapshot".to_owned());
        event.content_quality = Some("numeric_observation".to_owned());
        event.title = "Binance USD-M open interest ABCUSDT".to_owned();
        event.body = r#"{"symbol":"ABCUSDT","open_interest":"1042","event_time_ms":1}"#.to_owned();
        let mut response = response();
        response.event_type = EventType::FundingShift;
        response.terminal_decision = TerminalDecision::HighConfidenceStructured;
        response.evidence_sentences = vec![event.body.clone()];

        let result = verify_model_response(&event, &response);

        assert!(!result.supported);
        assert!(
            result
                .contradiction_flags
                .contains(&ContradictionFlag::EvidenceWeak)
        );
    }

    fn response() -> ModelStructuringResponse {
        ModelStructuringResponse {
            event_type: EventType::Incident,
            normalized_symbols: vec!["ABC".to_owned()],
            symbol_confidence_band: ConfidenceBand::Strong,
            topic_summary: "Incident confirmed".to_owned(),
            stance_summary: "Evidence supports an incident classification".to_owned(),
            risk_summary: "Operational risk is present".to_owned(),
            regime_hint: "event_driven".to_owned(),
            scenario_hint: "watch_only".to_owned(),
            confidence_band: ConfidenceBand::High,
            confidence_score: 0.9,
            novelty_score: 0.8,
            relevance_decay_hint: RelevanceDecayHint::MultiDay,
            contradiction_flags: Vec::new(),
            evidence_ids: Vec::new(),
            evidence_sentences: vec!["Real sentence".to_owned()],
            terminal_decision: TerminalDecision::HighConfidenceStructured,
        }
    }

    fn event() -> RawIntelEvent {
        RawIntelEvent {
            event_id: "e".to_owned(),
            source_id: "s".to_owned(),
            source_category: "news".to_owned(),
            source_name: "S".to_owned(),
            fetched_at_ms: 1,
            published_at_ms: None,
            observed_at_ms: 1,
            language: "en".to_owned(),
            title: "A title".to_owned(),
            body: "Real sentence.".to_owned(),
            url: "u".to_owned(),
            author_or_channel: None,
            trust_tier: "T1".to_owned(),
            cadence_tier: "low".to_owned(),
            content_hash: "h".to_owned(),
            dedup_key: "d".to_owned(),
            symbol_candidates: vec!["ABC".to_owned()],
            event_category_hint: None,
            top50_relevance: "unknown".to_owned(),
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
}
