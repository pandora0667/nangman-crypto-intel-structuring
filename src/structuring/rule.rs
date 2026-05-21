use crate::models::output::{
    ConfidenceBand, ContradictionFlag, EventType, RelevanceDecayHint, TerminalDecision,
};
use crate::models::raw::RawIntelEvent;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub struct RuleAssessment {
    pub event_type: EventType,
    pub normalized_symbols: Vec<String>,
    pub symbol_confidence_band: ConfidenceBand,
    pub confidence_score: f64,
    pub confidence_band: ConfidenceBand,
    pub evidence_sentences: Vec<String>,
    pub contradiction_flags: Vec<ContradictionFlag>,
    pub terminal_decision: TerminalDecision,
    pub high_risk: bool,
    pub topic_summary: String,
    pub stance_summary: String,
    pub risk_summary: String,
    pub regime_hint: String,
    pub scenario_hint: String,
    pub relevance_decay_hint: RelevanceDecayHint,
    pub novelty_score: f64,
}

pub fn assess(event: &RawIntelEvent) -> RuleAssessment {
    let text = format!("{} {}", event.title, event.body).to_ascii_lowercase();
    let event_type = classify_event_type(&text);
    let high_risk = matches!(
        event_type,
        EventType::Delisting
            | EventType::Incident
            | EventType::Regulatory
            | EventType::DepositWithdrawal
    );
    let normalized_symbols = normalize_symbols(&event.symbol_candidates);
    let symbol_confidence_band = if normalized_symbols.is_empty() {
        ConfidenceBand::Weak
    } else if event.source_category.contains("project") {
        ConfidenceBand::Strong
    } else {
        ConfidenceBand::Moderate
    };
    let evidence_sentences = evidence_candidates(event, &text);
    let contradiction_flags = contradiction_flags(event, &text, &evidence_sentences);
    let confidence_score = rule_confidence(
        &event_type,
        &symbol_confidence_band,
        &evidence_sentences,
        &contradiction_flags,
    );
    let confidence_band = confidence_band(confidence_score);
    let terminal_decision = terminal_decision(
        confidence_score,
        normalized_symbols.is_empty(),
        !contradiction_flags.is_empty(),
        high_risk,
    );

    RuleAssessment {
        topic_summary: topic_summary(event, &event_type),
        stance_summary: stance_summary(&event_type),
        risk_summary: risk_summary(&event_type),
        regime_hint: regime_hint(&event_type).to_owned(),
        scenario_hint: scenario_hint(&event_type).to_owned(),
        relevance_decay_hint: relevance_decay_hint(&event_type),
        novelty_score: novelty_score(&event_type, event),
        event_type,
        normalized_symbols,
        symbol_confidence_band,
        confidence_score,
        confidence_band,
        evidence_sentences,
        contradiction_flags,
        terminal_decision,
        high_risk,
    }
}

fn classify_event_type(text: &str) -> EventType {
    let rules = [
        (
            EventType::Delisting,
            ["delist", "remove trading pair", "trading pair removal"].as_slice(),
        ),
        (
            EventType::Listing,
            ["list", "listing", "new trading pair"].as_slice(),
        ),
        (
            EventType::DepositWithdrawal,
            [
                "deposit",
                "withdrawal",
                "suspend deposits",
                "suspend withdrawals",
            ]
            .as_slice(),
        ),
        (
            EventType::Incident,
            ["exploit", "hack", "breach", "incident", "outage", "halt"].as_slice(),
        ),
        (
            EventType::Regulatory,
            [
                "sec",
                "cftc",
                "lawsuit",
                "regulator",
                "regulatory",
                "sanction",
            ]
            .as_slice(),
        ),
        (EventType::TokenUnlock, ["unlock", "vesting"].as_slice()),
        (
            EventType::Governance,
            ["governance", "proposal", "vote"].as_slice(),
        ),
        (
            EventType::Partnership,
            ["partnership", "integrates", "collaboration"].as_slice(),
        ),
        (
            EventType::FundingShift,
            ["funding rate", "open interest", "liquidation"].as_slice(),
        ),
        (
            EventType::SocialBacklash,
            ["backlash", "criticism", "controversy"].as_slice(),
        ),
        (
            EventType::SocialHype,
            ["hype", "viral", "surge in mentions"].as_slice(),
        ),
        (
            EventType::MacroEvent,
            ["fomc", "inflation", "cpi", "rate cut", "fed"].as_slice(),
        ),
    ];
    rules
        .iter()
        .find_map(|(event_type, keywords)| {
            keywords
                .iter()
                .any(|keyword| text.contains(keyword))
                .then_some(event_type.clone())
        })
        .unwrap_or(EventType::Other)
}

fn normalize_symbols(symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| {
            !symbol.is_empty()
                && symbol.len() <= 12
                && symbol.chars().all(|ch| ch.is_ascii_alphanumeric())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn evidence_candidates(event: &RawIntelEvent, text: &str) -> Vec<String> {
    let source = if event.body.trim().is_empty() {
        event.title.as_str()
    } else {
        event.body.as_str()
    };
    source
        .split(['.', '\n'])
        .map(str::trim)
        .filter(|sentence| sentence.len() >= 12)
        .filter(|sentence| {
            let lower = sentence.to_ascii_lowercase();
            text_keywords(text)
                .iter()
                .any(|keyword| lower.contains(keyword))
        })
        .take(3)
        .map(ToOwned::to_owned)
        .collect()
}

fn text_keywords(text: &str) -> Vec<&'static str> {
    [
        "list",
        "delist",
        "deposit",
        "withdraw",
        "hack",
        "exploit",
        "regulat",
        "proposal",
        "unlock",
        "funding",
        "partnership",
    ]
    .into_iter()
    .filter(|keyword| text.contains(keyword))
    .collect()
}

fn contradiction_flags(
    event: &RawIntelEvent,
    text: &str,
    evidence: &[String],
) -> Vec<ContradictionFlag> {
    let mut flags = Vec::new();
    if event.title.to_ascii_lowercase().contains("rumor") || text.contains("unconfirmed") {
        flags.push(ContradictionFlag::RumorVsOfficial);
    }
    if !event.symbol_candidates.is_empty() && normalize_symbols(&event.symbol_candidates).len() > 3
    {
        flags.push(ContradictionFlag::SymbolAmbiguity);
    }
    if evidence.is_empty() && !matches!(classify_event_type(text), EventType::Other) {
        flags.push(ContradictionFlag::EvidenceWeak);
    }
    flags
}

fn rule_confidence(
    event_type: &EventType,
    symbol_band: &ConfidenceBand,
    evidence: &[String],
    contradictions: &[ContradictionFlag],
) -> f64 {
    let mut score: f64 = match event_type {
        EventType::Other => 0.35,
        EventType::Listing | EventType::Delisting | EventType::DepositWithdrawal => 0.72,
        EventType::Incident | EventType::Regulatory => 0.62,
        _ => 0.55,
    };
    if !evidence.is_empty() {
        score += 0.12;
    }
    if matches!(symbol_band, ConfidenceBand::Strong) {
        score += 0.08;
    }
    if !contradictions.is_empty() {
        score -= 0.2;
    }
    score.clamp(0.0, 1.0)
}

fn confidence_band(score: f64) -> ConfidenceBand {
    if score >= 0.8 {
        ConfidenceBand::High
    } else if score >= 0.55 {
        ConfidenceBand::Medium
    } else {
        ConfidenceBand::Low
    }
}

fn terminal_decision(
    score: f64,
    no_symbols: bool,
    conflicted: bool,
    high_risk: bool,
) -> TerminalDecision {
    if conflicted {
        TerminalDecision::Conflicted
    } else if score >= 0.8 {
        TerminalDecision::HighConfidenceStructured
    } else if no_symbols && score >= 0.45 {
        TerminalDecision::GeneralMarketContext
    } else if score >= 0.55 {
        TerminalDecision::LowConfidenceStructured
    } else if high_risk {
        TerminalDecision::UnsupportedOrWeak
    } else {
        TerminalDecision::IrrelevantOrNoise
    }
}

fn topic_summary(event: &RawIntelEvent, event_type: &EventType) -> String {
    format!("{}: {}", event_type_label(event_type), event.title)
}

fn stance_summary(event_type: &EventType) -> String {
    match event_type {
        EventType::Listing | EventType::Partnership => {
            "원문 기준 긍정적 해석 가능성은 있으나 매매 판단은 보류".to_owned()
        }
        EventType::Delisting | EventType::Incident | EventType::Regulatory => {
            "원문 기준 headline/event risk가 존재하며 관찰 대상으로 분류".to_owned()
        }
        _ => "원문 기반 일반 시장 정보로 분류".to_owned(),
    }
}

fn risk_summary(event_type: &EventType) -> String {
    match event_type {
        EventType::Delisting => "상장폐지 또는 거래쌍 제거 관련 유동성/심리 리스크".to_owned(),
        EventType::Incident => "보안/운영 사고 관련 신뢰도 및 변동성 리스크".to_owned(),
        EventType::Regulatory => "규제/법적 불확실성 리스크".to_owned(),
        EventType::DepositWithdrawal => {
            "입출금 운영 이벤트로 인한 단기 거래 불편 리스크".to_owned()
        }
        _ => "직접적인 고위험 신호는 rule layer에서 확인되지 않음".to_owned(),
    }
}

fn regime_hint(event_type: &EventType) -> &'static str {
    match event_type {
        EventType::MacroEvent | EventType::Regulatory => "risk_off",
        EventType::SocialHype => "social_mania",
        EventType::Other => "uncertain",
        _ => "event_driven",
    }
}

fn scenario_hint(event_type: &EventType) -> &'static str {
    match event_type {
        EventType::Incident | EventType::Regulatory | EventType::Delisting => {
            "structural_break_possible"
        }
        EventType::Other => "noise_only",
        _ => "watch_only",
    }
}

fn relevance_decay_hint(event_type: &EventType) -> RelevanceDecayHint {
    match event_type {
        EventType::Incident | EventType::Regulatory => RelevanceDecayHint::MultiDay,
        EventType::Listing | EventType::Delisting | EventType::DepositWithdrawal => {
            RelevanceDecayHint::Hours
        }
        EventType::MacroEvent => RelevanceDecayHint::Day,
        _ => RelevanceDecayHint::Hours,
    }
}

fn novelty_score(event_type: &EventType, event: &RawIntelEvent) -> f64 {
    if event.source_category.contains("official") || event.source_category.contains("project") {
        0.72
    } else if matches!(event_type, EventType::Other) {
        0.35
    } else {
        0.58
    }
}

fn event_type_label(event_type: &EventType) -> &'static str {
    match event_type {
        EventType::Listing => "listing",
        EventType::Delisting => "delisting",
        EventType::DepositWithdrawal => "deposit_withdrawal",
        EventType::Incident => "incident",
        EventType::Partnership => "partnership",
        EventType::TokenUnlock => "token_unlock",
        EventType::Governance => "governance",
        EventType::FundingShift => "funding_shift",
        EventType::MacroEvent => "macro_event",
        EventType::Regulatory => "regulatory",
        EventType::SocialBacklash => "social_backlash",
        EventType::SocialHype => "social_hype",
        EventType::Other => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delisting_is_high_risk() {
        let event = RawIntelEvent {
            event_id: "e".to_owned(),
            source_id: "s".to_owned(),
            source_category: "exchange_notice".to_owned(),
            source_name: "S".to_owned(),
            fetched_at_ms: 1,
            published_at_ms: Some(1),
            observed_at_ms: 1,
            language: "en".to_owned(),
            title: "Exchange will delist ABC".to_owned(),
            body: "Exchange will delist ABC spot trading pair.".to_owned(),
            url: "https://example.com".to_owned(),
            author_or_channel: None,
            trust_tier: "T0".to_owned(),
            cadence_tier: "low".to_owned(),
            content_hash: "h".to_owned(),
            dedup_key: "d".to_owned(),
            symbol_candidates: vec!["ABC".to_owned()],
            event_category_hint: None,
            top50_relevance: "relevant".to_owned(),
            content_kind: Some("exchange_notice".to_owned()),
            content_quality: Some("full_text".to_owned()),
            content_quality_score: Some(90),
            source_quality: Some("trusted_symbol_match".to_owned()),
            source_relevance_scope: Some("direct_asset".to_owned()),
            direct_asset_count: Some(1),
            matched_asset_count: Some(1),
            historical_source_depth: None,
            backfill_window_start_ms: None,
            backfill_window_end_ms: None,
            source_time_range_verified: None,
            schema_version: "raw_intel_event_v1".to_owned(),
        };
        let assessment = assess(&event);
        assert_eq!(assessment.event_type, EventType::Delisting);
        assert!(assessment.high_risk);
    }
}
