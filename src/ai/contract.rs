use crate::error::{AppError, AppResult};
use crate::models::output::{
    ConfidenceBand, ContradictionFlag, EventType, RelevanceDecayHint, TerminalDecision,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelStage {
    Haiku,
    HaikuRepair,
    Sonnet,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceSnippet {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelStructuringRequest {
    pub raw_event_id: String,
    pub source_id: String,
    pub source_category: String,
    pub title: String,
    pub body: String,
    pub url: String,
    pub symbol_candidates: Vec<String>,
    pub event_category_hint: Option<String>,
    pub top50_relevance: String,
    pub content_kind: String,
    pub content_quality: String,
    pub content_quality_score: String,
    pub source_quality: String,
    pub source_relevance_scope: String,
    pub rule_event_type: EventType,
    pub rule_confidence: f64,
    pub evidence_candidates: Vec<String>,
    pub evidence_pack: Vec<EvidenceSnippet>,
    pub market_context_status: String,
    pub market_context_summary: String,
    #[serde(default)]
    pub repair_context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelStructuringResponse {
    pub event_type: EventType,
    pub normalized_symbols: Vec<String>,
    pub symbol_confidence_band: ConfidenceBand,
    pub topic_summary: String,
    pub stance_summary: String,
    pub risk_summary: String,
    pub regime_hint: String,
    pub scenario_hint: String,
    pub confidence_band: ConfidenceBand,
    pub confidence_score: f64,
    pub novelty_score: f64,
    pub relevance_decay_hint: RelevanceDecayHint,
    pub contradiction_flags: Vec<ContradictionFlag>,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
    #[serde(default)]
    pub evidence_sentences: Vec<String>,
    pub terminal_decision: TerminalDecision,
}

impl ModelStructuringResponse {
    pub fn hydrate_evidence_sentences(
        &mut self,
        evidence_pack: &[EvidenceSnippet],
    ) -> AppResult<()> {
        if self.evidence_ids.is_empty() {
            return Ok(());
        }
        let by_id = evidence_pack
            .iter()
            .map(|snippet| (snippet.id.as_str(), snippet.text.as_str()))
            .collect::<BTreeMap<_, _>>();
        let mut sentences = Vec::new();
        for evidence_id in &self.evidence_ids {
            let Some(sentence) = by_id.get(evidence_id.as_str()) else {
                return Err(AppError::validation(format!(
                    "model returned unknown evidence_id {evidence_id}"
                )));
            };
            sentences.push((*sentence).to_owned());
        }
        self.evidence_sentences = sentences;
        Ok(())
    }

    pub fn validate_evidence_gate(&self) -> AppResult<()> {
        if !matches!(
            self.symbol_confidence_band,
            ConfidenceBand::Weak | ConfidenceBand::Moderate | ConfidenceBand::Strong
        ) {
            return Err(crate::error::AppError::validation(
                "model symbol_confidence_band must be weak/moderate/strong",
            ));
        }
        if !matches!(
            self.confidence_band,
            ConfidenceBand::Low | ConfidenceBand::Medium | ConfidenceBand::High
        ) {
            return Err(crate::error::AppError::validation(
                "model confidence_band must be low/medium/high",
            ));
        }
        if matches!(
            self.confidence_band,
            ConfidenceBand::High | ConfidenceBand::Strong
        ) && self.evidence_sentences.is_empty()
        {
            return Err(crate::error::AppError::validation(
                "model high confidence without evidence",
            ));
        }
        if self.confidence_score < 0.0 || self.confidence_score > 1.0 {
            return Err(crate::error::AppError::validation(
                "model confidence_score must be 0..1",
            ));
        }
        if self.novelty_score < 0.0 || self.novelty_score > 1.0 {
            return Err(crate::error::AppError::validation(
                "model novelty_score must be 0..1",
            ));
        }
        if matches!(
            self.terminal_decision,
            TerminalDecision::HighConfidenceStructured | TerminalDecision::Conflicted
        ) && self.evidence_sentences.is_empty()
        {
            return Err(crate::error::AppError::validation(
                "model terminal decision requires evidence",
            ));
        }
        if matches!(self.terminal_decision, TerminalDecision::QuarantineOnly) {
            return Err(crate::error::AppError::validation(
                "model must not emit quarantine_only",
            ));
        }
        for field in [
            &self.topic_summary,
            &self.stance_summary,
            &self.risk_summary,
            &self.regime_hint,
            &self.scenario_hint,
        ] {
            if field.trim().is_empty() {
                return Err(crate::error::AppError::validation(
                    "model text fields must not be empty",
                ));
            }
            if field.chars().count() > 512 {
                return Err(crate::error::AppError::validation(
                    "model text fields must be <=512 chars",
                ));
            }
        }
        for symbol in &self.normalized_symbols {
            validate_symbol(symbol)?;
        }
        Ok(())
    }
}

fn validate_symbol(symbol: &str) -> AppResult<()> {
    let trimmed = symbol.trim();
    if trimmed.is_empty() {
        return Err(crate::error::AppError::validation(
            "model normalized symbol must not be empty",
        ));
    }
    if trimmed.len() > 16 {
        return Err(crate::error::AppError::validation(
            "model normalized symbol must be <=16 chars",
        ));
    }
    if trimmed != trimmed.to_ascii_uppercase()
        || !trimmed
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
    {
        return Err(crate::error::AppError::validation(
            "model normalized symbol must be uppercase ASCII alphanumeric",
        ));
    }
    Ok(())
}

#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn structure(
        &self,
        stage: ModelStage,
        request: &ModelStructuringRequest,
    ) -> AppResult<ModelStructuringResponse>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_model_response_with_execution_only_terminal() {
        let mut response = response();
        response.terminal_decision = TerminalDecision::QuarantineOnly;

        assert!(response.validate_evidence_gate().is_err());
    }

    #[test]
    fn rejects_invalid_symbol_band_for_model_contract() {
        let mut response = response();
        response.symbol_confidence_band = ConfidenceBand::High;

        assert!(response.validate_evidence_gate().is_err());
    }

    #[test]
    fn rejects_noncanonical_symbol() {
        let mut response = response();
        response.normalized_symbols = vec!["btc-usd".to_owned()];

        assert!(response.validate_evidence_gate().is_err());
    }

    fn response() -> ModelStructuringResponse {
        ModelStructuringResponse {
            event_type: EventType::Incident,
            normalized_symbols: vec!["BTC".to_owned()],
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
            evidence_sentences: vec!["Evidence sentence".to_owned()],
            terminal_decision: TerminalDecision::HighConfidenceStructured,
        }
    }

    #[test]
    fn hydrates_evidence_ids_from_pack() {
        let mut response = response();
        response.evidence_ids = vec!["E1".to_owned()];
        response.evidence_sentences.clear();
        response
            .hydrate_evidence_sentences(&[EvidenceSnippet {
                id: "E1".to_owned(),
                text: "Exact source sentence".to_owned(),
            }])
            .unwrap();

        assert_eq!(response.evidence_sentences, vec!["Exact source sentence"]);
    }
}
