use crate::ai::contract::{
    ModelProvider, ModelStage, ModelStructuringRequest, ModelStructuringResponse,
};
use crate::error::{AppError, AppResult};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_bedrockruntime::Client;
use aws_smithy_types::Blob;
use aws_types::region::Region;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct BedrockConfig {
    pub enabled: bool,
    pub region: String,
    pub profile: Option<String>,
    pub primary_model_id: String,
    pub escalation_model_id: String,
    pub max_input_chars: usize,
    pub max_output_tokens: i32,
    pub temperature: f32,
}

pub struct BedrockModelProvider {
    config: BedrockConfig,
    client: Option<Client>,
}

impl BedrockModelProvider {
    pub async fn new(config: BedrockConfig) -> AppResult<Self> {
        if !config.enabled {
            return Ok(Self {
                config,
                client: None,
            });
        }
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()));
        if let Some(profile) = &config.profile {
            loader = loader.profile_name(profile);
        }
        let sdk_config = loader.load().await;
        Ok(Self {
            config,
            client: Some(Client::new(&sdk_config)),
        })
    }

    fn model_id(&self, stage: ModelStage) -> &str {
        match stage {
            ModelStage::Haiku | ModelStage::HaikuRepair => &self.config.primary_model_id,
            ModelStage::Sonnet => &self.config.escalation_model_id,
        }
    }
}

#[async_trait]
impl ModelProvider for BedrockModelProvider {
    async fn structure(
        &self,
        stage: ModelStage,
        request: &ModelStructuringRequest,
    ) -> AppResult<ModelStructuringResponse> {
        let Some(client) = &self.client else {
            return Err(AppError::bedrock("Bedrock model provider disabled"));
        };
        let static_prompt = build_static_prompt(stage);
        let dynamic_prompt = build_dynamic_prompt(stage, request, self.config.max_input_chars);
        let max_tokens = match stage {
            ModelStage::Haiku | ModelStage::HaikuRepair => self.config.max_output_tokens.min(800),
            ModelStage::Sonnet => self.config.max_output_tokens,
        };
        let body = json!({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": self.config.temperature,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": static_prompt,
                        "cache_control": {
                            "type": "ephemeral",
                            "ttl": "1h"
                        }
                    },
                    {
                        "type": "text",
                        "text": dynamic_prompt
                    }
                ]
            }],
            "output_config": {
                "format": {
                    "type": "json_schema",
                    "schema": model_response_schema()
                }
            }
        });
        let output = client
            .invoke_model()
            .model_id(self.model_id(stage))
            .content_type("application/json")
            .accept("application/json")
            .body(Blob::new(serde_json::to_vec(&body)?))
            .send()
            .await
            .map_err(|error| {
                AppError::bedrock(format!("invoke_model {}: {error}", self.model_id(stage)))
            })?;
        let bytes = output.body.as_ref();
        let response_json: serde_json::Value = serde_json::from_slice(bytes)?;
        let text = extract_anthropic_text(&response_json)?;
        let mut parsed: ModelStructuringResponse =
            serde_json::from_str(extract_json_object(&text)?)?;
        parsed.hydrate_evidence_sentences(&request.evidence_pack)?;
        parsed.validate_evidence_gate()?;
        Ok(parsed)
    }
}

fn build_static_prompt(stage: ModelStage) -> String {
    let role = match stage {
        ModelStage::Haiku => "Primary extractor. Be conservative and finish safe cases.",
        ModelStage::HaikuRepair => {
            "Repair extractor. Fix only schema, evidence IDs, and confidence consistency."
        }
        ModelStage::Sonnet => {
            "Escalation adjudicator. Resolve high impact ambiguity and produce a terminal decision."
        }
    };
    format!(
        r#"You are INTEL-L1, a crypto market intelligence structuring worker.

Role:
{role}

Task:
- Convert one raw market-intelligence item into one JSON object.
- Use only supplied evidence IDs from evidence_pack.
- Return evidence_ids, not free-form source quotes.
- If a symbol is not directly supported by evidence, use an empty normalized_symbols list or weak symbol confidence.
- If the item is useful but not coin-specific, use general_market_context.
- If evidence is weak and the item is low impact, use unsupported_or_weak or low_confidence_structured.
- Use content_quality, source_quality, and source_relevance_scope as routing evidence quality hints.
- Use market_context_summary when available; if it is unavailable, do not invent cross-market confirmation.
- Never make a high-confidence structured claim from title_only, metadata_fallback, unknown, or global_symbol_scan evidence unless the evidence_pack directly supports it.
- Never make a high-confidence funding_shift claim from a single numeric snapshot without market_context_summary support.
- Never produce trading, execution, sizing, entry, exit, or live-readiness recommendations.
- Do not infer buy/sell direction.

Allowed event_type values:
listing, delisting, deposit_withdrawal, incident, partnership, token_unlock, governance,
funding_shift, macro_event, regulatory, social_backlash, social_hype, other

Allowed symbol_confidence_band values:
weak, moderate, strong

Allowed confidence_band values:
low, medium, high

Allowed relevance_decay_hint values:
minutes, hours, day, multi_day, structural

Allowed contradiction_flags values:
time_mismatch, symbol_ambiguity, source_claim_conflict, rumor_vs_official,
title_body_mismatch, evidence_weak

Allowed terminal_decision values:
high_confidence_structured, low_confidence_structured, general_market_context,
conflicted, unsupported_or_weak, irrelevant_or_noise
"#
    )
}

fn build_dynamic_prompt(
    stage: ModelStage,
    request: &ModelStructuringRequest,
    max_input_chars: usize,
) -> String {
    let body = if request.body.chars().count() > max_input_chars {
        request
            .body
            .chars()
            .take(max_input_chars)
            .collect::<String>()
    } else {
        request.body.clone()
    };
    let body_section = match stage {
        ModelStage::Sonnet => format!("body_excerpt: {}\n", body),
        ModelStage::Haiku | ModelStage::HaikuRepair => String::new(),
    };
    let repair_context = request
        .repair_context
        .as_ref()
        .map(|value| format!("repair_context: {value}\n"))
        .unwrap_or_default();
    format!(
        r#"Input:
raw_event_id: {}
source_id: {}
source_category: {}
url: {}
symbol_candidates: {:?}
event_category_hint: {:?}
top50_relevance: {}
content_kind: {}
content_quality: {}
content_quality_score: {}
source_quality: {}
source_relevance_scope: {}
rule_event_type: {:?}
rule_confidence: {}
market_context_status: {}
market_context_summary: {}
evidence_candidates: {:?}
evidence_pack: {:?}
title: {}
{}{}"#,
        request.raw_event_id,
        request.source_id,
        request.source_category,
        request.url,
        request.symbol_candidates,
        request.event_category_hint,
        request.top50_relevance,
        request.content_kind,
        request.content_quality,
        request.content_quality_score,
        request.source_quality,
        request.source_relevance_scope,
        request.rule_event_type,
        request.rule_confidence,
        request.market_context_status,
        request.market_context_summary,
        request.evidence_candidates,
        request.evidence_pack,
        request.title,
        body_section,
        repair_context
    )
}

fn model_response_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "event_type": {
                "type": "string",
                "enum": [
                    "listing",
                    "delisting",
                    "deposit_withdrawal",
                    "incident",
                    "partnership",
                    "token_unlock",
                    "governance",
                    "funding_shift",
                    "macro_event",
                    "regulatory",
                    "social_backlash",
                    "social_hype",
                    "other"
                ]
            },
            "normalized_symbols": {
                "type": "array",
                "items": {"type": "string"}
            },
            "symbol_confidence_band": {
                "type": "string",
                "enum": ["weak", "moderate", "strong"]
            },
            "topic_summary": {"type": "string"},
            "stance_summary": {"type": "string"},
            "risk_summary": {"type": "string"},
            "regime_hint": {"type": "string"},
            "scenario_hint": {"type": "string"},
            "confidence_band": {
                "type": "string",
                "enum": ["low", "medium", "high"]
            },
            "confidence_score": {"type": "number"},
            "novelty_score": {"type": "number"},
            "relevance_decay_hint": {
                "type": "string",
                "enum": ["minutes", "hours", "day", "multi_day", "structural"]
            },
            "contradiction_flags": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": [
                        "time_mismatch",
                        "symbol_ambiguity",
                        "source_claim_conflict",
                        "rumor_vs_official",
                        "title_body_mismatch",
                        "evidence_weak"
                    ]
                }
            },
            "evidence_ids": {
                "type": "array",
                "items": {"type": "string"}
            },
            "terminal_decision": {
                "type": "string",
                "enum": [
                    "high_confidence_structured",
                    "low_confidence_structured",
                    "general_market_context",
                    "conflicted",
                    "unsupported_or_weak",
                    "irrelevant_or_noise"
                ]
            }
        },
        "required": [
            "event_type",
            "normalized_symbols",
            "symbol_confidence_band",
            "topic_summary",
            "stance_summary",
            "risk_summary",
            "regime_hint",
            "scenario_hint",
            "confidence_band",
            "confidence_score",
            "novelty_score",
            "relevance_decay_hint",
            "contradiction_flags",
            "evidence_ids",
            "terminal_decision"
        ]
    })
}

fn extract_anthropic_text(response_json: &serde_json::Value) -> AppResult<String> {
    response_json
        .get("content")
        .and_then(|value| value.as_array())
        .and_then(|items| {
            items
                .iter()
                .find_map(|item| item.get("text").and_then(|text| text.as_str()))
        })
        .map(ToOwned::to_owned)
        .ok_or_else(|| AppError::bedrock("Bedrock response did not contain text content"))
}

fn extract_json_object(text: &str) -> AppResult<&str> {
    let start = text
        .find('{')
        .ok_or_else(|| AppError::bedrock("model response does not contain JSON object"))?;
    let end = text
        .rfind('}')
        .ok_or_else(|| AppError::bedrock("model response does not contain JSON object end"))?;
    if end < start {
        return Err(AppError::bedrock(
            "model response JSON object bounds invalid",
        ));
    }
    Ok(&text[start..=end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_json_from_wrapped_model_text() {
        assert_eq!(
            extract_json_object("```json\n{\"a\":1}\n```").unwrap(),
            "{\"a\":1}"
        );
    }
}
