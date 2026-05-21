use crate::error::{AppError, AppResult};
use crate::hash::sha256_prefixed;
use crate::models::constants::{RAW_EVENT_SCHEMA_VERSION, RAW_POINTER_SCHEMA_VERSION};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RawIntelEventCreatedPointer {
    pub schema_version: String,
    pub event_id: String,
    pub source_id: String,
    pub source_category: String,
    pub fetched_at_ms: i64,
    pub published_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub content_hash: String,
    pub dedup_key: String,
    #[serde(default)]
    pub symbol_candidates: Vec<String>,
    pub top50_relevance: String,
    pub storage_ref: RawIntelEventStorageRef,
}

impl RawIntelEventCreatedPointer {
    pub fn parse(bytes: &[u8]) -> AppResult<Self> {
        let pointer: Self = serde_json::from_slice(bytes)?;
        pointer.validate()?;
        Ok(pointer)
    }

    pub fn validate(&self) -> AppResult<()> {
        if self.schema_version != RAW_POINTER_SCHEMA_VERSION {
            return Err(AppError::validation(format!(
                "raw pointer schema mismatch: expected {RAW_POINTER_SCHEMA_VERSION}, got {}",
                self.schema_version
            )));
        }
        if self.event_id.trim().is_empty() {
            return Err(AppError::validation("raw pointer event_id is required"));
        }
        self.storage_ref.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RawIntelEventStorageRef {
    pub kind: String,
    pub endpoint_alias: String,
    pub bucket: String,
    pub key: String,
    pub line_number: usize,
    pub byte_offset: usize,
    pub byte_length: usize,
    pub content_sha256: String,
}

impl RawIntelEventStorageRef {
    pub fn validate(&self) -> AppResult<()> {
        if self.kind != "rustfs_jsonl_record" {
            return Err(AppError::validation(format!(
                "unsupported raw storage kind: {}",
                self.kind
            )));
        }
        if self.bucket.trim().is_empty() || self.key.trim().is_empty() {
            return Err(AppError::validation("raw storage bucket/key are required"));
        }
        if self.byte_length == 0 {
            return Err(AppError::validation(
                "raw storage byte_length must be positive",
            ));
        }
        if !self.content_sha256.starts_with("sha256:") {
            return Err(AppError::validation(
                "raw storage content_sha256 must be sha256-prefixed",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct RawIntelEvent {
    pub event_id: String,
    pub source_id: String,
    pub source_category: String,
    pub source_name: String,
    pub fetched_at_ms: i64,
    pub published_at_ms: Option<i64>,
    pub observed_at_ms: i64,
    pub language: String,
    pub title: String,
    pub body: String,
    pub url: String,
    pub author_or_channel: Option<String>,
    pub trust_tier: String,
    pub cadence_tier: String,
    pub content_hash: String,
    pub dedup_key: String,
    #[serde(default)]
    pub symbol_candidates: Vec<String>,
    pub event_category_hint: Option<String>,
    pub top50_relevance: String,
    #[serde(default)]
    pub content_kind: Option<String>,
    #[serde(default)]
    pub content_quality: Option<String>,
    #[serde(default)]
    pub content_quality_score: Option<u8>,
    #[serde(default)]
    pub source_quality: Option<String>,
    #[serde(default)]
    pub source_relevance_scope: Option<String>,
    #[serde(default)]
    pub direct_asset_count: Option<usize>,
    #[serde(default)]
    pub matched_asset_count: Option<usize>,
    #[serde(default)]
    pub historical_source_depth: Option<String>,
    #[serde(default)]
    pub backfill_window_start_ms: Option<i64>,
    #[serde(default)]
    pub backfill_window_end_ms: Option<i64>,
    #[serde(default)]
    pub source_time_range_verified: Option<bool>,
    pub schema_version: String,
}

impl RawIntelEvent {
    pub fn parse_verified(bytes: &[u8], pointer: &RawIntelEventCreatedPointer) -> AppResult<Self> {
        let actual_sha = sha256_prefixed(bytes);
        if actual_sha != pointer.storage_ref.content_sha256 {
            return Err(AppError::validation(format!(
                "raw record sha mismatch for {}",
                pointer.event_id
            )));
        }
        let event: Self = serde_json::from_slice(bytes)?;
        event.validate_against_pointer(pointer)?;
        Ok(event)
    }

    fn validate_against_pointer(&self, pointer: &RawIntelEventCreatedPointer) -> AppResult<()> {
        if self.schema_version != RAW_EVENT_SCHEMA_VERSION {
            return Err(AppError::validation(format!(
                "raw event schema mismatch: expected {RAW_EVENT_SCHEMA_VERSION}, got {}",
                self.schema_version
            )));
        }
        if self.event_id != pointer.event_id {
            return Err(AppError::validation(format!(
                "raw event id mismatch: pointer={} raw={}",
                pointer.event_id, self.event_id
            )));
        }
        if self.content_hash != pointer.content_hash {
            return Err(AppError::validation(format!(
                "raw content_hash mismatch for {}",
                self.event_id
            )));
        }
        Ok(())
    }

    pub fn evidence_text(&self, max_body_chars: usize) -> String {
        let body = if self.body.chars().count() > max_body_chars {
            self.body.chars().take(max_body_chars).collect::<String>()
        } else {
            self.body.clone()
        };
        format!("{}\n\n{}", self.title, body)
    }

    pub fn content_kind_or_unknown(&self) -> &str {
        self.content_kind.as_deref().unwrap_or("unknown")
    }

    pub fn content_quality_or_unknown(&self) -> &str {
        self.content_quality.as_deref().unwrap_or("unknown")
    }

    pub fn content_quality_score_label(&self) -> String {
        self.content_quality_score
            .map(|score| score.to_string())
            .unwrap_or_else(|| "unknown".to_owned())
    }

    pub fn source_quality_or_unknown(&self) -> &str {
        self.source_quality.as_deref().unwrap_or("unknown")
    }

    pub fn source_relevance_scope_or_unknown(&self) -> &str {
        self.source_relevance_scope.as_deref().unwrap_or("unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_wrong_pointer_schema() {
        let payload = br#"{
            "schema_version":"bad",
            "event_id":"e1",
            "source_id":"s",
            "source_category":"news",
            "fetched_at_ms":1,
            "published_at_ms":null,
            "created_at_ms":1,
            "content_hash":"h",
            "dedup_key":"d",
            "symbol_candidates":[],
            "top50_relevance":"unknown",
            "storage_ref":{"kind":"rustfs_jsonl_record","endpoint_alias":"rustfs-primary","bucket":"b","key":"k","line_number":1,"byte_offset":0,"byte_length":1,"content_sha256":"sha256:abc"}
        }"#;
        assert!(RawIntelEventCreatedPointer::parse(payload).is_err());
    }

    #[test]
    fn verifies_raw_event_hash_and_pointer_identity() {
        let raw = br#"{"event_id":"e1","source_id":"s","source_category":"news","source_name":"S","fetched_at_ms":1,"published_at_ms":null,"observed_at_ms":1,"language":"en","title":"T","body":"B","url":"https://example.com","author_or_channel":null,"trust_tier":"T1","cadence_tier":"low","content_hash":"content-hash","dedup_key":"d","symbol_candidates":[],"event_category_hint":null,"top50_relevance":"unknown","schema_version":"raw_intel_event_v1"}"#;
        let pointer = RawIntelEventCreatedPointer {
            schema_version: RAW_POINTER_SCHEMA_VERSION.to_owned(),
            event_id: "e1".to_owned(),
            source_id: "s".to_owned(),
            source_category: "news".to_owned(),
            fetched_at_ms: 1,
            published_at_ms: None,
            created_at_ms: 1,
            content_hash: "content-hash".to_owned(),
            dedup_key: "d".to_owned(),
            symbol_candidates: Vec::new(),
            top50_relevance: "unknown".to_owned(),
            storage_ref: RawIntelEventStorageRef {
                kind: "rustfs_jsonl_record".to_owned(),
                endpoint_alias: "rustfs-primary".to_owned(),
                bucket: "b".to_owned(),
                key: "k".to_owned(),
                line_number: 1,
                byte_offset: 0,
                byte_length: raw.len(),
                content_sha256: sha256_prefixed(raw),
            },
        };

        let parsed = RawIntelEvent::parse_verified(raw, &pointer).unwrap();
        assert_eq!(parsed.event_id, "e1");
    }
}
