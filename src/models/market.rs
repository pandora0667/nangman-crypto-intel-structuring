use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MarketL1IndexPointer {
    pub schema_version: String,
    pub canonical_manifest_key: String,
    pub l1_run_id: String,
    pub status: String,
    pub finished_at_ms: i64,
    pub input_time_range_start_ms: i64,
    pub input_time_range_end_ms: i64,
    #[serde(default)]
    pub indexed_window_start_ms: Option<i64>,
    #[serde(default)]
    pub indexed_window_end_ms: Option<i64>,
    pub schema_version_emitted: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MarketL1Manifest {
    pub schema_version: String,
    pub l1_run_id: String,
    pub status: String,
    pub input_time_range_start_ms: i64,
    pub input_time_range_end_ms: i64,
    pub schema_version_emitted: String,
    pub report_key: String,
    pub output_object_keys: Vec<String>,
    #[serde(default)]
    pub market_data_quality_summary_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_summary_key: Option<String>,
    #[serde(default)]
    pub market_regime_context_key: Option<String>,
    #[serde(default)]
    pub symbol_universe_snapshot_key: Option<String>,
    pub output_record_count: usize,
    pub slice_count_total: usize,
    pub finished_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MarketL1Report {
    pub schema_version: String,
    pub l1_run_id: String,
    pub input_time_range_start_ms: i64,
    pub input_time_range_end_ms: i64,
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_schema_versions: Vec<String>,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_object_keys: Vec<String>,
    pub input_record_count: usize,
    pub duplicate_event_count: usize,
    pub invalid_event_count: usize,
    pub payload_hash_mismatch_count: usize,
    pub slice_count_total: usize,
    pub slice_count_complete: usize,
    pub slice_count_partial: usize,
    pub slice_count_incomplete: usize,
    pub slice_count_reference_only: usize,
    pub output_object_keys: Vec<String>,
    #[serde(default)]
    pub market_data_quality_summary_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_key: Option<String>,
    #[serde(default)]
    pub market_feature_delta_summary_key: Option<String>,
    #[serde(default)]
    pub market_regime_context_key: Option<String>,
    #[serde(default)]
    pub symbol_universe_snapshot_key: Option<String>,
    pub status: String,
    pub failure_reason: Option<String>,
    pub manifest_key: String,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub runner_git_sha: String,
    pub runner_git_dirty: bool,
    pub runner_build_profile: String,
    pub schema_version_emitted: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketContextSnapshot {
    pub status: MarketContextStatus,
    pub basis_timestamp_ms: Option<i64>,
    pub basis_kind: String,
    pub window_start_ms: Option<i64>,
    pub window_end_ms: Option<i64>,
    pub manifest_key: Option<String>,
    pub output_object_keys: Vec<String>,
    pub market_data_quality_summary_key: Option<String>,
    pub market_feature_delta_key: Option<String>,
    pub market_feature_delta_summary_key: Option<String>,
    pub market_regime_context_key: Option<String>,
    pub symbol_universe_snapshot_key: Option<String>,
    pub symbol_summaries: Vec<MarketSymbolSummary>,
    pub unavailable_reason: Option<String>,
}

impl MarketContextSnapshot {
    pub fn unavailable(reason: impl Into<String>, basis_kind: impl Into<String>) -> Self {
        Self {
            status: MarketContextStatus::Unavailable,
            basis_timestamp_ms: None,
            basis_kind: basis_kind.into(),
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
            unavailable_reason: Some(reason.into()),
        }
    }

    pub fn pending(reason: impl Into<String>, basis_timestamp_ms: i64, basis_kind: &str) -> Self {
        Self {
            status: MarketContextStatus::Pending,
            basis_timestamp_ms: Some(basis_timestamp_ms),
            basis_kind: basis_kind.to_owned(),
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
            unavailable_reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketContextStatus {
    Available,
    AvailableSymbolContext,
    AvailableGeneralContext,
    NearestAvailable,
    SymbolContextOnly,
    StaleButUsable,
    Pending,
    Unavailable,
}

impl MarketContextStatus {
    pub fn is_any_available(&self) -> bool {
        matches!(
            self,
            Self::Available
                | Self::AvailableSymbolContext
                | Self::AvailableGeneralContext
                | Self::NearestAvailable
                | Self::SymbolContextOnly
                | Self::StaleButUsable
        )
    }

    pub fn is_symbol_usable(&self) -> bool {
        matches!(
            self,
            Self::Available
                | Self::AvailableSymbolContext
                | Self::NearestAvailable
                | Self::SymbolContextOnly
                | Self::StaleButUsable
        )
    }

    pub fn is_pending_or_unavailable(&self) -> bool {
        matches!(self, Self::Pending | Self::Unavailable)
    }

    pub fn is_stale_but_usable(&self) -> bool {
        matches!(self, Self::StaleButUsable)
    }

    pub fn supports_numeric_snapshot_escalation(&self) -> bool {
        matches!(
            self,
            Self::Available
                | Self::AvailableSymbolContext
                | Self::NearestAvailable
                | Self::SymbolContextOnly
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketSymbolSummary {
    pub symbol: String,
    pub venue: String,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub mid_price: Option<f64>,
    pub spread_bps: Option<f64>,
    pub trade_count: i64,
    pub trade_volume: f64,
    pub slice_completeness: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketL1ReadPlan {
    pub l1_run_id: String,
    pub manifest_key: String,
    pub report_key: String,
    pub output_object_keys: Vec<String>,
    pub market_data_quality_summary_key: Option<String>,
    pub market_feature_delta_key: Option<String>,
    pub market_feature_delta_summary_key: Option<String>,
    pub market_regime_context_key: Option<String>,
    pub symbol_universe_snapshot_key: Option<String>,
    pub input_time_range_start_ms: i64,
    pub input_time_range_end_ms: i64,
}
