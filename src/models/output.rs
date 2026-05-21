use crate::models::constants::{
    CONTEXT_FLAG_SCHEMA_VERSION, HEALTH_EVENT_SCHEMA_VERSION, INDEX_POINTER_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION, PACKET_REVISION_INDEX_SCHEMA_VERSION, QUARANTINE_SCHEMA_VERSION,
    STORY_CLUSTER_SCHEMA_VERSION, STORY_MEMBER_SCHEMA_VERSION, STRUCTURED_PACKET_SCHEMA_VERSION,
    STRUCTURED_POINTER_SCHEMA_VERSION,
};
use crate::models::market::{MarketContextSnapshot, MarketContextStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoryCluster {
    pub cluster_id: String,
    pub source_event_ids: Vec<String>,
    pub story_hint_key: String,
    pub primary_topic: String,
    pub secondary_topics: Vec<String>,
    pub related_symbols: Vec<String>,
    pub source_count: usize,
    pub trust_mix: String,
    pub first_published_at_ms: Option<i64>,
    pub last_updated_at_ms: i64,
    pub novelty_score: f64,
    pub conflict_level: ConflictLevel,
    pub conflicting_source_ids: Vec<String>,
    pub resolution_summary: String,
    pub schema_version: String,
}

impl StoryCluster {
    pub fn schema() -> String {
        STORY_CLUSTER_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictLevel {
    None,
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuredIntelPacket {
    pub packet_id: String,
    #[serde(default)]
    pub packet_family_id: String,
    #[serde(default)]
    pub raw_event_id: String,
    #[serde(default)]
    pub event_timestamp_ms: i64,
    #[serde(default)]
    pub revision: u32,
    #[serde(default)]
    pub supersedes_packet_id: Option<String>,
    pub cluster_id: String,
    pub source_event_ids: Vec<String>,
    pub published_at_ms: Option<i64>,
    pub fetched_at_ms: i64,
    pub structured_at_ms: i64,
    pub decision_available_at_ms: i64,
    pub normalized_symbols: Vec<String>,
    pub symbol_confidence_band: ConfidenceBand,
    pub symbol_resolution_trace: Vec<SymbolResolutionTrace>,
    pub event_type: EventType,
    pub topic_summary: String,
    pub stance_summary: String,
    pub risk_summary: String,
    pub regime_hint: String,
    pub scenario_hint: String,
    pub confidence_band: ConfidenceBand,
    pub novelty_score: f64,
    pub time_relevance_window: TimeRelevanceWindow,
    pub contradiction_flags: Vec<ContradictionFlag>,
    pub source_quality_summary: String,
    pub source_independence_summary: SourceIndependenceSummary,
    pub text_evidence: Vec<TextEvidence>,
    pub metric_evidence: Vec<MetricEvidence>,
    pub evidence_quality_reasons: Vec<EvidenceQualityReason>,
    pub market_context_status: MarketContextStatus,
    #[serde(default)]
    pub market_context_retry_after_ms: Option<i64>,
    #[serde(default)]
    pub market_context_expire_at_ms: Option<i64>,
    #[serde(default)]
    pub market_context_terminal_reason: Option<String>,
    pub market_context_ref: MarketContextRef,
    pub model_tier_used: ModelTierUsed,
    pub terminal_decision: TerminalDecision,
    pub evidence_sentences: Vec<String>,
    pub market_context: MarketContextSnapshot,
    pub schema_version: String,
}

impl StructuredIntelPacket {
    pub fn schema() -> String {
        STRUCTURED_PACKET_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SymbolResolutionTrace {
    pub raw_mentions: Vec<String>,
    pub resolved_project: Option<String>,
    pub resolved_asset: Option<String>,
    pub canonical_symbol: Option<String>,
    pub venue_symbols: Vec<String>,
    pub mapping_confidence: ConfidenceBand,
    pub ambiguity_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceIndependenceSummary {
    pub source_event_count: usize,
    pub independent_source_count: usize,
    pub official_source_present: bool,
    pub duplicate_content_hashes: Vec<String>,
    pub syndicated_from: Option<String>,
    pub original_source_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TextEvidence {
    pub evidence_text: String,
    pub source_event_id: String,
    pub source_id: String,
    pub published_at_ms: Option<i64>,
    pub evidence_kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricEvidence {
    pub metric_name: String,
    pub symbol: Option<String>,
    pub venue: Option<String>,
    pub value: Option<f64>,
    pub previous_value: Option<f64>,
    pub delta_pct: Option<f64>,
    pub window_ms: Option<i64>,
    pub observed_at_ms: i64,
    pub source_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceQualityReason {
    BaselineMissing,
    SingleNumericSnapshot,
    SingleSourceOnly,
    TitleOnly,
    SymbolAmbiguous,
    MarketContextMissing,
    DuplicateOrSyndicatedSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketContextRef {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextFlagPacket {
    pub flag_packet_id: String,
    pub packet_id: String,
    pub cluster_id: String,
    pub normalized_symbols: Vec<String>,
    pub observe_only: bool,
    pub block_new_entries: bool,
    pub reduce_only: bool,
    pub paper_only: bool,
    pub context_flag: String,
    pub risk_flag: String,
    pub regime_flag: String,
    pub scenario_flag: String,
    pub time_relevance_window: TimeRelevanceWindow,
    pub flag_confidence_band: ConfidenceBand,
    pub reason_summary: String,
    pub model_tier_used: ModelTierUsed,
    pub schema_version: String,
}

impl ContextFlagPacket {
    pub fn schema() -> String {
        CONTEXT_FLAG_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeRelevanceWindow {
    pub start_ms: i64,
    pub end_ms: i64,
    pub relevance_decay_hint: RelevanceDecayHint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelevanceDecayHint {
    Minutes,
    Hours,
    Day,
    MultiDay,
    Structural,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceBand {
    Weak,
    Low,
    Moderate,
    Medium,
    Strong,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Listing,
    Delisting,
    DepositWithdrawal,
    Incident,
    Partnership,
    TokenUnlock,
    Governance,
    FundingShift,
    MacroEvent,
    Regulatory,
    SocialBacklash,
    SocialHype,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ContradictionFlag {
    TimeMismatch,
    SymbolAmbiguity,
    SourceClaimConflict,
    RumorVsOfficial,
    TitleBodyMismatch,
    EvidenceWeak,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelTierUsed {
    RuleOnly,
    Haiku,
    Sonnet,
    FallbackOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TerminalDecision {
    HighConfidenceStructured,
    LowConfidenceStructured,
    GeneralMarketContext,
    Conflicted,
    UnsupportedOrWeak,
    IrrelevantOrNoise,
    QuarantineOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuringHealthEvent {
    pub health_event_id: String,
    pub observed_at_ms: i64,
    pub input_event_count: usize,
    pub cluster_count: usize,
    pub structured_packet_count: usize,
    pub flag_packet_count: usize,
    pub model_l0_invocations: usize,
    pub model_l1_invocations: usize,
    pub fallback_count: usize,
    pub conflict_high_count: usize,
    pub health_level: HealthLevel,
    pub reason: Option<String>,
    pub schema_version: String,
}

impl StructuringHealthEvent {
    pub fn schema() -> String {
        HEALTH_EVENT_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthLevel {
    Healthy,
    Degraded,
    FallbackOnly,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputObjectRef {
    pub object_family: String,
    pub key: String,
    pub record_count: usize,
    pub byte_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoryMember {
    pub schema_version: String,
    pub story_hint_key: String,
    pub cluster_id: String,
    pub raw_event_id: String,
    pub source_id: String,
    pub source_category: String,
    pub normalized_symbols: Vec<String>,
    pub event_type: EventType,
    pub confidence_band: ConfidenceBand,
    pub contradiction_flags: Vec<ContradictionFlag>,
    pub trust_tier: String,
    pub published_at_ms: Option<i64>,
    pub observed_at_ms: i64,
    pub novelty_score: f64,
    pub structuring_policy_version: String,
}

impl StoryMember {
    pub fn schema() -> String {
        STORY_MEMBER_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntelL1Manifest {
    pub schema_version: String,
    pub run_id: String,
    pub raw_event_id: String,
    pub status: String,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub structuring_policy_version: String,
    pub output_object_count: usize,
    pub output_objects: Vec<OutputObjectRef>,
    pub structured_packet_count: usize,
    pub context_flag_packet_count: usize,
    pub story_cluster_count: usize,
    pub health_event_count: usize,
}

impl IntelL1Manifest {
    pub fn schema() -> String {
        MANIFEST_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntelL1IndexPointer {
    pub schema_version: String,
    pub packet_id: String,
    pub raw_event_id: String,
    pub status: String,
    pub manifest_key: String,
    pub structured_packet_keys: Vec<String>,
    pub context_flag_keys: Vec<String>,
    pub finished_at_ms: i64,
    pub structuring_policy_version: String,
}

impl IntelL1IndexPointer {
    pub fn schema() -> String {
        INDEX_POINTER_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PacketRevisionIndex {
    pub schema_version: String,
    pub packet_family_id: String,
    pub raw_event_id: String,
    pub latest_revision: u32,
    pub latest_packet_id: String,
    pub latest_structured_key: String,
    pub market_context_status: MarketContextStatus,
    pub updated_at_ms: i64,
}

impl PacketRevisionIndex {
    pub fn schema() -> String {
        PACKET_REVISION_INDEX_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuredPointer {
    pub schema_version: String,
    pub packet_id: String,
    pub raw_event_id: String,
    pub terminal_decision: TerminalDecision,
    pub storage_ref: S3ObjectPointer,
    pub manifest_key: String,
    pub created_at_ms: i64,
}

impl StructuredPointer {
    pub fn schema() -> String {
        STRUCTURED_POINTER_SCHEMA_VERSION.to_owned()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct S3ObjectPointer {
    pub bucket: String,
    pub key: String,
    pub content_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuarantineEvent {
    pub schema_version: String,
    pub quarantine_id: String,
    pub raw_event_id: Option<String>,
    pub observed_at_ms: i64,
    pub failure_class: String,
    pub retryable: bool,
    pub reason: String,
}

impl QuarantineEvent {
    pub fn new(
        quarantine_id: String,
        raw_event_id: Option<String>,
        observed_at_ms: i64,
        failure_class: impl Into<String>,
        retryable: bool,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: QUARANTINE_SCHEMA_VERSION.to_owned(),
            quarantine_id,
            raw_event_id,
            observed_at_ms,
            failure_class: failure_class.into(),
            retryable,
            reason: reason.into(),
        }
    }
}
