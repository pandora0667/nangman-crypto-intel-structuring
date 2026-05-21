pub const RAW_POINTER_SCHEMA_VERSION: &str = "raw_intel_event_created_v2";
pub const RAW_EVENT_SCHEMA_VERSION: &str = "raw_intel_event_v1";
pub const STRUCTURED_PACKET_SCHEMA_VERSION: &str = "structured_intel_packet_v1";
pub const CONTEXT_FLAG_SCHEMA_VERSION: &str = "context_flag_packet_v1";
pub const STORY_CLUSTER_SCHEMA_VERSION: &str = "story_cluster_v1";
pub const STORY_MEMBER_SCHEMA_VERSION: &str = "story_member_v1";
pub const HEALTH_EVENT_SCHEMA_VERSION: &str = "structuring_health_event_v1";
pub const QUARANTINE_SCHEMA_VERSION: &str = "intel_l1_quarantine_event_v1";
pub const MANIFEST_SCHEMA_VERSION: &str = "intel_l1_manifest_v1";
pub const INDEX_POINTER_SCHEMA_VERSION: &str = "intel_l1_index_pointer_v1";
pub const PACKET_REVISION_INDEX_SCHEMA_VERSION: &str = "intel_l1_packet_revision_index_v1";
pub const MARKET_L1_POINTER_SCHEMA_VERSION: &str = "l1_index_pointer_v1";
pub const MARKET_L1_MANIFEST_SCHEMA_VERSION: &str = "l1_manifest_v1";
pub const MARKET_L1_REPORT_SCHEMA_VERSION: &str = "normalization_report_v1";
pub const MARKET_L1_SLICE_SCHEMA_VERSION: &str = "normalized_market_slice_v1";
pub const STRUCTURING_POLICY_VERSION: &str = "intel_l1_policy_20260509_p0_replay_guard_v1";
pub const DEFAULT_PRIMARY_MODEL_ID: &str = "global.anthropic.claude-haiku-4-5-20251001-v1:0";
pub const DEFAULT_ESCALATION_MODEL_ID: &str = "global.anthropic.claude-sonnet-4-6";

pub const FORBIDDEN_OUTPUT_TERMS: &[&str] = &[
    "buy",
    "sell",
    "long",
    "short",
    "position_size",
    "live_ready",
    "execution_approved",
];
