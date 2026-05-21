use crate::models::constants::{
    CONTEXT_FLAG_SCHEMA_VERSION, HEALTH_EVENT_SCHEMA_VERSION, INDEX_POINTER_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION, PACKET_REVISION_INDEX_SCHEMA_VERSION, QUARANTINE_SCHEMA_VERSION,
    STORY_CLUSTER_SCHEMA_VERSION, STORY_MEMBER_SCHEMA_VERSION, STRUCTURED_PACKET_SCHEMA_VERSION,
};
use crate::time::time_part;

pub fn structured_packet_key(timestamp_ms: i64, raw_event_id: &str, packet_id: &str) -> String {
    let part = time_part(timestamp_ms);
    format!(
        "structured-intel-packet/schema={STRUCTURED_PACKET_SCHEMA_VERSION}/dt={}/hour={:02}/raw_event_id={}/packet_id={}/part-000001.jsonl",
        part.event_date,
        part.hour,
        path_segment(raw_event_id),
        path_segment(packet_id)
    )
}

pub fn context_flag_key(timestamp_ms: i64, raw_event_id: &str, flag_packet_id: &str) -> String {
    let part = time_part(timestamp_ms);
    format!(
        "context-flag-packet/schema={CONTEXT_FLAG_SCHEMA_VERSION}/dt={}/hour={:02}/raw_event_id={}/flag_packet_id={}/part-000001.jsonl",
        part.event_date,
        part.hour,
        path_segment(raw_event_id),
        path_segment(flag_packet_id)
    )
}

pub fn story_cluster_key(timestamp_ms: i64, raw_event_id: &str, cluster_id: &str) -> String {
    let part = time_part(timestamp_ms);
    format!(
        "story-cluster/schema={STORY_CLUSTER_SCHEMA_VERSION}/dt={}/hour={:02}/raw_event_id={}/cluster_id={}/part-000001.jsonl",
        part.event_date,
        part.hour,
        path_segment(raw_event_id),
        path_segment(cluster_id)
    )
}

pub fn story_member_prefix(story_hint_key: &str, policy_version: &str) -> String {
    format!(
        "story-members/schema={STORY_MEMBER_SCHEMA_VERSION}/story_hint_key={}/policy={}/",
        path_segment(story_hint_key),
        path_segment(policy_version)
    )
}

pub fn story_member_key(story_hint_key: &str, policy_version: &str, raw_event_id: &str) -> String {
    format!(
        "{}raw_event_id={}.json",
        story_member_prefix(story_hint_key, policy_version),
        path_segment(raw_event_id)
    )
}

pub fn health_key(timestamp_ms: i64, raw_event_id: &str, health_event_id: &str) -> String {
    let part = time_part(timestamp_ms);
    format!(
        "structuring-health/schema={HEALTH_EVENT_SCHEMA_VERSION}/dt={}/hour={:02}/raw_event_id={}/health_event_id={}/part-000001.jsonl",
        part.event_date,
        part.hour,
        path_segment(raw_event_id),
        path_segment(health_event_id)
    )
}

pub fn manifest_key(timestamp_ms: i64, raw_event_id: &str, run_id: &str) -> String {
    let part = time_part(timestamp_ms);
    format!(
        "manifests/schema={MANIFEST_SCHEMA_VERSION}/dt={}/hour={:02}/raw_event_id={}/run_id={}.json",
        part.event_date,
        part.hour,
        path_segment(raw_event_id),
        path_segment(run_id)
    )
}

pub fn index_key(raw_event_id: &str, policy_version: &str) -> String {
    format!(
        "intel-l1-index/schema={INDEX_POINTER_SCHEMA_VERSION}/raw_event_id={}/policy={}.json",
        path_segment(raw_event_id),
        path_segment(policy_version)
    )
}

pub fn prepared_index_key(raw_event_id: &str, policy_version: &str) -> String {
    format!(
        "intel-l1-index/status=prepared/schema={INDEX_POINTER_SCHEMA_VERSION}/raw_event_id={}/policy={}.json",
        path_segment(raw_event_id),
        path_segment(policy_version)
    )
}

pub fn packet_revision_index_prefix(packet_family_id: &str) -> String {
    format!(
        "packet-revision-index/schema={PACKET_REVISION_INDEX_SCHEMA_VERSION}/packet_family_id={}/",
        path_segment(packet_family_id)
    )
}

pub fn packet_revision_index_key(packet_family_id: &str, revision: u32) -> String {
    format!(
        "{}revision={:010}.json",
        packet_revision_index_prefix(packet_family_id),
        revision
    )
}

pub fn quarantine_key(
    timestamp_ms: i64,
    raw_event_id: Option<&str>,
    quarantine_id: &str,
) -> String {
    let part = time_part(timestamp_ms);
    format!(
        "quarantine/schema={QUARANTINE_SCHEMA_VERSION}/dt={}/hour={:02}/raw_event_id={}/quarantine_id={}.json",
        part.event_date,
        part.hour,
        path_segment(raw_event_id.unwrap_or("unknown")),
        path_segment(quarantine_id)
    )
}

pub fn path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_structured_key() {
        assert_eq!(
            structured_packet_key(0, "raw:e", "pkt/1"),
            "structured-intel-packet/schema=structured_intel_packet_v1/dt=1970-01-01/hour=00/raw_event_id=raw_e/packet_id=pkt_1/part-000001.jsonl"
        );
    }

    #[test]
    fn index_key_is_deterministic_for_redelivery() {
        assert_eq!(
            index_key("raw:e", "policy/1"),
            index_key("raw:e", "policy/1")
        );
        assert_ne!(
            index_key("raw:e", "policy/1"),
            index_key("raw:e", "policy/2")
        );
    }

    #[test]
    fn prepared_and_success_index_keys_are_separate() {
        assert_ne!(
            prepared_index_key("raw:e", "policy/1"),
            index_key("raw:e", "policy/1")
        );
        assert!(prepared_index_key("raw:e", "policy/1").contains("status=prepared"));
    }

    #[test]
    fn story_member_key_groups_by_hint_and_policy() {
        assert_eq!(
            story_member_key("hint/1", "policy/1", "raw:e"),
            "story-members/schema=story_member_v1/story_hint_key=hint_1/policy=policy_1/raw_event_id=raw_e.json"
        );
    }
}
