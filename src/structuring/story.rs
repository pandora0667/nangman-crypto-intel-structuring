use crate::error::AppResult;
use crate::hash::stable_short_id;
use crate::models::constants::STORY_MEMBER_SCHEMA_VERSION;
use crate::models::output::{
    ConflictLevel, ContextFlagPacket, ContradictionFlag, EventType, HealthLevel, StoryCluster,
    StoryMember,
};
use crate::models::raw::RawIntelEvent;
use crate::storage::object_store::ObjectStore;
use crate::structuring::packet::PacketSet;
use crate::time::time_part;
use crate::workflow::keys;
use std::collections::{BTreeMap, BTreeSet};

pub struct StoryMergeManager {
    store: ObjectStore,
    member_scan_limit: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoryMergeResult {
    pub story_member_key: String,
    pub story_member_bytes: Vec<u8>,
    pub member_count: usize,
}

impl StoryMergeManager {
    pub fn new(store: ObjectStore, member_scan_limit: usize) -> Self {
        Self {
            store,
            member_scan_limit: member_scan_limit.max(1),
        }
    }

    pub async fn merge_current_event(
        &self,
        event: &RawIntelEvent,
        packet_set: &mut PacketSet,
        policy_version: &str,
        observed_at_ms: i64,
    ) -> AppResult<StoryMergeResult> {
        let current_member =
            StoryMember::from_packet_set(event, packet_set, policy_version, observed_at_ms);
        let story_member_key = keys::story_member_key(
            &current_member.story_hint_key,
            policy_version,
            &current_member.raw_event_id,
        );
        let story_member_bytes = serde_json::to_vec_pretty(&current_member)?;

        let prefix = keys::story_member_prefix(&current_member.story_hint_key, policy_version);
        let mut members = Vec::new();
        for key in self
            .store
            .list_keys(&prefix, self.member_scan_limit)
            .await?
        {
            let member = self.store.get_json::<StoryMember>(&key).await?;
            if member.raw_event_id == current_member.raw_event_id {
                continue;
            }
            if self
                .store
                .object_exists(&keys::index_key(&member.raw_event_id, policy_version))
                .await?
            {
                members.push(member);
            }
        }
        members.push(current_member);

        let merged_cluster = merge_story_members(&packet_set.story_cluster, members);
        apply_story_cluster(packet_set, merged_cluster);

        Ok(StoryMergeResult {
            story_member_key,
            story_member_bytes,
            member_count: packet_set.story_cluster.source_event_ids.len(),
        })
    }
}

impl StoryMember {
    pub fn from_packet_set(
        event: &RawIntelEvent,
        packet_set: &PacketSet,
        policy_version: &str,
        observed_at_ms: i64,
    ) -> Self {
        Self {
            schema_version: STORY_MEMBER_SCHEMA_VERSION.to_owned(),
            story_hint_key: packet_set.story_cluster.story_hint_key.clone(),
            cluster_id: packet_set.story_cluster.cluster_id.clone(),
            raw_event_id: event.event_id.clone(),
            source_id: event.source_id.clone(),
            source_category: event.source_category.clone(),
            normalized_symbols: packet_set.structured_packet.normalized_symbols.clone(),
            event_type: packet_set.structured_packet.event_type.clone(),
            confidence_band: packet_set.structured_packet.confidence_band.clone(),
            contradiction_flags: packet_set.structured_packet.contradiction_flags.clone(),
            trust_tier: event.trust_tier.clone(),
            published_at_ms: event.published_at_ms,
            observed_at_ms,
            novelty_score: packet_set.structured_packet.novelty_score,
            structuring_policy_version: policy_version.to_owned(),
        }
    }
}

pub fn story_hint_key(event: &RawIntelEvent, event_type: &EventType, symbols: &[String]) -> String {
    let basis_ms = event.published_at_ms.unwrap_or(event.fetched_at_ms);
    let date = time_part(basis_ms).event_date;
    let symbol_signature = symbol_signature(symbols);
    let topic_signature = if symbol_signature == "general" {
        title_signature(&event.title)
    } else {
        event_type_label(event_type).to_owned()
    };
    stable_short_id(
        "story_hint",
        &[
            &date,
            event_type_label(event_type),
            &symbol_signature,
            &topic_signature,
        ],
    )
}

pub fn story_cluster_id(story_hint_key: &str, policy_version: &str) -> String {
    stable_short_id("story", &[story_hint_key, policy_version])
}

pub fn merge_story_members(base: &StoryCluster, members: Vec<StoryMember>) -> StoryCluster {
    let mut by_event_id = BTreeMap::<String, StoryMember>::new();
    for member in members {
        by_event_id
            .entry(member.raw_event_id.clone())
            .or_insert(member);
    }
    let members = by_event_id.into_values().collect::<Vec<_>>();
    if members.is_empty() {
        return base.clone();
    }

    let source_event_ids = members
        .iter()
        .map(|member| member.raw_event_id.clone())
        .collect::<Vec<_>>();
    let related_symbols = members
        .iter()
        .flat_map(|member| member.normalized_symbols.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let source_ids = members
        .iter()
        .map(|member| member.source_id.clone())
        .collect::<BTreeSet<_>>();
    let event_types = members
        .iter()
        .map(|member| event_type_label(&member.event_type).to_owned())
        .collect::<BTreeSet<_>>();
    let contradiction_flags = merged_contradiction_flags(&members, event_types.len() > 1);
    let conflict_level = conflict_level(&contradiction_flags, source_ids.len());
    let conflicting_source_ids =
        if matches!(conflict_level, ConflictLevel::Medium | ConflictLevel::High) {
            source_ids.iter().cloned().collect()
        } else {
            Vec::new()
        };

    StoryCluster {
        cluster_id: base.cluster_id.clone(),
        source_event_ids,
        story_hint_key: base.story_hint_key.clone(),
        primary_topic: primary_topic(&event_types),
        secondary_topics: event_types.into_iter().collect(),
        related_symbols,
        source_count: source_ids.len(),
        trust_mix: trust_mix(&members),
        first_published_at_ms: first_published_at_ms(&members),
        last_updated_at_ms: members
            .iter()
            .map(|member| member.observed_at_ms)
            .max()
            .unwrap_or(base.last_updated_at_ms),
        novelty_score: members
            .iter()
            .map(|member| member.novelty_score)
            .fold(base.novelty_score, f64::max),
        conflict_level,
        conflicting_source_ids,
        resolution_summary: resolution_summary(&contradiction_flags, source_ids.len()),
        schema_version: base.schema_version.clone(),
    }
}

fn apply_story_cluster(packet_set: &mut PacketSet, cluster: StoryCluster) {
    packet_set.structured_packet.cluster_id = cluster.cluster_id.clone();
    packet_set.structured_packet.source_event_ids = cluster.source_event_ids.clone();
    packet_set.structured_packet.normalized_symbols = cluster.related_symbols.clone();
    if let Some(context_flag_packet) = packet_set.context_flag_packet.as_mut() {
        update_context_flag_for_cluster(context_flag_packet, &cluster);
    }
    packet_set.story_cluster = cluster;
    packet_set.health_event.conflict_high_count =
        usize::from(packet_set.story_cluster.conflict_level == ConflictLevel::High);
    packet_set.health_event.flag_packet_count =
        usize::from(packet_set.context_flag_packet.is_some());
    if packet_set.health_event.fallback_count > 0 {
        packet_set.health_event.health_level = HealthLevel::FallbackOnly;
    } else if packet_set.story_cluster.conflict_level == ConflictLevel::High {
        packet_set.health_event.health_level = HealthLevel::Degraded;
    }
}

fn update_context_flag_for_cluster(
    context_flag_packet: &mut ContextFlagPacket,
    cluster: &StoryCluster,
) {
    context_flag_packet.cluster_id = cluster.cluster_id.clone();
    context_flag_packet.normalized_symbols = cluster.related_symbols.clone();
}

fn merged_contradiction_flags(
    members: &[StoryMember],
    event_type_conflict: bool,
) -> Vec<ContradictionFlag> {
    let mut flags = members
        .iter()
        .flat_map(|member| member.contradiction_flags.clone())
        .collect::<BTreeSet<_>>();
    if event_type_conflict {
        flags.insert(ContradictionFlag::SourceClaimConflict);
    }
    flags.into_iter().collect()
}

fn conflict_level(flags: &[ContradictionFlag], source_count: usize) -> ConflictLevel {
    if flags.iter().any(|flag| {
        matches!(
            flag,
            ContradictionFlag::SourceClaimConflict | ContradictionFlag::RumorVsOfficial
        )
    }) {
        ConflictLevel::High
    } else if !flags.is_empty() {
        ConflictLevel::Medium
    } else if source_count > 1 {
        ConflictLevel::Low
    } else {
        ConflictLevel::None
    }
}

fn first_published_at_ms(members: &[StoryMember]) -> Option<i64> {
    members
        .iter()
        .filter_map(|member| member.published_at_ms)
        .min()
}

fn trust_mix(members: &[StoryMember]) -> String {
    let mut counts = BTreeMap::<String, usize>::new();
    for member in members {
        *counts.entry(member.trust_tier.clone()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(tier, count)| format!("{tier}={count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn primary_topic(event_types: &BTreeSet<String>) -> String {
    if event_types.len() == 1 {
        event_types
            .iter()
            .next()
            .cloned()
            .unwrap_or_else(|| "other".to_owned())
    } else {
        "mixed_topic_conflict".to_owned()
    }
}

fn resolution_summary(flags: &[ContradictionFlag], source_count: usize) -> String {
    if flags.is_empty() && source_count <= 1 {
        "single source story".to_owned()
    } else if flags.is_empty() {
        "merged independent sources with no conflict detected".to_owned()
    } else {
        "merged sources with preserved conflict flags".to_owned()
    }
}

fn symbol_signature(symbols: &[String]) -> String {
    let signature = symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",");
    if signature.is_empty() {
        "general".to_owned()
    } else {
        signature
    }
}

fn title_signature(title: &str) -> String {
    let normalized_title = title.to_ascii_lowercase();
    let tokens = normalized_title
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| token.len() >= 4)
        .filter(|token| !STOPWORDS.contains(token))
        .take(8)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        "untitled".to_owned()
    } else {
        tokens.join("_")
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

const STOPWORDS: &[&str] = &[
    "about", "after", "amid", "from", "into", "over", "that", "this", "with", "will",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::constants::STORY_CLUSTER_SCHEMA_VERSION;
    use crate::models::output::ConfidenceBand;

    #[test]
    fn same_symbol_event_type_and_day_share_story_hint() {
        let first = raw_event("raw1", "src1", "ABC exploit confirmed");
        let second = raw_event("raw2", "src2", "Protocol incident update for ABC");

        assert_eq!(
            story_hint_key(&first, &EventType::Incident, &["ABC".to_owned()]),
            story_hint_key(&second, &EventType::Incident, &["ABC".to_owned()])
        );
    }

    #[test]
    fn merge_preserves_sources_and_conflict() {
        let base = StoryCluster {
            cluster_id: "story_1".to_owned(),
            source_event_ids: vec!["raw1".to_owned()],
            story_hint_key: "hint".to_owned(),
            primary_topic: "incident".to_owned(),
            secondary_topics: Vec::new(),
            related_symbols: vec!["ABC".to_owned()],
            source_count: 1,
            trust_mix: "T1=1".to_owned(),
            first_published_at_ms: Some(1),
            last_updated_at_ms: 1,
            novelty_score: 0.5,
            conflict_level: ConflictLevel::None,
            conflicting_source_ids: Vec::new(),
            resolution_summary: "single source story".to_owned(),
            schema_version: STORY_CLUSTER_SCHEMA_VERSION.to_owned(),
        };
        let merged = merge_story_members(
            &base,
            vec![
                member("raw1", "src1", EventType::Incident),
                member("raw2", "src2", EventType::Regulatory),
            ],
        );

        assert_eq!(merged.source_event_ids, vec!["raw1", "raw2"]);
        assert_eq!(merged.source_count, 2);
        assert_eq!(merged.conflict_level, ConflictLevel::High);
        assert_eq!(merged.conflicting_source_ids, vec!["src1", "src2"]);
        assert!(merged.secondary_topics.contains(&"regulatory".to_owned()));
    }

    #[test]
    fn source_count_counts_unique_sources_not_member_count() {
        let base = StoryCluster {
            cluster_id: "story_1".to_owned(),
            source_event_ids: vec!["raw1".to_owned()],
            story_hint_key: "hint".to_owned(),
            primary_topic: "incident".to_owned(),
            secondary_topics: Vec::new(),
            related_symbols: vec!["ABC".to_owned()],
            source_count: 1,
            trust_mix: "T1=1".to_owned(),
            first_published_at_ms: Some(1),
            last_updated_at_ms: 1,
            novelty_score: 0.5,
            conflict_level: ConflictLevel::None,
            conflicting_source_ids: Vec::new(),
            resolution_summary: "single source story".to_owned(),
            schema_version: STORY_CLUSTER_SCHEMA_VERSION.to_owned(),
        };
        let merged = merge_story_members(
            &base,
            vec![
                member("raw1", "src1", EventType::Incident),
                member("raw2", "src1", EventType::Incident),
            ],
        );

        assert_eq!(merged.source_event_ids, vec!["raw1", "raw2"]);
        assert_eq!(merged.source_count, 1);
        assert_eq!(merged.conflict_level, ConflictLevel::None);
    }

    fn member(raw_event_id: &str, source_id: &str, event_type: EventType) -> StoryMember {
        StoryMember {
            schema_version: STORY_MEMBER_SCHEMA_VERSION.to_owned(),
            story_hint_key: "hint".to_owned(),
            cluster_id: "story_1".to_owned(),
            raw_event_id: raw_event_id.to_owned(),
            source_id: source_id.to_owned(),
            source_category: "news".to_owned(),
            normalized_symbols: vec!["ABC".to_owned()],
            event_type,
            confidence_band: ConfidenceBand::Medium,
            contradiction_flags: Vec::new(),
            trust_tier: "T1".to_owned(),
            published_at_ms: Some(1),
            observed_at_ms: 1,
            novelty_score: 0.5,
            structuring_policy_version: "policy".to_owned(),
        }
    }

    fn raw_event(event_id: &str, source_id: &str, title: &str) -> RawIntelEvent {
        RawIntelEvent {
            event_id: event_id.to_owned(),
            source_id: source_id.to_owned(),
            source_category: "news".to_owned(),
            source_name: "News".to_owned(),
            fetched_at_ms: 1,
            published_at_ms: Some(1),
            observed_at_ms: 1,
            language: "en".to_owned(),
            title: title.to_owned(),
            body: title.to_owned(),
            url: "https://example.com".to_owned(),
            author_or_channel: None,
            trust_tier: "T1".to_owned(),
            cadence_tier: "low".to_owned(),
            content_hash: "h".to_owned(),
            dedup_key: event_id.to_owned(),
            symbol_candidates: vec!["ABC".to_owned()],
            event_category_hint: None,
            top50_relevance: "relevant".to_owned(),
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
