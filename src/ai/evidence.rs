use crate::ai::contract::EvidenceSnippet;
use crate::models::raw::RawIntelEvent;
use std::collections::BTreeSet;

const DEFAULT_MAX_ITEMS: usize = 10;
const DEFAULT_MAX_TEXT_CHARS: usize = 420;

#[derive(Debug, Clone)]
struct Candidate {
    text: String,
    score: i32,
    order: usize,
}

pub fn build_evidence_pack(event: &RawIntelEvent) -> Vec<EvidenceSnippet> {
    build_evidence_pack_with_limits(event, DEFAULT_MAX_ITEMS, DEFAULT_MAX_TEXT_CHARS)
}

pub fn build_evidence_pack_with_limits(
    event: &RawIntelEvent,
    max_items: usize,
    max_text_chars: usize,
) -> Vec<EvidenceSnippet> {
    let mut candidates = Vec::new();
    let mut order = 0;

    push_candidate(
        &mut candidates,
        event.title.trim(),
        score_text(event, event.title.trim()) + 10,
        &mut order,
    );

    for line in event.body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        for sentence in split_sentences(line) {
            let sentence = sentence.trim();
            if sentence.is_empty() {
                continue;
            }
            push_candidate(
                &mut candidates,
                sentence,
                score_text(event, sentence),
                &mut order,
            );
        }
    }

    let mut seen = BTreeSet::new();
    candidates.retain(|candidate| {
        let key = normalize_for_dedup(&candidate.text);
        if key.is_empty() || seen.contains(&key) {
            return false;
        }
        seen.insert(key);
        true
    });
    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.order.cmp(&right.order))
    });

    candidates
        .into_iter()
        .take(max_items)
        .enumerate()
        .map(|(index, candidate)| EvidenceSnippet {
            id: format!("E{}", index + 1),
            text: truncate_chars(&candidate.text, max_text_chars),
        })
        .collect()
}

fn push_candidate(candidates: &mut Vec<Candidate>, text: &str, score: i32, order: &mut usize) {
    let normalized = normalize_whitespace(text);
    if normalized.chars().count() < 12 {
        return;
    }
    candidates.push(Candidate {
        text: normalized,
        score,
        order: *order,
    });
    *order += 1;
}

fn split_sentences(line: &str) -> Vec<&str> {
    let mut sentences = Vec::new();
    let mut start = 0;
    for (index, ch) in line.char_indices() {
        if matches!(ch, '.' | '!' | '?' | '\u{3002}') {
            let end = index + ch.len_utf8();
            if start < end {
                sentences.push(&line[start..end]);
            }
            start = end;
        }
    }
    if start < line.len() {
        sentences.push(&line[start..]);
    }
    sentences
}

fn score_text(event: &RawIntelEvent, text: &str) -> i32 {
    let lower = text.to_ascii_lowercase();
    let mut score = 0;
    for keyword in [
        "listing",
        "delisting",
        "remove",
        "suspend",
        "withdrawal",
        "deposit",
        "hack",
        "exploit",
        "incident",
        "security",
        "regulatory",
        "sec",
        "unlock",
        "governance",
        "proposal",
        "partnership",
        "funding",
        "open interest",
    ] {
        if lower.contains(keyword) {
            score += 4;
        }
    }
    for symbol in &event.symbol_candidates {
        let symbol = symbol.trim();
        if !symbol.is_empty()
            && text
                .to_ascii_uppercase()
                .contains(&symbol.to_ascii_uppercase())
        {
            score += 3;
        }
    }
    if event.source_category.contains("notice") || event.source_category.contains("exchange") {
        score += 2;
    }
    if event.trust_tier == "T1" {
        score += 1;
    }
    score
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_for_dedup(text: &str) -> String {
    normalize_whitespace(text).to_ascii_lowercase()
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    text.chars().take(max_chars).collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_ranked_stable_evidence_pack() {
        let event = RawIntelEvent {
            event_id: "e".to_owned(),
            source_id: "s".to_owned(),
            source_category: "exchange_notice".to_owned(),
            source_name: "S".to_owned(),
            fetched_at_ms: 1,
            published_at_ms: Some(1),
            observed_at_ms: 1,
            language: "en".to_owned(),
            title: "ABC listing notice".to_owned(),
            body:
                "Noise sentence. ABC deposits will open tomorrow. ABC deposits will open tomorrow."
                    .to_owned(),
            url: "https://example.com".to_owned(),
            author_or_channel: None,
            trust_tier: "T1".to_owned(),
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

        let pack = build_evidence_pack_with_limits(&event, 3, 80);

        assert_eq!(pack[0].id, "E1");
        assert!(pack[0].text.contains("ABC"));
        assert_eq!(pack.len(), 3);
    }
}
