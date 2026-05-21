use crate::admission::market_l1::build_market_l1_read_plan;
use crate::error::AppResult;
use crate::models::market::{
    MarketContextSnapshot, MarketContextStatus, MarketL1IndexPointer, MarketL1Manifest,
    MarketL1Report,
};
use crate::storage::object_store::ObjectStore;
use crate::time::{floor_window, time_part};
use std::collections::BTreeSet;

const LATEST_BEFORE_MAX_INDEX_KEYS_PER_HOUR: usize = 5_000;
const HOUR_MS: i64 = 3_600_000;

#[derive(Clone)]
pub struct MarketL1Reader {
    store: ObjectStore,
    window_ms: i64,
    radius_windows: i64,
    latest_before_lookback_ms: i64,
    stale_after_ms: i64,
}

impl MarketL1Reader {
    pub fn new(
        store: ObjectStore,
        window_ms: i64,
        radius_windows: i64,
        latest_before_lookback_ms: i64,
        stale_after_ms: i64,
    ) -> Self {
        Self {
            store,
            window_ms,
            radius_windows: radius_windows.max(0),
            latest_before_lookback_ms: latest_before_lookback_ms.max(window_ms),
            stale_after_ms: stale_after_ms.max(window_ms),
        }
    }

    pub async fn context_for(
        &self,
        published_at_ms: Option<i64>,
        fetched_at_ms: i64,
        symbols: &[String],
    ) -> MarketContextSnapshot {
        let (basis_timestamp_ms, basis_kind) = match published_at_ms {
            Some(value) => (value, "published_at_ms"),
            None => (fetched_at_ms, "fetched_at_ms"),
        };
        match self
            .read_contexts(basis_timestamp_ms, basis_kind, symbols)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => MarketContextSnapshot::pending(
                format!("Market-L1 unavailable: {error}"),
                basis_timestamp_ms,
                basis_kind,
            ),
        }
    }

    async fn read_contexts(
        &self,
        basis_timestamp_ms: i64,
        basis_kind: &str,
        symbols: &[String],
    ) -> AppResult<MarketContextSnapshot> {
        let basis_window_start_ms = floor_window(basis_timestamp_ms, self.window_ms);
        let mut snapshots = Vec::new();
        let mut last_error = None;
        for window_start_ms in self.candidate_window_starts(basis_window_start_ms).await {
            match self
                .read_single_context(
                    basis_timestamp_ms,
                    basis_kind,
                    symbols,
                    basis_window_start_ms,
                    window_start_ms,
                )
                .await
            {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => last_error = Some(error),
            }
        }
        merge_snapshots(snapshots).ok_or_else(|| {
            last_error.unwrap_or_else(|| {
                crate::error::AppError::validation("Market-L1 no usable windows")
            })
        })
    }

    async fn candidate_window_starts(&self, basis_window_start_ms: i64) -> Vec<i64> {
        let mut ordered = Vec::new();
        let mut seen = BTreeSet::new();
        for offset in -self.radius_windows..=self.radius_windows {
            push_unique(
                &mut ordered,
                &mut seen,
                basis_window_start_ms + offset * self.window_ms,
            );
        }
        if let Ok(Some(latest_before)) =
            self.latest_before_window_start(basis_window_start_ms).await
        {
            push_unique(&mut ordered, &mut seen, latest_before);
        }
        ordered
    }

    async fn latest_before_window_start(
        &self,
        basis_window_start_ms: i64,
    ) -> AppResult<Option<i64>> {
        let earliest = basis_window_start_ms.saturating_sub(self.latest_before_lookback_ms);
        let mut latest = None;
        for prefix in index_prefixes(self.window_ms, earliest, basis_window_start_ms) {
            for key in self
                .store
                .list_keys(&prefix, LATEST_BEFORE_MAX_INDEX_KEYS_PER_HOUR)
                .await?
            {
                let Some(window_start_ms) = parse_window_start_ms(&key) else {
                    continue;
                };
                if window_start_ms > basis_window_start_ms || window_start_ms < earliest {
                    continue;
                }
                latest =
                    Some(latest.map_or(window_start_ms, |value: i64| value.max(window_start_ms)));
            }
        }
        Ok(latest)
    }

    async fn read_single_context(
        &self,
        basis_timestamp_ms: i64,
        basis_kind: &str,
        symbols: &[String],
        basis_window_start_ms: i64,
        window_start_ms: i64,
    ) -> AppResult<MarketContextSnapshot> {
        let window_end_ms = window_start_ms + self.window_ms;
        let pointer_key = index_pointer_key(self.window_ms, window_start_ms);
        let pointer = self
            .store
            .get_json::<MarketL1IndexPointer>(&pointer_key)
            .await?;
        let manifest = self
            .store
            .get_json::<MarketL1Manifest>(&pointer.canonical_manifest_key)
            .await?;
        let report = self
            .store
            .get_json::<MarketL1Report>(&manifest.report_key)
            .await?;
        let plan = build_market_l1_read_plan(
            &pointer,
            &manifest,
            &report,
            &pointer.canonical_manifest_key,
            window_start_ms,
            window_end_ms,
        )?;

        let symbol_summaries = if symbols.is_empty() {
            Vec::new()
        } else {
            crate::market::parquet_compact::read_symbol_summaries(&self.store, &plan, symbols)
                .await?
        };

        Ok(MarketContextSnapshot {
            status: context_status(
                symbols,
                &symbol_summaries,
                window_start_ms,
                basis_window_start_ms,
                self.stale_after_ms,
            ),
            basis_timestamp_ms: Some(basis_timestamp_ms),
            basis_kind: basis_kind.to_owned(),
            window_start_ms: Some(window_start_ms),
            window_end_ms: Some(window_end_ms),
            manifest_key: Some(plan.manifest_key),
            output_object_keys: plan.output_object_keys,
            market_data_quality_summary_key: plan.market_data_quality_summary_key,
            market_feature_delta_key: plan.market_feature_delta_key,
            market_feature_delta_summary_key: plan.market_feature_delta_summary_key,
            market_regime_context_key: plan.market_regime_context_key,
            symbol_universe_snapshot_key: plan.symbol_universe_snapshot_key,
            symbol_summaries,
            unavailable_reason: None,
        })
    }
}

fn push_unique(values: &mut Vec<i64>, seen: &mut BTreeSet<i64>, value: i64) {
    if seen.insert(value) {
        values.push(value);
    }
}

fn merge_snapshots(mut snapshots: Vec<MarketContextSnapshot>) -> Option<MarketContextSnapshot> {
    if snapshots.is_empty() {
        return None;
    }
    let mut first = snapshots.remove(0);
    for snapshot in snapshots {
        first.status = merge_status(&first.status, &snapshot.status);
        first.output_object_keys.extend(snapshot.output_object_keys);
        if first.market_data_quality_summary_key.is_none() {
            first.market_data_quality_summary_key = snapshot.market_data_quality_summary_key;
        }
        if first.market_feature_delta_key.is_none() {
            first.market_feature_delta_key = snapshot.market_feature_delta_key;
        }
        if first.market_feature_delta_summary_key.is_none() {
            first.market_feature_delta_summary_key = snapshot.market_feature_delta_summary_key;
        }
        if first.market_regime_context_key.is_none() {
            first.market_regime_context_key = snapshot.market_regime_context_key;
        }
        if first.symbol_universe_snapshot_key.is_none() {
            first.symbol_universe_snapshot_key = snapshot.symbol_universe_snapshot_key;
        }
        first.symbol_summaries.extend(snapshot.symbol_summaries);
    }
    Some(first)
}

fn context_status(
    requested_symbols: &[String],
    symbol_summaries: &[crate::models::market::MarketSymbolSummary],
    window_start_ms: i64,
    basis_window_start_ms: i64,
    stale_after_ms: i64,
) -> MarketContextStatus {
    if requested_symbols.is_empty() {
        return MarketContextStatus::AvailableGeneralContext;
    }
    if symbol_summaries.is_empty() {
        return MarketContextStatus::AvailableGeneralContext;
    }
    if window_start_ms == basis_window_start_ms {
        MarketContextStatus::AvailableSymbolContext
    } else if basis_window_start_ms.saturating_sub(window_start_ms) > stale_after_ms {
        MarketContextStatus::StaleButUsable
    } else {
        MarketContextStatus::NearestAvailable
    }
}

fn merge_status(left: &MarketContextStatus, right: &MarketContextStatus) -> MarketContextStatus {
    use MarketContextStatus::*;
    match (left, right) {
        (AvailableSymbolContext, _) | (_, AvailableSymbolContext) => AvailableSymbolContext,
        (NearestAvailable, _) | (_, NearestAvailable) => NearestAvailable,
        (SymbolContextOnly, _) | (_, SymbolContextOnly) => SymbolContextOnly,
        (StaleButUsable, _) | (_, StaleButUsable) => StaleButUsable,
        (Available, _) | (_, Available) => Available,
        (AvailableGeneralContext, _) | (_, AvailableGeneralContext) => AvailableGeneralContext,
        (Pending, _) | (_, Pending) => Pending,
        (Unavailable, Unavailable) => Unavailable,
    }
}

fn index_prefixes(window_ms: i64, earliest_ms: i64, latest_ms: i64) -> Vec<String> {
    let mut prefixes = Vec::new();
    let mut current = floor_window(earliest_ms.min(latest_ms), HOUR_MS);
    let latest_hour = floor_window(latest_ms.max(earliest_ms), HOUR_MS);
    while current <= latest_hour {
        let part = time_part(current);
        prefixes.push(format!(
            "l1_index/window_ms={window_ms}/event_date={}/hour={:02}/",
            part.event_date, part.hour
        ));
        let next = current.saturating_add(HOUR_MS);
        if next <= current {
            break;
        }
        current = next;
    }
    prefixes
}

fn parse_window_start_ms(key: &str) -> Option<i64> {
    key.strip_suffix(".json")?
        .rsplit_once("window_start_ms=")?
        .1
        .parse()
        .ok()
}

pub fn index_pointer_key(window_ms: i64, window_start_ms: i64) -> String {
    let part = time_part(window_start_ms);
    format!(
        "l1_index/window_ms={window_ms}/event_date={}/hour={:02}/window_start_ms={window_start_ms}.json",
        part.event_date, part.hour
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_market_l1_index_key() {
        assert_eq!(
            index_pointer_key(1_000, 0),
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/window_start_ms=0.json"
        );
    }

    #[test]
    fn parses_window_start_from_index_key() {
        assert_eq!(
            parse_window_start_ms(
                "l1_index/window_ms=1000/event_date=2026-05-08/hour=12/window_start_ms=1778242444000.json"
            ),
            Some(1_778_242_444_000)
        );
    }

    #[test]
    fn builds_previous_and_current_hour_prefixes_for_cross_hour_lookback() {
        assert_eq!(
            index_prefixes(1_000, 3_599_000, 3_600_000),
            vec![
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/".to_owned(),
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=01/".to_owned(),
            ]
        );
    }

    #[test]
    fn builds_all_hour_prefixes_for_multi_hour_latest_before_lookback() {
        assert_eq!(
            index_prefixes(1_000, 0, 14_400_000),
            vec![
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/".to_owned(),
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=01/".to_owned(),
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=02/".to_owned(),
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=03/".to_owned(),
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=04/".to_owned(),
            ]
        );
    }

    #[test]
    fn marks_old_symbol_context_as_stale_but_usable() {
        let status = context_status(
            &["SUI".to_owned()],
            &[crate::models::market::MarketSymbolSummary {
                symbol: "SUI".to_owned(),
                venue: "binance".to_owned(),
                window_start_ms: 0,
                window_end_ms: 1_000,
                mid_price: Some(1.0),
                spread_bps: Some(1.0),
                trade_count: 1,
                trade_volume: 10.0,
                slice_completeness: "complete".to_owned(),
            }],
            0,
            3_600_000,
            600_000,
        );
        assert_eq!(status, MarketContextStatus::StaleButUsable);
    }

    #[test]
    fn marks_nearby_symbol_context_as_nearest_available() {
        let status = context_status(
            &["SUI".to_owned()],
            &[crate::models::market::MarketSymbolSummary {
                symbol: "SUI".to_owned(),
                venue: "binance".to_owned(),
                window_start_ms: 0,
                window_end_ms: 1_000,
                mid_price: Some(1.0),
                spread_bps: Some(1.0),
                trade_count: 1,
                trade_volume: 10.0,
                slice_completeness: "complete".to_owned(),
            }],
            0,
            300_000,
            600_000,
        );
        assert_eq!(status, MarketContextStatus::NearestAvailable);
    }
}
