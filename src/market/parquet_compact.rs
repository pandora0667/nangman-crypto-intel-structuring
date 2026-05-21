use crate::error::{AppError, AppResult};
use crate::models::market::{MarketL1ReadPlan, MarketSymbolSummary};
use crate::storage::object_store::ObjectStore;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};

pub async fn read_symbol_summaries(
    store: &ObjectStore,
    plan: &MarketL1ReadPlan,
    symbols: &[String],
) -> AppResult<Vec<MarketSymbolSummary>> {
    let wanted = symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect::<BTreeSet<_>>();
    if wanted.is_empty() {
        return Ok(Vec::new());
    }

    let mut summaries = BTreeMap::<String, MarketSymbolSummary>::new();
    for key in &plan.output_object_keys {
        let bytes = Bytes::from(store.get_bytes(key).await?);
        for summary in scan_parquet_bytes(bytes, &wanted)? {
            summaries
                .entry(format!("{}:{}", summary.symbol, summary.venue))
                .or_insert(summary);
        }
    }
    Ok(summaries.into_values().collect())
}

fn scan_parquet_bytes(
    bytes: Bytes,
    wanted: &BTreeSet<String>,
) -> AppResult<Vec<MarketSymbolSummary>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|error| AppError::parquet(error.to_string()))?
        .with_batch_size(2048)
        .build()
        .map_err(|error| AppError::parquet(error.to_string()))?;

    let mut output = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| AppError::parquet(error.to_string()))?;
        output.extend(extract_batch_summaries(&batch, wanted)?);
    }
    Ok(output)
}

fn extract_batch_summaries(
    batch: &RecordBatch,
    wanted: &BTreeSet<String>,
) -> AppResult<Vec<MarketSymbolSummary>> {
    let base_asset = string_col(batch, "base_asset")?;
    let symbol_canonical = string_col(batch, "symbol_canonical")?;
    let venue = string_col(batch, "venue")?;
    let slice_completeness = string_col(batch, "slice_completeness")?;
    let window_start_ms = i64_col(batch, "window_start_ms")?;
    let window_end_ms = i64_col(batch, "window_end_ms")?;
    let trade_count = i64_col(batch, "trade_count")?;
    let trade_volume = f64_col(batch, "trade_volume")?;
    let mid_price = optional_f64_col(batch, "mid_price")?;
    let spread_bps = optional_f64_col(batch, "spread_bps")?;

    let mut summaries = Vec::new();
    for index in 0..batch.num_rows() {
        let symbol = base_asset.value(index).to_ascii_uppercase();
        let canonical = symbol_canonical.value(index).to_ascii_uppercase();
        if !wanted.contains(&symbol) && !wanted.contains(&canonical) {
            continue;
        }
        summaries.push(MarketSymbolSummary {
            symbol,
            venue: venue.value(index).to_owned(),
            window_start_ms: window_start_ms.value(index),
            window_end_ms: window_end_ms.value(index),
            mid_price: nullable_value(mid_price, index),
            spread_bps: nullable_value(spread_bps, index),
            trade_count: trade_count.value(index),
            trade_volume: trade_volume.value(index),
            slice_completeness: slice_completeness.value(index).to_owned(),
        });
    }
    Ok(summaries)
}

fn string_col<'a>(batch: &'a RecordBatch, name: &str) -> AppResult<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| AppError::parquet(format!("missing column {name}")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AppError::parquet(format!("column {name} is not StringArray")))
}

fn i64_col<'a>(batch: &'a RecordBatch, name: &str) -> AppResult<&'a Int64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| AppError::parquet(format!("missing column {name}")))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| AppError::parquet(format!("column {name} is not Int64Array")))
}

fn f64_col<'a>(batch: &'a RecordBatch, name: &str) -> AppResult<&'a Float64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| AppError::parquet(format!("missing column {name}")))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| AppError::parquet(format!("column {name} is not Float64Array")))
}

fn optional_f64_col<'a>(batch: &'a RecordBatch, name: &str) -> AppResult<Option<&'a Float64Array>> {
    match batch.column_by_name(name) {
        Some(array) => Ok(Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| AppError::parquet(format!("column {name} is not Float64Array")))?,
        )),
        None => Ok(None),
    }
}

fn nullable_value(array: Option<&Float64Array>, index: usize) -> Option<f64> {
    let array = array?;
    if array.is_null(index) {
        None
    } else {
        Some(array.value(index))
    }
}
