use crate::error::AppResult;
use crate::models::market::MarketContextStatus;
use crate::models::output::{ModelTierUsed, TerminalDecision};
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Serialize)]
pub struct ProcessingMetric {
    pub raw_event_id: String,
    pub packet_id: String,
    pub model_tier_used: ModelTierUsed,
    pub terminal_decision: TerminalDecision,
    pub market_context_status: MarketContextStatus,
    pub ack_ready: bool,
    pub fallback_count: usize,
    pub conflict_count: usize,
    pub haiku_invocation_count: usize,
    pub sonnet_invocation_count: usize,
    pub numeric_snapshot_count: usize,
    pub stale_market_context_count: usize,
    pub sonnet_on_numeric_snapshot_count: usize,
}

pub fn emit_processing_metric(metric: &ProcessingMetric) -> AppResult<()> {
    let document = json!({
        "_aws": {
            "Timestamp": crate::time::now_ms(),
            "CloudWatchMetrics": [{
                "Namespace": "NangmanCrypto/IntelL1",
                "Dimensions": [["Service", "ModelTier"]],
                "Metrics": [
                    {"Name": "ProcessedEventCount", "Unit": "Count"},
                    {"Name": "AckReadyCount", "Unit": "Count"},
                    {"Name": "FallbackCount", "Unit": "Count"},
                    {"Name": "ConflictCount", "Unit": "Count"},
                    {"Name": "HaikuInvocationCount", "Unit": "Count"},
                    {"Name": "SonnetInvocationCount", "Unit": "Count"},
                    {"Name": "NumericSnapshotCount", "Unit": "Count"},
                    {"Name": "StaleMarketContextCount", "Unit": "Count"},
                    {"Name": "SonnetOnNumericSnapshotCount", "Unit": "Count"}
                ]
            }]
        },
        "Service": "intel-structuring-app",
        "ModelTier": format!("{:?}", metric.model_tier_used),
        "TerminalDecision": format!("{:?}", metric.terminal_decision),
        "MarketContextStatus": format!("{:?}", metric.market_context_status),
        "ProcessedEventCount": 1,
        "AckReadyCount": usize::from(metric.ack_ready),
        "FallbackCount": metric.fallback_count,
        "ConflictCount": metric.conflict_count,
        "HaikuInvocationCount": metric.haiku_invocation_count,
        "SonnetInvocationCount": metric.sonnet_invocation_count,
        "NumericSnapshotCount": metric.numeric_snapshot_count,
        "StaleMarketContextCount": metric.stale_market_context_count,
        "SonnetOnNumericSnapshotCount": metric.sonnet_on_numeric_snapshot_count,
        "raw_event_id": metric.raw_event_id,
        "packet_id": metric.packet_id
    });
    println!("{}", serde_json::to_string(&document)?);
    Ok(())
}
