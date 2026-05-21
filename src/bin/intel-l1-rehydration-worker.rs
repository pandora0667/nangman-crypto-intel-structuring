use intel_structuring_app::config::Args;
use intel_structuring_app::error::{AppError, AppResult};
use intel_structuring_app::market::reader::MarketL1Reader;
use intel_structuring_app::nats::publisher::StructuredPublisher;
use intel_structuring_app::storage::object_store::ObjectStore;
use intel_structuring_app::workflow::rehydration::PendingMarketContextRehydrator;

const DEFAULT_MAX_PACKETS: usize = 512;

#[tokio::main]
async fn main() -> AppResult<()> {
    let args = Args::parse(std::iter::once("intel-l1-rehydration-worker".to_owned()))?;
    let max_packets = parse_max_packets(std::env::args().skip(1))?;
    let output_store = ObjectStore::connect(args.output_store.clone()).await?;
    let market_store = ObjectStore::connect(args.market_l1_store.clone()).await?;
    let market_reader = MarketL1Reader::new(
        market_store,
        args.market_l1_window_ms,
        args.processing.market_context_window_radius,
        args.processing.market_context_latest_before_lookback_ms,
        args.processing.market_context_stale_after_ms,
    );
    let publisher = StructuredPublisher::connect(&args.nats).await?;
    let rehydrator = PendingMarketContextRehydrator::new(
        output_store,
        market_reader,
        publisher,
        args.processing,
    );
    let published = rehydrator.run_once(max_packets).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "mode": "pending_market_context_rehydration",
            "max_packets": max_packets,
            "published_revisions": published,
        }))?
    );
    Ok(())
}

fn parse_max_packets(values: impl Iterator<Item = String>) -> AppResult<usize> {
    let mut max_packets = DEFAULT_MAX_PACKETS;
    let mut values = values.peekable();
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--max-packets" => {
                let Some(value) = values.next() else {
                    return Err(AppError::config("--max-packets requires a value"));
                };
                max_packets = value
                    .parse::<usize>()
                    .map_err(|error| AppError::config(format!("invalid --max-packets: {error}")))?;
            }
            "--help" | "-h" => {
                return Err(AppError::config(
                    "intel-l1-rehydration-worker [--max-packets <positive integer>]",
                ));
            }
            other => {
                return Err(AppError::config(format!(
                    "unknown rehydration argument: {other}"
                )));
            }
        }
    }
    if max_packets == 0 {
        return Err(AppError::config("--max-packets must be positive"));
    }
    Ok(max_packets)
}
