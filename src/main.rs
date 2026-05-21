use intel_structuring_app::ai::bedrock::BedrockModelProvider;
use intel_structuring_app::config::Args;
use intel_structuring_app::error::{AppError, AppResult};
use intel_structuring_app::market::reader::MarketL1Reader;
use intel_structuring_app::nats::consumer::{RawIntelConsumer, RawIntelMessage};
use intel_structuring_app::nats::publisher::StructuredPublisher;
use intel_structuring_app::storage::object_store::ObjectStore;
use intel_structuring_app::structuring::router::ModelRouter;
use intel_structuring_app::workflow::processor::IntelStructuringProcessor;

#[tokio::main]
async fn main() -> AppResult<()> {
    let args = Args::parse(std::env::args())?;
    let rustfs_store = ObjectStore::connect(args.rustfs_store.clone()).await?;
    let output_store = ObjectStore::connect(args.output_store.clone()).await?;
    let market_store = ObjectStore::connect(args.market_l1_store.clone()).await?;
    let market_reader = MarketL1Reader::new(
        market_store,
        args.market_l1_window_ms,
        args.processing.market_context_window_radius,
        args.processing.market_context_latest_before_lookback_ms,
        args.processing.market_context_stale_after_ms,
    );
    let model_provider = BedrockModelProvider::new(args.bedrock.clone()).await?;
    let router = ModelRouter::new(model_provider, args.model_policy.clone());
    let publisher = StructuredPublisher::connect(&args.nats).await?;
    let mut consumer = RawIntelConsumer::connect(&args.nats).await?;

    let processor = IntelStructuringProcessor::new(
        rustfs_store,
        output_store,
        market_reader,
        router,
        publisher,
        args.processing.clone(),
    );

    let mut processed = 0usize;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        if let Some(max_messages) = args.max_messages
            && processed >= max_messages
        {
            break;
        }

        tokio::select! {
            shutdown_result = &mut shutdown => {
                shutdown_result?;
                break;
            }
            message_result = consumer.next_message() => {
                let Some(message) = (match message_result {
                    Ok(message) => message,
                    Err(error) => {
                        log_message_failure("fetch_failed", &error)?;
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                }) else {
                    if args.exit_on_idle {
                        break;
                    }
                    continue;
                };
                process_and_ack_message(&processor, message).await?;
                processed += 1;
            }
        }
    }

    Ok(())
}

async fn process_and_ack_message<P>(
    processor: &IntelStructuringProcessor<P>,
    message: RawIntelMessage,
) -> AppResult<()>
where
    P: intel_structuring_app::ai::contract::ModelProvider,
{
    match processor.process_nats_message(&message).await {
        Ok(ack) if ack.should_ack() => {
            if let Err(error) = message.ack().await {
                log_message_failure("ack_failed", &error)?;
            }
        }
        Ok(_) => {}
        Err(error) => {
            log_message_failure("processing_failed", &error)?;
        }
    }
    Ok(())
}

fn log_message_failure(event: &str, error: &AppError) -> AppResult<()> {
    eprintln!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "level": "error",
            "event": event,
            "ack": "no",
            "error": error.to_string()
        }))?
    );
    Ok(())
}

async fn shutdown_signal() -> AppResult<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
    }
    Ok(())
}
