use crate::error::{AppError, AppResult};
use crate::nats::config::NatsConfig;
use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use futures_util::StreamExt;
use std::time::Duration;

pub struct RawIntelConsumer {
    consumer: PullConsumer,
    batch_size: usize,
}

pub struct RawIntelMessage {
    inner: async_nats::jetstream::Message,
}

impl RawIntelConsumer {
    pub async fn connect(config: &NatsConfig) -> AppResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|error| AppError::nats(format!("connect {}: {error}", config.url)))?;
        let jetstream = jetstream::new(client);
        let stream = jetstream
            .get_stream(&config.raw_stream)
            .await
            .map_err(|error| {
                AppError::nats(format!("get raw stream {}: {error}", config.raw_stream))
            })?;
        let consumer = stream
            .get_or_create_consumer(
                &config.raw_consumer,
                jetstream::consumer::pull::Config {
                    durable_name: Some(config.raw_consumer.clone()),
                    filter_subject: config.raw_subject.clone(),
                    ack_policy: AckPolicy::Explicit,
                    ack_wait: Duration::from_secs(config.ack_wait_secs),
                    max_deliver: config.max_deliver,
                    max_ack_pending: config.batch_size as i64,
                    deliver_policy: raw_deliver_policy(&config.raw_deliver_policy)?,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| {
                AppError::nats(format!(
                    "get/create raw consumer {} on stream {}: {error}",
                    config.raw_consumer, config.raw_stream
                ))
            })?;
        Ok(Self {
            consumer,
            batch_size: config.batch_size.max(1),
        })
    }

    pub async fn next_message(&mut self) -> AppResult<Option<RawIntelMessage>> {
        let mut messages = self
            .consumer
            .fetch()
            .max_messages(self.batch_size)
            .expires(Duration::from_secs(5))
            .messages()
            .await
            .map_err(|error| AppError::nats(format!("fetch raw messages: {error}")))?;
        match messages.next().await {
            Some(Ok(message)) => Ok(Some(RawIntelMessage { inner: message })),
            Some(Err(error)) => Err(AppError::nats(format!("read raw message: {error}"))),
            None => Ok(None),
        }
    }
}

fn raw_deliver_policy(value: &str) -> AppResult<DeliverPolicy> {
    match value {
        "all" => Ok(DeliverPolicy::All),
        "new" => Ok(DeliverPolicy::New),
        "last" => Ok(DeliverPolicy::Last),
        "last_per_subject" => Ok(DeliverPolicy::LastPerSubject),
        other => Err(AppError::config(format!(
            "unsupported INTEL_L1_RAW_DELIVER_POLICY: {other}"
        ))),
    }
}

impl RawIntelMessage {
    pub fn payload(&self) -> &[u8] {
        &self.inner.payload
    }

    pub async fn ack(self) -> AppResult<()> {
        self.inner
            .double_ack()
            .await
            .map_err(|error| AppError::nats(format!("raw double ack failed: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_deliver_policies() {
        assert!(matches!(
            raw_deliver_policy("all").unwrap(),
            DeliverPolicy::All
        ));
        assert!(matches!(
            raw_deliver_policy("new").unwrap(),
            DeliverPolicy::New
        ));
        assert!(matches!(
            raw_deliver_policy("last").unwrap(),
            DeliverPolicy::Last
        ));
        assert!(matches!(
            raw_deliver_policy("last_per_subject").unwrap(),
            DeliverPolicy::LastPerSubject
        ));
    }

    #[test]
    fn rejects_unknown_deliver_policy() {
        assert!(raw_deliver_policy("oldest").is_err());
    }
}
