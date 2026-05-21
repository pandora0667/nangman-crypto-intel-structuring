use crate::error::{AppError, AppResult};
use crate::models::output::{
    ContextFlagPacket, StructuredIntelPacket, StructuredPointer, StructuringHealthEvent,
};
use crate::nats::config::NatsConfig;
use async_nats::jetstream;
use async_nats::jetstream::stream;
use bytes::Bytes;
use serde::Serialize;
use std::time::Duration;

pub struct StructuredPublisher {
    client: async_nats::Client,
    jetstream: jetstream::Context,
    stream: String,
    structured_packet_subject: String,
    context_flag_subject: String,
    health_subject: String,
}

impl StructuredPublisher {
    pub async fn connect(config: &NatsConfig) -> AppResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|error| AppError::nats(format!("connect {}: {error}", config.url)))?;
        let jetstream = jetstream::new(client.clone());
        if config.ensure_output_stream {
            jetstream
                .get_or_create_stream(stream::Config {
                    name: config.structured_stream.clone(),
                    subjects: vec![
                        config.structured_packet_subject.clone(),
                        config.context_flag_subject.clone(),
                        config.health_subject.clone(),
                    ],
                    retention: stream::RetentionPolicy::Limits,
                    storage: stream::StorageType::File,
                    max_age: Duration::from_secs(config.output_stream_max_age_secs),
                    duplicate_window: Duration::from_secs(
                        config.output_stream_duplicate_window_secs,
                    ),
                    ..Default::default()
                })
                .await
                .map_err(|error| {
                    AppError::nats(format!(
                        "get/create output stream {}: {error}",
                        config.structured_stream
                    ))
                })?;
        }
        Ok(Self {
            client,
            jetstream,
            stream: config.structured_stream.clone(),
            structured_packet_subject: config.structured_packet_subject.clone(),
            context_flag_subject: config.context_flag_subject.clone(),
            health_subject: config.health_subject.clone(),
        })
    }

    pub async fn publish_structured_pointer(
        &self,
        packet: &StructuredIntelPacket,
        pointer: &StructuredPointer,
    ) -> AppResult<()> {
        self.publish(&self.structured_packet_subject, &packet.packet_id, pointer)
            .await
    }

    pub async fn publish_context_flag_pointer(
        &self,
        flag: &ContextFlagPacket,
        pointer: &StructuredPointer,
    ) -> AppResult<()> {
        self.publish(&self.context_flag_subject, &flag.flag_packet_id, pointer)
            .await
    }

    pub async fn publish_health(&self, health: &StructuringHealthEvent) -> AppResult<()> {
        self.publish(&self.health_subject, &health.health_event_id, health)
            .await
    }

    pub async fn flush(&self) -> AppResult<()> {
        self.client
            .flush()
            .await
            .map_err(|error| AppError::nats(format!("flush: {error}")))
    }

    async fn publish<T: Serialize>(
        &self,
        subject: &str,
        message_id: &str,
        payload: &T,
    ) -> AppResult<()> {
        let bytes = Bytes::from(serde_json::to_vec(payload)?);
        let message = jetstream::message::PublishMessage::build()
            .expected_stream(&self.stream)
            .message_id(message_id)
            .payload(bytes);
        let ack = self
            .jetstream
            .send_publish(subject.to_owned(), message)
            .await
            .map_err(|error| AppError::nats(format!("publish {subject}: {error}")))?
            .await
            .map_err(|error| AppError::nats(format!("publish ack {subject}: {error}")))?;
        if ack.stream != self.stream {
            return Err(AppError::nats(format!(
                "publish ack stream mismatch expected={} actual={}",
                self.stream, ack.stream
            )));
        }
        Ok(())
    }
}
