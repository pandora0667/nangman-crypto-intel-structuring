#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsConfig {
    pub url: String,
    pub raw_stream: String,
    pub raw_subject: String,
    pub raw_consumer: String,
    pub raw_deliver_policy: String,
    pub structured_stream: String,
    pub structured_packet_subject: String,
    pub context_flag_subject: String,
    pub health_subject: String,
    pub ensure_output_stream: bool,
    pub output_stream_max_age_secs: u64,
    pub output_stream_duplicate_window_secs: u64,
    pub ack_wait_secs: u64,
    pub max_deliver: i64,
    pub batch_size: usize,
}
