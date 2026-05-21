use crate::ai::bedrock::BedrockConfig;
use crate::error::{AppError, AppResult};
use crate::models::constants::{
    DEFAULT_ESCALATION_MODEL_ID, DEFAULT_PRIMARY_MODEL_ID, STRUCTURING_POLICY_VERSION,
};
use crate::nats::config::NatsConfig;
use crate::storage::object_store::ObjectStoreConfig;

pub const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
pub const DEFAULT_RAW_STREAM: &str = "RAW_INTEL";
pub const DEFAULT_RAW_SUBJECT: &str = "raw_intel_event.created";
pub const DEFAULT_RAW_CONSUMER: &str = "intel-structuring-l1";
pub const DEFAULT_RAW_DELIVER_POLICY: &str = "all";
pub const DEFAULT_STRUCTURED_STREAM: &str = "STRUCTURED_INTEL";
pub const DEFAULT_STRUCTURED_PACKET_SUBJECT: &str = "structured_intel_packet.created";
pub const DEFAULT_CONTEXT_FLAG_SUBJECT: &str = "context_flag_packet.created";
pub const DEFAULT_HEALTH_SUBJECT: &str = "structuring_health_event.created";
pub const DEFAULT_RUSTFS_ENDPOINT: &str = "https://s3.nangman.cloud";
pub const DEFAULT_RUSTFS_BUCKET: &str = "intel-crawl-app-l0";
pub const DEFAULT_RUSTFS_REGION: &str = "us-east-1";
pub const DEFAULT_OUTPUT_BUCKET: &str = "nangman-crypto-dev-intel-structuring-l1-962214";
pub const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
pub const DEFAULT_MARKET_L1_BUCKET: &str = "nangman-crypto-dev-market-ingest-l1-962214";
pub const DEFAULT_MARKET_L1_WINDOW_MS: i64 = 1_000;
pub const DEFAULT_MARKET_CONTEXT_LATEST_BEFORE_LOOKBACK_MS: i64 = 6 * 60 * 60 * 1_000;
pub const DEFAULT_MARKET_CONTEXT_STALE_AFTER_MS: i64 = 10 * 60 * 1_000;
pub const DEFAULT_MARKET_CONTEXT_RETRY_INTERVAL_MS: i64 = 5 * 60 * 1_000;
pub const DEFAULT_MARKET_CONTEXT_EXPIRE_AFTER_MS: i64 = 6 * 60 * 60 * 1_000;

#[derive(Debug, Clone)]
pub struct Args {
    pub nats: NatsConfig,
    pub rustfs_store: ObjectStoreConfig,
    pub output_store: ObjectStoreConfig,
    pub market_l1_store: ObjectStoreConfig,
    pub market_l1_window_ms: i64,
    pub bedrock: BedrockConfig,
    pub model_policy: ModelPolicyConfig,
    pub processing: ProcessingConfig,
    pub max_messages: Option<usize>,
    pub exit_on_idle: bool,
}

#[derive(Debug, Clone)]
pub struct ModelPolicyConfig {
    pub primary_model_id: String,
    pub escalation_model_id: String,
    pub escalate_if_confidence_below: f64,
    pub sonnet_budget_ratio: f64,
    pub enable_bedrock: bool,
}

#[derive(Debug, Clone)]
pub struct ProcessingConfig {
    pub structuring_policy_version: String,
    pub chunk_max_records: usize,
    pub market_context_window_radius: i64,
    pub market_context_latest_before_lookback_ms: i64,
    pub market_context_stale_after_ms: i64,
    pub market_context_retry_interval_ms: i64,
    pub market_context_expire_after_ms: i64,
    pub max_raw_body_chars: usize,
    pub story_member_scan_limit: usize,
}

impl Args {
    pub fn parse<I>(mut values: I) -> AppResult<Self>
    where
        I: Iterator<Item = String>,
    {
        let _program = values.next();
        let mut args = Self::from_env();

        while let Some(arg) = values.next() {
            match arg.as_str() {
                "--nats-url" => args.nats.url = required_value(&mut values, "--nats-url")?,
                "--raw-stream" => {
                    args.nats.raw_stream = required_value(&mut values, "--raw-stream")?
                }
                "--raw-subject" => {
                    args.nats.raw_subject = required_value(&mut values, "--raw-subject")?
                }
                "--raw-consumer" => {
                    args.nats.raw_consumer = required_value(&mut values, "--raw-consumer")?
                }
                "--structured-stream" => {
                    args.nats.structured_stream =
                        required_value(&mut values, "--structured-stream")?
                }
                "--structured-packet-subject" => {
                    args.nats.structured_packet_subject =
                        required_value(&mut values, "--structured-packet-subject")?
                }
                "--context-flag-subject" => {
                    args.nats.context_flag_subject =
                        required_value(&mut values, "--context-flag-subject")?
                }
                "--health-subject" => {
                    args.nats.health_subject = required_value(&mut values, "--health-subject")?
                }
                "--ensure-output-stream" => {
                    args.nats.ensure_output_stream =
                        parse_bool(&required_value(&mut values, "--ensure-output-stream")?)?;
                }
                "--rustfs-endpoint" => {
                    args.rustfs_store.endpoint =
                        Some(required_value(&mut values, "--rustfs-endpoint")?)
                }
                "--rustfs-bucket" => {
                    args.rustfs_store.bucket = required_value(&mut values, "--rustfs-bucket")?
                }
                "--rustfs-region" => {
                    args.rustfs_store.region = required_value(&mut values, "--rustfs-region")?
                }
                "--output-bucket" => {
                    args.output_store.bucket = required_value(&mut values, "--output-bucket")?
                }
                "--aws-region" => {
                    let region = required_value(&mut values, "--aws-region")?;
                    args.output_store.region = region.clone();
                    args.market_l1_store.region = region.clone();
                    args.bedrock.region = region;
                }
                "--aws-profile" => {
                    let profile = required_value(&mut values, "--aws-profile")?;
                    args.output_store.profile = Some(profile.clone());
                    args.market_l1_store.profile = Some(profile.clone());
                    args.bedrock.profile = Some(profile);
                }
                "--market-l1-bucket" => {
                    args.market_l1_store.bucket = required_value(&mut values, "--market-l1-bucket")?
                }
                "--market-l1-window-ms" => {
                    args.market_l1_window_ms =
                        parse_i64(&required_value(&mut values, "--market-l1-window-ms")?)?;
                }
                "--enable-bedrock" => {
                    args.model_policy.enable_bedrock =
                        parse_bool(&required_value(&mut values, "--enable-bedrock")?)?;
                    args.bedrock.enabled = args.model_policy.enable_bedrock;
                }
                "--primary-model-id" => {
                    args.model_policy.primary_model_id =
                        required_value(&mut values, "--primary-model-id")?;
                    args.bedrock.primary_model_id = args.model_policy.primary_model_id.clone();
                }
                "--escalation-model-id" => {
                    args.model_policy.escalation_model_id =
                        required_value(&mut values, "--escalation-model-id")?;
                    args.bedrock.escalation_model_id =
                        args.model_policy.escalation_model_id.clone();
                }
                "--max-messages" => {
                    args.max_messages = Some(parse_usize(&required_value(
                        &mut values,
                        "--max-messages",
                    )?)?);
                }
                "--exit-on-idle" => {
                    args.exit_on_idle =
                        parse_bool(&required_value(&mut values, "--exit-on-idle")?)?;
                }
                "--chunk-max-records" => {
                    args.processing.chunk_max_records =
                        parse_usize(&required_value(&mut values, "--chunk-max-records")?)?;
                }
                "--help" | "-h" => return Err(AppError::config(help())),
                other => {
                    return Err(AppError::config(format!(
                        "unknown argument: {other}\n\n{}",
                        help()
                    )));
                }
            }
        }

        args.validate()?;
        Ok(args)
    }

    fn from_env() -> Self {
        let aws_region = env_or("AWS_REGION", DEFAULT_AWS_REGION);
        let enable_bedrock = env_bool("INTEL_L1_ENABLE_BEDROCK", false);
        let primary_model_id = env_or("INTEL_L1_PRIMARY_MODEL_ID", DEFAULT_PRIMARY_MODEL_ID);
        let escalation_model_id =
            env_or("INTEL_L1_ESCALATION_MODEL_ID", DEFAULT_ESCALATION_MODEL_ID);

        Self {
            nats: NatsConfig {
                url: env_or("NATS_URL", DEFAULT_NATS_URL),
                raw_stream: env_or("INTEL_L1_RAW_NATS_STREAM", DEFAULT_RAW_STREAM),
                raw_subject: env_or("INTEL_L1_RAW_NATS_SUBJECT", DEFAULT_RAW_SUBJECT),
                raw_consumer: env_or("INTEL_L1_RAW_NATS_CONSUMER", DEFAULT_RAW_CONSUMER),
                raw_deliver_policy: env_or(
                    "INTEL_L1_RAW_DELIVER_POLICY",
                    DEFAULT_RAW_DELIVER_POLICY,
                ),
                structured_stream: env_or("INTEL_L1_OUTPUT_NATS_STREAM", DEFAULT_STRUCTURED_STREAM),
                structured_packet_subject: env_or(
                    "INTEL_L1_STRUCTURED_PACKET_SUBJECT",
                    DEFAULT_STRUCTURED_PACKET_SUBJECT,
                ),
                context_flag_subject: env_or(
                    "INTEL_L1_CONTEXT_FLAG_SUBJECT",
                    DEFAULT_CONTEXT_FLAG_SUBJECT,
                ),
                health_subject: env_or("INTEL_L1_HEALTH_SUBJECT", DEFAULT_HEALTH_SUBJECT),
                ensure_output_stream: env_bool("INTEL_L1_ENSURE_OUTPUT_STREAM", true),
                output_stream_max_age_secs: env_u64(
                    "INTEL_L1_OUTPUT_STREAM_MAX_AGE_SECS",
                    14 * 24 * 60 * 60,
                ),
                output_stream_duplicate_window_secs: env_u64(
                    "INTEL_L1_OUTPUT_STREAM_DUPLICATE_WINDOW_SECS",
                    24 * 60 * 60,
                ),
                ack_wait_secs: env_u64("INTEL_L1_RAW_ACK_WAIT_SECS", 300),
                max_deliver: env_i64("INTEL_L1_RAW_MAX_DELIVER", 20),
                batch_size: env_usize("INTEL_L1_RAW_BATCH_SIZE", 1),
            },
            rustfs_store: ObjectStoreConfig {
                endpoint: Some(env_or(
                    "INTEL_L1_L0_RUSTFS_ENDPOINT",
                    DEFAULT_RUSTFS_ENDPOINT,
                )),
                bucket: env_or("INTEL_L1_L0_RUSTFS_BUCKET", DEFAULT_RUSTFS_BUCKET),
                region: env_or("INTEL_L1_L0_RUSTFS_REGION", DEFAULT_RUSTFS_REGION),
                force_path_style: env_bool("INTEL_L1_L0_RUSTFS_FORCE_PATH_STYLE", true),
                profile: env_opt("INTEL_L1_L0_RUSTFS_AWS_PROFILE"),
                access_key_id: env_opt("INTEL_L1_L0_RUSTFS_ACCESS_KEY_ID"),
                secret_access_key: env_opt("INTEL_L1_L0_RUSTFS_SECRET_ACCESS_KEY"),
            },
            output_store: ObjectStoreConfig {
                endpoint: env_opt("INTEL_L1_OUTPUT_S3_ENDPOINT"),
                bucket: env_or("INTEL_L1_OUTPUT_S3_BUCKET", DEFAULT_OUTPUT_BUCKET),
                region: env_or("INTEL_L1_OUTPUT_S3_REGION", &aws_region),
                force_path_style: env_bool("INTEL_L1_OUTPUT_S3_FORCE_PATH_STYLE", false),
                profile: env_opt("AWS_PROFILE"),
                access_key_id: None,
                secret_access_key: None,
            },
            market_l1_store: ObjectStoreConfig {
                endpoint: env_opt("INTEL_L1_MARKET_S3_ENDPOINT"),
                bucket: env_or("INTEL_L1_MARKET_L1_BUCKET", DEFAULT_MARKET_L1_BUCKET),
                region: env_or("INTEL_L1_MARKET_S3_REGION", &aws_region),
                force_path_style: env_bool("INTEL_L1_MARKET_S3_FORCE_PATH_STYLE", false),
                profile: env_opt("AWS_PROFILE"),
                access_key_id: None,
                secret_access_key: None,
            },
            market_l1_window_ms: env_i64("INTEL_L1_MARKET_WINDOW_MS", DEFAULT_MARKET_L1_WINDOW_MS),
            bedrock: BedrockConfig {
                enabled: enable_bedrock,
                region: env_or("BEDROCK_REGION", &aws_region),
                profile: env_opt("AWS_PROFILE"),
                primary_model_id: primary_model_id.clone(),
                escalation_model_id: escalation_model_id.clone(),
                max_input_chars: env_usize("INTEL_L1_MODEL_MAX_INPUT_CHARS", 12_000),
                max_output_tokens: env_i32("INTEL_L1_MODEL_MAX_OUTPUT_TOKENS", 1200),
                temperature: env_f32("INTEL_L1_MODEL_TEMPERATURE", 0.0),
            },
            model_policy: ModelPolicyConfig {
                primary_model_id,
                escalation_model_id,
                escalate_if_confidence_below: env_f64(
                    "INTEL_L1_ESCALATE_IF_CONFIDENCE_BELOW",
                    0.65,
                ),
                sonnet_budget_ratio: env_f64("INTEL_L1_SONNET_BUDGET_RATIO", 0.15),
                enable_bedrock,
            },
            processing: ProcessingConfig {
                structuring_policy_version: env_or(
                    "INTEL_L1_STRUCTURING_POLICY_VERSION",
                    STRUCTURING_POLICY_VERSION,
                ),
                chunk_max_records: env_usize("INTEL_L1_CHUNK_MAX_RECORDS", 1000),
                market_context_window_radius: env_i64("INTEL_L1_MARKET_CONTEXT_RADIUS_WINDOWS", 1),
                market_context_latest_before_lookback_ms: env_i64(
                    "INTEL_L1_MARKET_CONTEXT_LATEST_BEFORE_LOOKBACK_MS",
                    DEFAULT_MARKET_CONTEXT_LATEST_BEFORE_LOOKBACK_MS,
                ),
                market_context_stale_after_ms: env_i64(
                    "INTEL_L1_MARKET_CONTEXT_STALE_AFTER_MS",
                    DEFAULT_MARKET_CONTEXT_STALE_AFTER_MS,
                ),
                market_context_retry_interval_ms: env_i64(
                    "INTEL_L1_MARKET_CONTEXT_RETRY_INTERVAL_MS",
                    DEFAULT_MARKET_CONTEXT_RETRY_INTERVAL_MS,
                ),
                market_context_expire_after_ms: env_i64(
                    "INTEL_L1_MARKET_CONTEXT_EXPIRE_AFTER_MS",
                    DEFAULT_MARKET_CONTEXT_EXPIRE_AFTER_MS,
                ),
                max_raw_body_chars: env_usize("INTEL_L1_MAX_RAW_BODY_CHARS", 20_000),
                story_member_scan_limit: env_usize("INTEL_L1_STORY_MEMBER_SCAN_LIMIT", 128),
            },
            max_messages: env_opt("INTEL_L1_MAX_MESSAGES").and_then(|value| value.parse().ok()),
            exit_on_idle: env_bool("INTEL_L1_EXIT_ON_IDLE", false),
        }
    }

    fn validate(&self) -> AppResult<()> {
        validate_non_empty(&self.nats.url, "NATS_URL")?;
        validate_non_empty(&self.output_store.bucket, "INTEL_L1_OUTPUT_S3_BUCKET")?;
        validate_non_empty(&self.market_l1_store.bucket, "INTEL_L1_MARKET_L1_BUCKET")?;
        validate_non_empty(&self.rustfs_store.bucket, "INTEL_L1_L0_RUSTFS_BUCKET")?;
        if self.market_l1_window_ms <= 0 {
            return Err(AppError::config(
                "INTEL_L1_MARKET_WINDOW_MS must be positive",
            ));
        }
        if self.nats.ack_wait_secs == 0 {
            return Err(AppError::config(
                "INTEL_L1_RAW_ACK_WAIT_SECS must be positive",
            ));
        }
        if self.nats.max_deliver <= 0 {
            return Err(AppError::config(
                "INTEL_L1_RAW_MAX_DELIVER must be positive",
            ));
        }
        if self.nats.batch_size == 0 {
            return Err(AppError::config("INTEL_L1_RAW_BATCH_SIZE must be positive"));
        }
        validate_deliver_policy(&self.nats.raw_deliver_policy)?;
        if self.nats.output_stream_max_age_secs == 0 {
            return Err(AppError::config(
                "INTEL_L1_OUTPUT_STREAM_MAX_AGE_SECS must be positive",
            ));
        }
        if self.nats.output_stream_duplicate_window_secs == 0 {
            return Err(AppError::config(
                "INTEL_L1_OUTPUT_STREAM_DUPLICATE_WINDOW_SECS must be positive",
            ));
        }
        validate_ratio(
            self.model_policy.escalate_if_confidence_below,
            "INTEL_L1_ESCALATE_IF_CONFIDENCE_BELOW",
        )?;
        validate_ratio(
            self.model_policy.sonnet_budget_ratio,
            "INTEL_L1_SONNET_BUDGET_RATIO",
        )?;
        if self.processing.chunk_max_records == 0 {
            return Err(AppError::config(
                "INTEL_L1_CHUNK_MAX_RECORDS must be positive",
            ));
        }
        if self.processing.story_member_scan_limit == 0 {
            return Err(AppError::config(
                "INTEL_L1_STORY_MEMBER_SCAN_LIMIT must be positive",
            ));
        }
        if self.processing.market_context_latest_before_lookback_ms <= 0 {
            return Err(AppError::config(
                "INTEL_L1_MARKET_CONTEXT_LATEST_BEFORE_LOOKBACK_MS must be positive",
            ));
        }
        if self.processing.market_context_stale_after_ms <= 0 {
            return Err(AppError::config(
                "INTEL_L1_MARKET_CONTEXT_STALE_AFTER_MS must be positive",
            ));
        }
        if self.processing.market_context_retry_interval_ms <= 0 {
            return Err(AppError::config(
                "INTEL_L1_MARKET_CONTEXT_RETRY_INTERVAL_MS must be positive",
            ));
        }
        if self.processing.market_context_expire_after_ms <= 0 {
            return Err(AppError::config(
                "INTEL_L1_MARKET_CONTEXT_EXPIRE_AFTER_MS must be positive",
            ));
        }
        Ok(())
    }
}

fn required_value<I>(values: &mut I, name: &str) -> AppResult<String>
where
    I: Iterator<Item = String>,
{
    values
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::config(format!("{name} requires a value")))
}

fn parse_bool(value: &str) -> AppResult<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(AppError::config(format!("{value} must be true or false"))),
    }
}

fn parse_usize(value: &str) -> AppResult<usize> {
    value
        .parse::<usize>()
        .map_err(|_| AppError::config(format!("{value} must be a positive integer")))
}

fn parse_i64(value: &str) -> AppResult<i64> {
    value
        .parse::<i64>()
        .map_err(|_| AppError::config(format!("{value} must be an integer")))
}

fn validate_non_empty(value: &str, name: &str) -> AppResult<()> {
    if value.trim().is_empty() {
        Err(AppError::config(format!("{name} must not be empty")))
    } else {
        Ok(())
    }
}

fn validate_ratio(value: f64, name: &str) -> AppResult<()> {
    if !(0.0..=1.0).contains(&value) {
        Err(AppError::config(format!("{name} must be between 0 and 1")))
    } else {
        Ok(())
    }
}

fn validate_deliver_policy(value: &str) -> AppResult<()> {
    match value {
        "all" | "new" | "last" | "last_per_subject" => Ok(()),
        other => Err(AppError::config(format!(
            "INTEL_L1_RAW_DELIVER_POLICY must be one of all,new,last,last_per_subject, got {other}"
        ))),
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_opt(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| parse_bool(&value).ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_i32(name: &str, default: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f32(name: &str, default: f32) -> f32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn help() -> String {
    "Usage: intel-structuring-app [--max-messages N] [--exit-on-idle true|false] [--enable-bedrock true|false]".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cli_overrides() {
        let args = Args::parse(
            [
                "intel-structuring-app",
                "--max-messages",
                "1",
                "--exit-on-idle",
                "true",
                "--enable-bedrock",
                "false",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();

        assert_eq!(args.max_messages, Some(1));
        assert!(args.exit_on_idle);
        assert!(!args.model_policy.enable_bedrock);
        assert_eq!(
            args.processing.market_context_latest_before_lookback_ms,
            DEFAULT_MARKET_CONTEXT_LATEST_BEFORE_LOOKBACK_MS
        );
        assert_eq!(
            args.processing.market_context_stale_after_ms,
            DEFAULT_MARKET_CONTEXT_STALE_AFTER_MS
        );
    }
}
