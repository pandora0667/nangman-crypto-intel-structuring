use crate::error::{AppError, AppResult};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStoreConfig {
    pub endpoint: Option<String>,
    pub bucket: String,
    pub region: String,
    pub force_path_style: bool,
    pub profile: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Clone)]
pub struct ObjectStore {
    client: Client,
    bucket: String,
}

impl ObjectStore {
    pub async fn connect(config: ObjectStoreConfig) -> AppResult<Self> {
        validate_config(&config)?;
        let mut loader =
            aws_config::defaults(BehaviorVersion::latest()).region(Region::new(config.region));
        if let Some(profile) = config.profile {
            loader = loader.profile_name(profile);
        }
        if let Some(endpoint) = config.endpoint {
            loader = loader.endpoint_url(endpoint.trim_end_matches('/'));
        }
        let sdk_config = loader.load().await;
        let mut s3_builder =
            S3ConfigBuilder::from(&sdk_config).force_path_style(config.force_path_style);
        if let (Some(access_key_id), Some(secret_access_key)) =
            (config.access_key_id, config.secret_access_key)
        {
            s3_builder = s3_builder.credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "intel-structuring-app-explicit-object-store",
            ));
        }
        let s3_config = s3_builder.build();
        let store = Self {
            client: Client::from_conf(s3_config),
            bucket: config.bucket,
        };
        store.head_bucket().await?;
        Ok(store)
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub async fn head_bucket(&self) -> AppResult<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|error| AppError::aws(format!("head_bucket {}: {error}", self.bucket)))?;
        Ok(())
    }

    pub async fn object_exists(&self, key: &str) -> AppResult<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) => {
                if error
                    .as_service_error()
                    .map(|service_error| service_error.is_not_found())
                    == Some(true)
                {
                    return Ok(false);
                }
                let message = error.to_string();
                if message.contains("NotFound")
                    || message.contains("404")
                    || message.contains("NoSuchKey")
                {
                    Ok(false)
                } else {
                    Err(AppError::aws(format!(
                        "head_object bucket={} key={} error={message}",
                        self.bucket, key
                    )))
                }
            }
        }
    }

    pub async fn get_bytes(&self, key: &str) -> AppResult<Vec<u8>> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| {
                AppError::aws(format!(
                    "get_object bucket={} key={} error={error}",
                    self.bucket, key
                ))
            })?;
        Ok(output
            .body
            .collect()
            .await
            .map_err(|error| AppError::aws(format!("collect body key={key}: {error}")))?
            .into_bytes()
            .to_vec())
    }

    pub async fn get_byte_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
    ) -> AppResult<Vec<u8>> {
        let end = offset
            .checked_add(length)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| AppError::validation("invalid byte range"))?;
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(format!("bytes={offset}-{end}"))
            .send()
            .await
            .map_err(|error| {
                AppError::aws(format!(
                    "get_object_range bucket={} key={} range={offset}-{end} error={error}",
                    self.bucket, key
                ))
            })?;
        Ok(output
            .body
            .collect()
            .await
            .map_err(|error| AppError::aws(format!("collect ranged body key={key}: {error}")))?
            .into_bytes()
            .to_vec())
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> AppResult<T> {
        let bytes = self.get_bytes(key).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn list_keys(&self, prefix: &str, max_keys: usize) -> AppResult<Vec<String>> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        let mut keys = Vec::new();
        let mut continuation_token = None;
        while keys.len() < max_keys {
            let remaining = max_keys.saturating_sub(keys.len()).min(i32::MAX as usize) as i32;
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .max_keys(remaining);
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }
            let output = request.send().await.map_err(|error| {
                AppError::aws(format!(
                    "list_objects_v2 bucket={} prefix={} error={error}",
                    self.bucket, prefix
                ))
            })?;
            for object in output.contents() {
                if let Some(key) = object.key() {
                    keys.push(key.to_owned());
                    if keys.len() >= max_keys {
                        break;
                    }
                }
            }
            continuation_token = output.next_continuation_token().map(ToOwned::to_owned);
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(keys)
    }

    pub async fn put_json_if_absent<T: Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> AppResult<Vec<u8>> {
        let bytes = serde_json::to_vec_pretty(value)?;
        self.put_bytes_guarded(key, bytes.clone(), "application/json", true)
            .await?;
        Ok(bytes)
    }

    pub async fn put_json_idempotent<T: Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> AppResult<Vec<u8>> {
        let bytes = serde_json::to_vec_pretty(value)?;
        self.put_bytes_idempotent(key, bytes.clone(), "application/json")
            .await?;
        Ok(bytes)
    }

    pub async fn put_jsonl_if_absent<T: Serialize>(
        &self,
        key: &str,
        records: &[T],
    ) -> AppResult<Vec<u8>> {
        let (bytes, _) = crate::jsonl::build_jsonl_chunk(records)?;
        self.put_bytes_guarded(key, bytes.clone(), "application/x-ndjson", true)
            .await?;
        Ok(bytes)
    }

    pub async fn put_jsonl_idempotent<T: Serialize>(
        &self,
        key: &str,
        records: &[T],
    ) -> AppResult<Vec<u8>> {
        let (bytes, _) = crate::jsonl::build_jsonl_chunk(records)?;
        self.put_bytes_idempotent(key, bytes.clone(), "application/x-ndjson")
            .await?;
        Ok(bytes)
    }

    pub async fn put_bytes_if_absent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: &'static str,
    ) -> AppResult<()> {
        self.put_bytes_guarded(key, bytes, content_type, true).await
    }

    pub async fn put_bytes_idempotent(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: &'static str,
    ) -> AppResult<()> {
        match self
            .put_bytes_guarded(key, bytes.clone(), content_type, true)
            .await
        {
            Ok(()) => Ok(()),
            Err(AppError::Validation(message)) if message.contains("object already exists") => {
                let existing = self.get_bytes(key).await?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(AppError::validation(format!(
                        "idempotency conflict bucket={} key={key}",
                        self.bucket
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn put_bytes_guarded(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: &'static str,
        if_absent: bool,
    ) -> AppResult<()> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(bytes));
        if if_absent {
            request = request.if_none_match("*");
        }
        request.send().await.map_err(|error| {
            let message = error.to_string();
            if if_absent && is_precondition_failure(&message) {
                AppError::validation(format!(
                    "object already exists bucket={} key={key}",
                    self.bucket
                ))
            } else {
                AppError::aws(format!(
                    "put_object bucket={} key={} error={message}",
                    self.bucket, key
                ))
            }
        })?;
        Ok(())
    }
}

fn validate_config(config: &ObjectStoreConfig) -> AppResult<()> {
    if config.bucket.trim().is_empty() {
        return Err(AppError::config("object store bucket is required"));
    }
    if config.region.trim().is_empty() {
        return Err(AppError::config("object store region is required"));
    }
    if let Some(endpoint) = &config.endpoint
        && !endpoint.starts_with("http://")
        && !endpoint.starts_with("https://")
    {
        return Err(AppError::config("object store endpoint must be http(s)"));
    }
    if config.access_key_id.is_some() != config.secret_access_key.is_some() {
        return Err(AppError::config(
            "object store explicit credentials require both access_key_id and secret_access_key",
        ));
    }
    Ok(())
}

fn is_precondition_failure(message: &str) -> bool {
    message.contains("PreconditionFailed")
        || message.contains("precondition")
        || message.contains("412")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_endpoint() {
        let config = ObjectStoreConfig {
            endpoint: Some("ftp://example.com".to_owned()),
            bucket: "b".to_owned(),
            region: "us-east-1".to_owned(),
            force_path_style: false,
            profile: None,
            access_key_id: None,
            secret_access_key: None,
        };
        assert!(validate_config(&config).is_err());
    }
}
