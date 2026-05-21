use std::error::Error;
use std::fmt;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug)]
pub enum AppError {
    Config(String),
    Validation(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Aws(String),
    Nats(String),
    Bedrock(String),
    Parquet(String),
}

impl AppError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub fn aws(message: impl Into<String>) -> Self {
        Self::Aws(message.into())
    }

    pub fn nats(message: impl Into<String>) -> Self {
        Self::Nats(message.into())
    }

    pub fn bedrock(message: impl Into<String>) -> Self {
        Self::Bedrock(message.into())
    }

    pub fn parquet(message: impl Into<String>) -> Self {
        Self::Parquet(message.into())
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(f, "config error: {message}"),
            Self::Validation(message) => write!(f, "validation error: {message}"),
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::Json(error) => write!(f, "json error: {error}"),
            Self::Aws(message) => write!(f, "aws error: {message}"),
            Self::Nats(message) => write!(f, "nats error: {message}"),
            Self::Bedrock(message) => write!(f, "bedrock error: {message}"),
            Self::Parquet(message) => write!(f, "parquet error: {message}"),
        }
    }
}

impl Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
