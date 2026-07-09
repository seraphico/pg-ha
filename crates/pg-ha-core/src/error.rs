use thiserror::Error;

/// Core error types for pg-ha
#[derive(Debug, Error)]
pub enum Error {
    #[error("DCS error: {0}")]
    Dcs(String),

    #[error("PostgreSQL error: {0}")]
    Postgres(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Lock acquisition failed")]
    LockFailed,

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
