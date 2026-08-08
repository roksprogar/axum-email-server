use thiserror::Error;

#[derive(Error, Debug)]
pub enum MailError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Reply error: {0}")]
    ReplyError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("I/O error: {0}")]
    Io(String),
}
