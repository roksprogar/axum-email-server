#![doc = include_str!("../README.md")]

pub mod config;
pub mod errors;
pub mod filter;
pub mod receive;
pub mod reply;
pub mod sender;
pub mod service;
pub mod storage;

#[cfg(feature = "axum")]
pub mod axum_api;

pub use config::{EmailProvider, MailConfig, StorageType};
pub use errors::MailError;
pub use sender::{BrevoMailSender, LettreMailSender, MailSender, OutgoingReply};
pub use service::MailService;
pub use storage::{
    AddressInfo, Attachment, ConversationSummary, ConversationThread, InMemoryStorage,
    MessageFilter, MessageSummary, NewReply, RawMessage, Storage, StoredMessage, StoredReply,
    ThreadItem,
};

#[cfg(feature = "postgres")]
pub use storage::PostgresStorage;
