use crate::errors::MailError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

pub mod memory;

#[cfg(feature = "postgres")]
pub mod postgres;

/// A received message, parsed enough to persist against the `mail_messages`
/// schema. `raw_data` always holds the full original MIME source; the rest
/// are best-effort extractions from it (`None` where a header/part is
/// absent) via `mail-parser`.
#[derive(Debug, Clone)]
pub struct RawMessage {
    pub address: String,
    pub envelope_from: String,
    pub envelope_to: String,
    pub message_id: Option<String>,
    pub from_display: Option<String>,
    pub subject: Option<String>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub raw_data: Vec<u8>,
    /// "pass"/"fail"/"softfail"/"neutral"/"temperror"/"permerror"/"none".
    pub spf_result: Option<String>,
    /// "pass"/"fail"/"neutral"/"temperror"/"permerror"/"none".
    pub dkim_result: Option<String>,
}

/// Lightweight row for list views — omits the heavy `body_*`/`raw_mime`
/// fields. `address` is the full recipient address (`local_part@domain`).
#[derive(Debug, Clone, Serialize)]
pub struct MessageSummary {
    pub id: i64,
    pub address: String,
    pub message_id: Option<String>,
    pub envelope_from: String,
    pub from_display: Option<String>,
    pub subject: Option<String>,
    pub spf_result: Option<String>,
    pub dkim_result: Option<String>,
    pub received_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
    pub replied_at: Option<DateTime<Utc>>,
}

/// A full stored message: bodies plus its attachments and any replies. The
/// raw MIME source is deliberately not included here — fetch it separately
/// if a caller ever needs it.
#[derive(Debug, Clone, Serialize)]
pub struct StoredMessage {
    pub id: i64,
    pub address: String,
    pub message_id: Option<String>,
    pub envelope_from: String,
    pub envelope_to: String,
    pub from_display: Option<String>,
    pub subject: Option<String>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub spf_result: Option<String>,
    pub dkim_result: Option<String>,
    pub received_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
    pub replied_at: Option<DateTime<Utc>>,
    pub attachments: Vec<Attachment>,
    pub replies: Vec<StoredReply>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Attachment {
    pub id: i64,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub size_bytes: Option<i32>,
    pub storage_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoredReply {
    pub id: i64,
    pub sent_by: Option<String>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    pub sent_at: DateTime<Utc>,
    /// "sent"/"failed".
    pub delivery_status: Option<String>,
}

/// Filter for [`Storage::list_messages`]. All fields optional; `limit: None`
/// means no limit.
#[derive(Debug, Clone, Default)]
pub struct MessageFilter {
    /// Restrict to messages whose recipient domain matches exactly.
    pub domain: Option<String>,
    /// Restrict to a single full recipient address (`local_part@domain`).
    pub address: Option<String>,
    /// Only messages that have not been marked read.
    pub unread_only: bool,
    pub limit: Option<i64>,
    pub offset: i64,
}

/// A configured inbox: one `mail_addresses` row joined to its domain. `address`
/// is the convenience `local_part@domain` the SMTP `RCPT TO` check matches.
/// Only `active` rows are accepted; inactive ones are retained (messages FK to
/// them) but rejected at `RCPT TO`.
#[derive(Debug, Clone, Serialize)]
pub struct AddressInfo {
    pub id: i64,
    pub domain: String,
    pub local_part: String,
    pub address: String,
    pub label: Option<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

/// A reply to persist after (attempted) delivery. Sending the mail itself is
/// a service-level concern (`lettre`); this only records the outcome.
#[derive(Debug, Clone)]
pub struct NewReply {
    /// FK to the `mail_messages.id` being replied to.
    pub message_id: i64,
    pub sent_by: Option<String>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
    /// "sent"/"failed".
    pub delivery_status: String,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Storage: Send + Sync {
    /// Whether `address` (e.g. "contact@leanhigh.com") is a configured,
    /// active address. Checked at `RCPT TO` — anything not recognized here is
    /// rejected before `DATA`.
    async fn is_valid_recipient(&self, address: &str) -> Result<bool, MailError>;

    async fn save_message(&self, message: RawMessage) -> Result<u64, MailError>;
    async fn health(&self) -> Result<(), MailError>;

    /// List messages newest-first, filtered by `filter`.
    async fn list_messages(&self, filter: &MessageFilter)
        -> Result<Vec<MessageSummary>, MailError>;

    /// Fetch one message with its attachments and replies, or `None` if no
    /// message has that id.
    async fn get_message(&self, id: i64) -> Result<Option<StoredMessage>, MailError>;

    /// Stamp `read_at = now()` if it is currently unread. Idempotent; a
    /// missing or already-read message is a no-op.
    async fn mark_read(&self, id: i64) -> Result<(), MailError>;

    /// Persist a reply against its message, returning the new reply's id.
    async fn save_reply(&self, reply: NewReply) -> Result<i64, MailError>;

    /// Stamp `replied_at = now()` on the parent message.
    async fn mark_replied(&self, id: i64) -> Result<(), MailError>;

    /// Every configured address across all domains, active and inactive, so the
    /// admin UI can list and toggle them. Ordered by domain then local part.
    async fn list_addresses(&self) -> Result<Vec<AddressInfo>, MailError>;

    /// Create (or re-activate, if it already exists) `local_part@domain`,
    /// creating the domain row on demand. Returns the resulting address. Both
    /// parts are matched case-insensitively; a duplicate is upserted to
    /// `active = true` rather than erroring, so "add" also un-removes.
    async fn create_address(
        &self,
        local_part: &str,
        domain: &str,
        label: Option<String>,
    ) -> Result<AddressInfo, MailError>;

    /// Toggle an address's `active` flag by id. Deactivating keeps the row
    /// (and its messages) but `RCPT TO` starts rejecting it, and it drops out
    /// of the inbox. A missing id is a no-op.
    async fn set_address_active(&self, id: i64, active: bool) -> Result<(), MailError>;

    /// Permanently delete one thread — the message together with its
    /// attachments and replies. Unlike deactivating an address (which only
    /// hides), this is irreversible. A missing id is a no-op.
    async fn delete_thread(&self, id: i64) -> Result<(), MailError>;

    /// Permanently delete an address and every message addressed to it (each
    /// with its attachments and replies). The domain row is left in place for
    /// reuse. Irreversible; a missing id is a no-op.
    async fn delete_address(&self, id: i64) -> Result<(), MailError>;
}

pub use self::memory::InMemoryStorage;

#[cfg(feature = "postgres")]
pub use self::postgres::PostgresStorage;
