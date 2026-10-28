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
    /// Where replies go: the `Reply-To` header address, else the `From`
    /// header address, else the envelope sender. Relays such as Brevo put a
    /// bounce address in the envelope, so `envelope_from` is not the person.
    pub reply_to: Option<String>,
    pub message_id: Option<String>,
    /// `In-Reply-To` header, used to thread this message into an existing
    /// conversation on ingest.
    pub in_reply_to: Option<String>,
    /// `References` header (space-separated Message-IDs), the fuller threading
    /// chain when present.
    pub references: Option<String>,
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

impl RawMessage {
    /// The person behind the message: `reply_to`, else the envelope sender.
    /// Used both to address replies and to group header-less follow-ups, since
    /// a relay's envelope sender (e.g. a per-message Brevo bounce address) is
    /// not the person.
    pub(crate) fn reply_address(&self) -> &str {
        self.reply_to.as_deref().unwrap_or(&self.envelope_from)
    }
}

/// Strip leading `Re:`/`Fwd:`/`Fw:` prefixes (any number, any case, optional
/// space before the colon) and trim. Returns `None` when nothing is left, so an
/// empty normalized subject never becomes a grouping key. Kept in step with the
/// migration's backfill regex so historical rows normalize the same way.
pub(crate) fn normalize_subject(subject: Option<&str>) -> Option<String> {
    let mut s = subject.unwrap_or("").trim();
    'outer: loop {
        for kw in ["re", "fwd", "fw"] {
            if let Some(head) = s.get(..kw.len()) {
                if head.eq_ignore_ascii_case(kw) {
                    let rest = s[kw.len()..].trim_start();
                    if let Some(after) = rest.strip_prefix(':') {
                        s = after.trim_start();
                        continue 'outer;
                    }
                }
            }
        }
        break;
    }
    let out = s.trim();
    (!out.is_empty()).then(|| out.to_string())
}

/// Strip surrounding angle brackets + whitespace from a Message-ID so ids
/// compare equal whether stored bracketed or bare.
pub(crate) fn normalize_message_id(id: &str) -> String {
    id.trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim()
        .to_string()
}

/// The set of Message-IDs an inbound message references (its `References` chain
/// plus `In-Reply-To`), normalized and de-duplicated, for conversation lookup.
pub(crate) fn referenced_ids(in_reply_to: Option<&str>, references: Option<&str>) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for raw in [references, in_reply_to].into_iter().flatten() {
        for tok in raw.split_whitespace() {
            let n = normalize_message_id(tok);
            if !n.is_empty() && !ids.contains(&n) {
                ids.push(n);
            }
        }
    }
    ids
}

/// Lightweight row for list views — omits the heavy `body_*`/`raw_mime`
/// fields. `address` is the full recipient address (`local_part@domain`).
#[derive(Debug, Clone, Serialize)]
pub struct MessageSummary {
    pub id: i64,
    pub address: String,
    pub message_id: Option<String>,
    pub envelope_from: String,
    /// See [`RawMessage::reply_to`]; `None` for messages stored before it existed.
    pub reply_to: Option<String>,
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
    /// The conversation this message belongs to (see [`ConversationSummary`]).
    pub conversation_id: i64,
    pub address: String,
    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Option<String>,
    pub envelope_from: String,
    pub envelope_to: String,
    /// See [`RawMessage::reply_to`]; `None` for messages stored before it existed.
    pub reply_to: Option<String>,
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

/// One conversation in the grouped inbox: its most recent activity, the latest
/// inbound participant, and unread/total counts — the row the UI renders in the
/// conversation list. A conversation groups an inbound message, our replies to
/// it, and any further inbound replies-to-replies under one `conversation_id`.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationSummary {
    pub conversation_id: i64,
    /// The inbox (`local_part@domain`) of the conversation's latest inbound message.
    pub address: String,
    /// Subject of the latest inbound message.
    pub subject: Option<String>,
    pub last_from_display: Option<String>,
    pub last_envelope_from: String,
    /// The latest inbound message's reply address (see [`RawMessage::reply_to`]).
    pub last_reply_to: Option<String>,
    /// Total items in the conversation (inbound messages + our replies).
    pub message_count: i64,
    /// Inbound messages not yet marked read.
    pub unread_count: i64,
    /// Most recent activity — the later of the last inbound receipt and the
    /// last reply we sent.
    pub last_activity_at: DateTime<Utc>,
    pub spf_result: Option<String>,
    pub dkim_result: Option<String>,
    /// Whether any reply has been sent in this conversation.
    pub replied: bool,
}

/// A conversation as an interleaved, chronological transcript: inbound messages
/// and our outbound replies ordered by time, for the thread view.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationThread {
    pub conversation_id: i64,
    /// The inbox the latest inbound message was addressed to (the reply identity).
    pub address: String,
    /// The conversation's subject (its earliest message's subject).
    pub subject: Option<String>,
    pub items: Vec<ThreadItem>,
    /// The latest inbound message id — the target the reply endpoint should use
    /// so a reply threads onto the most recent inbound message.
    pub latest_message_id: Option<i64>,
}

/// One entry in a [`ConversationThread`]: either an inbound message (full, with
/// its attachments) or one of our sent replies. Serializes internally tagged as
/// `{ "kind": "message", … }` / `{ "kind": "reply", … }`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ThreadItem {
    Message(Box<StoredMessage>),
    Reply(StoredReply),
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
    /// The `Message-ID` we stamped on the outbound reply (bare, no angle
    /// brackets), so a later inbound reply-to-this-reply threads back into the
    /// conversation. `None` if the reply carried no id.
    pub rfc_message_id: Option<String>,
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

    /// List conversations newest-activity-first, filtered by `filter` (matched
    /// against each conversation's latest inbound message). This is the grouped
    /// inbox view.
    async fn list_conversations(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<ConversationSummary>, MailError>;

    /// Fetch one conversation as an interleaved transcript of inbound messages
    /// and our replies, or `None` if the conversation is empty/unknown.
    async fn get_conversation(
        &self,
        conversation_id: i64,
    ) -> Result<Option<ConversationThread>, MailError>;

    /// Stamp `read_at = now()` on every unread inbound message in the
    /// conversation. Idempotent; unknown ids are a no-op.
    async fn mark_conversation_read(&self, conversation_id: i64) -> Result<(), MailError>;

    /// Permanently delete a whole conversation — every message in it with its
    /// attachments and replies. Irreversible; an unknown id is a no-op.
    async fn delete_conversation(&self, conversation_id: i64) -> Result<(), MailError>;

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
