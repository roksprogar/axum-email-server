use super::{
    AddressInfo, Attachment, MessageFilter, MessageSummary, NewReply, RawMessage, Storage,
    StoredMessage, StoredReply,
};
use crate::errors::MailError;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

/// One stored message plus the mutable read/reply state the read side needs.
struct Record {
    id: i64,
    msg: RawMessage,
    received_at: chrono::DateTime<Utc>,
    read_at: Option<chrono::DateTime<Utc>>,
    replied_at: Option<chrono::DateTime<Utc>>,
    replies: Vec<StoredReply>,
}

/// One configured address. Mirrors a `mail_addresses` row (+ its domain) so the
/// in-memory store backs the same address-management surface as Postgres.
struct AddressRecord {
    id: i64,
    domain: String,
    local_part: String,
    label: Option<String>,
    active: bool,
    created_at: chrono::DateTime<Utc>,
}

/// In-memory `Storage` implementation. Useful for local development and
/// tests; not persistent.
#[derive(Default)]
pub struct InMemoryStorage {
    records: Mutex<Vec<Record>>,
    next_msg_id: AtomicI64,
    next_reply_id: AtomicI64,
    addresses: Mutex<Vec<AddressRecord>>,
    next_address_id: AtomicI64,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure the set of accepted recipient addresses. Lowercased on
    /// insert; lookups are case-insensitive to match SMTP's
    /// case-insensitive-domain / case-sensitive-local-part convention
    /// loosely (we keep it simple and lowercase both sides for now).
    pub fn with_addresses<I, S>(self, addresses: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_addresses(addresses);
        self
    }

    pub fn add_addresses<I, S>(&self, addresses: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut records = self.addresses.lock().unwrap();
        for address in addresses {
            let addr = address.into();
            let Some((local_part, domain)) = split_addr(&addr) else {
                continue;
            };
            match records
                .iter_mut()
                .find(|a| a.local_part == local_part && a.domain == domain)
            {
                Some(existing) => existing.active = true,
                None => records.push(AddressRecord {
                    id: self.next_address_id.fetch_add(1, Ordering::SeqCst),
                    domain,
                    local_part,
                    label: None,
                    active: true,
                    created_at: Utc::now(),
                }),
            }
        }
    }
}

/// Domain portion of a full address, lowercased.
fn domain_of(address: &str) -> Option<String> {
    address.rsplit_once('@').map(|(_, d)| d.to_lowercase())
}

/// Split `local@domain` into lowercased `(local_part, domain)`, or `None` if
/// there's no `@`.
fn split_addr(address: &str) -> Option<(String, String)> {
    address
        .rsplit_once('@')
        .map(|(local, domain)| (local.to_lowercase(), domain.to_lowercase()))
}

fn address_info_of(record: &AddressRecord) -> AddressInfo {
    AddressInfo {
        id: record.id,
        domain: record.domain.clone(),
        local_part: record.local_part.clone(),
        address: format!("{}@{}", record.local_part, record.domain),
        label: record.label.clone(),
        active: record.active,
        created_at: record.created_at,
    }
}

fn summary_of(record: &Record) -> MessageSummary {
    MessageSummary {
        id: record.id,
        address: record.msg.address.clone(),
        message_id: record.msg.message_id.clone(),
        envelope_from: record.msg.envelope_from.clone(),
        from_display: record.msg.from_display.clone(),
        subject: record.msg.subject.clone(),
        spf_result: record.msg.spf_result.clone(),
        dkim_result: record.msg.dkim_result.clone(),
        received_at: record.received_at,
        read_at: record.read_at,
        replied_at: record.replied_at,
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn is_valid_recipient(&self, address: &str) -> Result<bool, MailError> {
        let Some((local_part, domain)) = split_addr(address) else {
            return Ok(false);
        };
        Ok(self
            .addresses
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?
            .iter()
            .any(|a| a.active && a.local_part == local_part && a.domain == domain))
    }

    async fn save_message(&self, message: RawMessage) -> Result<u64, MailError> {
        let id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
        self.records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?
            .push(Record {
                id,
                msg: message,
                received_at: Utc::now(),
                read_at: None,
                replied_at: None,
                replies: Vec::new(),
            });
        Ok(id as u64)
    }

    async fn health(&self) -> Result<(), MailError> {
        Ok(())
    }

    async fn list_messages(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<MessageSummary>, MailError> {
        // Only messages addressed to a currently-active address are listed; a
        // deactivated address drops out of the inbox (matching Postgres).
        let active: HashSet<String> = {
            let addresses = self
                .addresses
                .lock()
                .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
            addresses
                .iter()
                .filter(|a| a.active)
                .map(|a| format!("{}@{}", a.local_part, a.domain))
                .collect()
        };

        let records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;

        let domain = filter.domain.as_ref().map(|d| d.to_lowercase());
        let address = filter.address.as_ref().map(|a| a.to_lowercase());

        let mut matched: Vec<&Record> = records
            .iter()
            .filter(|r| {
                if !active.contains(&r.msg.address.to_lowercase()) {
                    return false;
                }
                if let Some(ref d) = domain {
                    if domain_of(&r.msg.address).as_ref() != Some(d) {
                        return false;
                    }
                }
                if let Some(ref a) = address {
                    if &r.msg.address.to_lowercase() != a {
                        return false;
                    }
                }
                if filter.unread_only && r.read_at.is_some() {
                    return false;
                }
                true
            })
            .collect();

        // Newest first, tie-broken by id so ordering is deterministic.
        matched.sort_by(|a, b| {
            b.received_at
                .cmp(&a.received_at)
                .then_with(|| b.id.cmp(&a.id))
        });

        let offset = filter.offset.max(0) as usize;
        let iter = matched.into_iter().skip(offset);
        let summaries: Vec<MessageSummary> = match filter.limit {
            Some(limit) if limit >= 0 => iter.take(limit as usize).map(summary_of).collect(),
            _ => iter.map(summary_of).collect(),
        };
        Ok(summaries)
    }

    async fn get_message(&self, id: i64) -> Result<Option<StoredMessage>, MailError> {
        let records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
        Ok(records.iter().find(|r| r.id == id).map(|r| StoredMessage {
            id: r.id,
            address: r.msg.address.clone(),
            message_id: r.msg.message_id.clone(),
            envelope_from: r.msg.envelope_from.clone(),
            envelope_to: r.msg.envelope_to.clone(),
            from_display: r.msg.from_display.clone(),
            subject: r.msg.subject.clone(),
            body_text: r.msg.body_text.clone(),
            body_html: r.msg.body_html.clone(),
            spf_result: r.msg.spf_result.clone(),
            dkim_result: r.msg.dkim_result.clone(),
            received_at: r.received_at,
            read_at: r.read_at,
            replied_at: r.replied_at,
            // In-memory storage doesn't retain parsed attachments.
            attachments: Vec::<Attachment>::new(),
            replies: r.replies.clone(),
        }))
    }

    async fn mark_read(&self, id: i64) -> Result<(), MailError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
        if let Some(r) = records.iter_mut().find(|r| r.id == id) {
            if r.read_at.is_none() {
                r.read_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    async fn save_reply(&self, reply: NewReply) -> Result<i64, MailError> {
        let reply_id = self.next_reply_id.fetch_add(1, Ordering::SeqCst);
        let mut records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
        let record = records
            .iter_mut()
            .find(|r| r.id == reply.message_id)
            .ok_or_else(|| {
                MailError::StorageError(format!("no message with id {}", reply.message_id))
            })?;
        record.replies.push(StoredReply {
            id: reply_id,
            sent_by: reply.sent_by,
            body_text: reply.body_text,
            body_html: reply.body_html,
            sent_at: Utc::now(),
            delivery_status: Some(reply.delivery_status),
        });
        Ok(reply_id)
    }

    async fn mark_replied(&self, id: i64) -> Result<(), MailError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
        if let Some(r) = records.iter_mut().find(|r| r.id == id) {
            r.replied_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn list_addresses(&self) -> Result<Vec<AddressInfo>, MailError> {
        let addresses = self
            .addresses
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
        let mut out: Vec<AddressInfo> = addresses.iter().map(address_info_of).collect();
        out.sort_by(|a, b| {
            a.domain
                .cmp(&b.domain)
                .then_with(|| a.local_part.cmp(&b.local_part))
        });
        Ok(out)
    }

    async fn create_address(
        &self,
        local_part: &str,
        domain: &str,
        label: Option<String>,
    ) -> Result<AddressInfo, MailError> {
        let local_part = local_part.to_lowercase();
        let domain = domain.to_lowercase();
        let mut addresses = self
            .addresses
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;

        if let Some(existing) = addresses
            .iter_mut()
            .find(|a| a.local_part == local_part && a.domain == domain)
        {
            existing.active = true;
            if label.is_some() {
                existing.label = label;
            }
            return Ok(address_info_of(existing));
        }

        let record = AddressRecord {
            id: self.next_address_id.fetch_add(1, Ordering::SeqCst),
            domain,
            local_part,
            label,
            active: true,
            created_at: Utc::now(),
        };
        let info = address_info_of(&record);
        addresses.push(record);
        Ok(info)
    }

    async fn set_address_active(&self, id: i64, active: bool) -> Result<(), MailError> {
        let mut addresses = self
            .addresses
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
        if let Some(a) = addresses.iter_mut().find(|a| a.id == id) {
            a.active = active;
        }
        Ok(())
    }

    async fn delete_thread(&self, id: i64) -> Result<(), MailError> {
        // A record owns its replies/attachments, so dropping it removes them.
        self.records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?
            .retain(|r| r.id != id);
        Ok(())
    }

    async fn delete_address(&self, id: i64) -> Result<(), MailError> {
        let address = {
            let mut addresses = self
                .addresses
                .lock()
                .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
            let address = addresses
                .iter()
                .find(|a| a.id == id)
                .map(|a| format!("{}@{}", a.local_part, a.domain));
            addresses.retain(|a| a.id != id);
            address
        };
        if let Some(address) = address {
            self.records
                .lock()
                .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?
                .retain(|r| r.msg.address.to_lowercase() != address);
        }
        Ok(())
    }
}
