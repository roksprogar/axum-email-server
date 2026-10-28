use super::{
    normalize_message_id, normalize_subject, referenced_ids, AddressInfo, Attachment,
    ConversationSummary, ConversationThread, MessageFilter, MessageSummary, NewReply, RawMessage,
    Storage, StoredMessage, StoredReply, ThreadItem,
};
use crate::errors::MailError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

/// One stored message plus the mutable read/reply state the read side needs.
struct Record {
    id: i64,
    /// The conversation this message groups into (the id of the conversation's
    /// first message). Resolved at [`InMemoryStorage::save_message`] time.
    conversation_id: i64,
    msg: RawMessage,
    received_at: DateTime<Utc>,
    read_at: Option<DateTime<Utc>>,
    replied_at: Option<DateTime<Utc>>,
    replies: Vec<StoredReply>,
    /// The RFC Message-ID of each reply, parallel to `replies`, so a later
    /// inbound reply-to-a-reply can thread back to this conversation. Mirrors
    /// `mail_replies.rfc_message_id`; kept out of the public `StoredReply`.
    reply_rfc_ids: Vec<Option<String>>,
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
        reply_to: record.msg.reply_to.clone(),
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
        let refs = referenced_ids(
            message.in_reply_to.as_deref(),
            message.references.as_deref(),
        );
        let subj_norm = normalize_subject(message.subject.as_deref());

        let mut records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;

        // 1) Thread by referenced Message-ID (a stored inbound message or one of
        //    our sent replies); the most recent match wins.
        let mut best: Option<(DateTime<Utc>, i64)> = None;
        if !refs.is_empty() {
            for r in records.iter() {
                let mut hit: Option<DateTime<Utc>> = None;
                if let Some(mid) = r.msg.message_id.as_deref() {
                    if refs.contains(&normalize_message_id(mid)) {
                        hit = Some(r.received_at);
                    }
                }
                for (i, rid) in r.reply_rfc_ids.iter().enumerate() {
                    if let Some(rid) = rid {
                        if refs.contains(&normalize_message_id(rid)) {
                            let ts = r.replies[i].sent_at;
                            hit = Some(hit.map_or(ts, |h| h.max(ts)));
                        }
                    }
                }
                if let Some(ts) = hit {
                    if best.is_none_or(|(bts, _)| ts > bts) {
                        best = Some((ts, r.conversation_id));
                    }
                }
            }
        }

        // 2) Header-less fallback: same inbox + same sender (reply address, see
        //    `RawMessage::reply_address`) + same normalized subject.
        if best.is_none() {
            if let Some(ref subj) = subj_norm {
                for r in records.iter() {
                    if r.msg.address.eq_ignore_ascii_case(&message.address)
                        && r.msg.reply_address() == message.reply_address()
                        && normalize_subject(r.msg.subject.as_deref()).as_deref()
                            == Some(subj.as_str())
                        && best.is_none_or(|(bts, _)| r.received_at > bts)
                    {
                        best = Some((r.received_at, r.conversation_id));
                    }
                }
            }
        }

        let conversation_id = best.map(|(_, c)| c).unwrap_or(id);

        records.push(Record {
            id,
            conversation_id,
            msg: message,
            received_at: Utc::now(),
            read_at: None,
            replied_at: None,
            replies: Vec::new(),
            reply_rfc_ids: Vec::new(),
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
            conversation_id: r.conversation_id,
            address: r.msg.address.clone(),
            message_id: r.msg.message_id.clone(),
            in_reply_to: r.msg.in_reply_to.clone(),
            references: r.msg.references.clone(),
            envelope_from: r.msg.envelope_from.clone(),
            envelope_to: r.msg.envelope_to.clone(),
            reply_to: r.msg.reply_to.clone(),
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
        record.reply_rfc_ids.push(reply.rfc_message_id);
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

    async fn list_conversations(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<ConversationSummary>, MailError> {
        // Only conversations with a message to a currently-active address (a
        // deactivated address drops out of the inbox, matching Postgres).
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

        let mut groups: HashMap<i64, Vec<&Record>> = HashMap::new();
        for r in records.iter() {
            if active.contains(&r.msg.address.to_lowercase()) {
                groups.entry(r.conversation_id).or_default().push(r);
            }
        }

        let mut summaries: Vec<ConversationSummary> = Vec::new();
        for (conv_id, msgs) in groups {
            let latest = msgs
                .iter()
                .max_by(|a, b| a.received_at.cmp(&b.received_at).then(a.id.cmp(&b.id)))
                .unwrap();

            if let Some(ref d) = domain {
                if domain_of(&latest.msg.address).as_deref() != Some(d.as_str()) {
                    continue;
                }
            }
            if let Some(ref a) = address {
                if latest.msg.address.to_lowercase() != *a {
                    continue;
                }
            }

            let unread_count = msgs.iter().filter(|r| r.read_at.is_none()).count() as i64;
            if filter.unread_only && unread_count == 0 {
                continue;
            }

            let reply_count: i64 = msgs.iter().map(|r| r.replies.len() as i64).sum();
            let last_received = msgs.iter().map(|r| r.received_at).max().unwrap();
            let last_reply = msgs
                .iter()
                .flat_map(|r| r.replies.iter().map(|x| x.sent_at))
                .max();
            let last_activity_at = last_reply.map_or(last_received, |lr| lr.max(last_received));

            summaries.push(ConversationSummary {
                conversation_id: conv_id,
                address: latest.msg.address.clone(),
                subject: latest.msg.subject.clone(),
                last_from_display: latest.msg.from_display.clone(),
                last_envelope_from: latest.msg.envelope_from.clone(),
                last_reply_to: latest.msg.reply_to.clone(),
                message_count: msgs.len() as i64 + reply_count,
                unread_count,
                last_activity_at,
                spf_result: latest.msg.spf_result.clone(),
                dkim_result: latest.msg.dkim_result.clone(),
                replied: reply_count > 0,
            });
        }

        summaries.sort_by(|a, b| {
            b.last_activity_at
                .cmp(&a.last_activity_at)
                .then(b.conversation_id.cmp(&a.conversation_id))
        });

        let offset = filter.offset.max(0) as usize;
        let iter = summaries.into_iter().skip(offset);
        Ok(match filter.limit {
            Some(limit) if limit >= 0 => iter.take(limit as usize).collect(),
            _ => iter.collect(),
        })
    }

    async fn get_conversation(
        &self,
        conversation_id: i64,
    ) -> Result<Option<ConversationThread>, MailError> {
        let records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;

        let mut msgs: Vec<&Record> = records
            .iter()
            .filter(|r| r.conversation_id == conversation_id)
            .collect();
        if msgs.is_empty() {
            return Ok(None);
        }
        msgs.sort_by(|a, b| a.received_at.cmp(&b.received_at).then(a.id.cmp(&b.id)));

        let subject = msgs.first().and_then(|r| r.msg.subject.clone());
        let address = msgs
            .last()
            .map(|r| r.msg.address.clone())
            .unwrap_or_default();
        let latest_message_id = msgs.last().map(|r| r.id);

        let mut ordered: Vec<(DateTime<Utc>, u8, i64, ThreadItem)> = Vec::new();
        for r in &msgs {
            let msg = StoredMessage {
                id: r.id,
                conversation_id: r.conversation_id,
                address: r.msg.address.clone(),
                message_id: r.msg.message_id.clone(),
                in_reply_to: r.msg.in_reply_to.clone(),
                references: r.msg.references.clone(),
                envelope_from: r.msg.envelope_from.clone(),
                envelope_to: r.msg.envelope_to.clone(),
                reply_to: r.msg.reply_to.clone(),
                from_display: r.msg.from_display.clone(),
                subject: r.msg.subject.clone(),
                body_text: r.msg.body_text.clone(),
                body_html: r.msg.body_html.clone(),
                spf_result: r.msg.spf_result.clone(),
                dkim_result: r.msg.dkim_result.clone(),
                received_at: r.received_at,
                read_at: r.read_at,
                replied_at: r.replied_at,
                attachments: Vec::<Attachment>::new(),
                replies: Vec::new(),
            };
            ordered.push((r.received_at, 0, r.id, ThreadItem::Message(Box::new(msg))));
        }
        for r in &msgs {
            for reply in &r.replies {
                ordered.push((reply.sent_at, 1, reply.id, ThreadItem::Reply(reply.clone())));
            }
        }
        ordered.sort_by(|x, y| x.0.cmp(&y.0).then(x.1.cmp(&y.1)).then(x.2.cmp(&y.2)));

        Ok(Some(ConversationThread {
            conversation_id,
            address,
            subject,
            items: ordered.into_iter().map(|(_, _, _, item)| item).collect(),
            latest_message_id,
        }))
    }

    async fn mark_conversation_read(&self, conversation_id: i64) -> Result<(), MailError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?;
        for r in records
            .iter_mut()
            .filter(|r| r.conversation_id == conversation_id)
        {
            if r.read_at.is_none() {
                r.read_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    async fn delete_conversation(&self, conversation_id: i64) -> Result<(), MailError> {
        self.records
            .lock()
            .map_err(|_| MailError::StorageError("poisoned lock".to_string()))?
            .retain(|r| r.conversation_id != conversation_id);
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
