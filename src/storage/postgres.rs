use super::{
    normalize_subject, referenced_ids, AddressInfo, Attachment, ConversationSummary,
    ConversationThread, MessageFilter, MessageSummary, NewReply, RawMessage, Storage,
    StoredMessage, StoredReply, ThreadItem,
};
use crate::errors::MailError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::collections::HashMap;

/// Postgres-backed [`Storage`] implementation, against the `mail_domains` /
/// `mail_addresses` / `mail_messages` / `mail_attachments` / `mail_replies`
/// schema. The consuming application owns the migrations that create it.
///
/// Enabled by the `postgres` feature; mirrors how `axum-email-otp-auth` ships
/// its `RedisStorage`.
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Split `local@domain` into lowercased `(local_part, domain)`.
fn split_address(address: &str) -> Result<(String, String), MailError> {
    let lower = address.to_lowercase();
    lower
        .rsplit_once('@')
        .map(|(local, domain)| (local.to_string(), domain.to_string()))
        .ok_or_else(|| MailError::StorageError(format!("invalid address: {address}")))
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn is_valid_recipient(&self, address: &str) -> Result<bool, MailError> {
        let (local_part, domain) = split_address(address)?;
        let exists = sqlx::query_scalar!(
            r#"SELECT EXISTS(
                SELECT 1 FROM mail_addresses a
                JOIN mail_domains d ON d.id = a.domain_id
                WHERE d.domain = $1 AND a.local_part = $2 AND a.active = true
             ) AS "exists!""#,
            domain,
            local_part
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(exists)
    }

    async fn save_message(&self, message: RawMessage) -> Result<u64, MailError> {
        let (local_part, domain) = split_address(&message.address)?;

        // One transaction: resolve the address, resolve the conversation the
        // message threads into, insert, and (for a brand-new conversation) seed
        // conversation_id from the row's own id — all-or-nothing.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;

        let address_id = sqlx::query_scalar!(
            "SELECT a.id FROM mail_addresses a
             JOIN mail_domains d ON d.id = a.domain_id
             WHERE d.domain = $1 AND a.local_part = $2",
            domain,
            local_part
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?
        .ok_or_else(|| {
            MailError::StorageError(format!("no address configured for {}", message.address))
        })?;

        let subject_normalized = normalize_subject(message.subject.as_deref());
        let refs = referenced_ids(
            message.in_reply_to.as_deref(),
            message.references.as_deref(),
        );

        // 1) Thread by referenced Message-ID: join the conversation of any known
        //    inbound message *or* one of our sent replies that this references.
        let mut conversation_id: Option<i64> = if refs.is_empty() {
            None
        } else {
            sqlx::query_scalar!(
                r#"
                SELECT conversation_id AS "conversation_id!" FROM (
                    SELECT conversation_id, received_at AS ts
                        FROM mail_messages WHERE message_id = ANY($1)
                    UNION ALL
                    SELECT m.conversation_id, r.sent_at AS ts
                        FROM mail_replies r JOIN mail_messages m ON m.id = r.message_id
                        WHERE r.rfc_message_id = ANY($1)
                ) hits
                WHERE conversation_id IS NOT NULL
                ORDER BY ts DESC
                LIMIT 1
                "#,
                &refs
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?
        };

        // 2) Header-less fallback: same inbox + same sender + same normalized
        //    subject — catches senders that drop In-Reply-To/References. The
        //    sender is the reply address, not the envelope sender (see
        //    `RawMessage::reply_address`); rows from before `reply_to` existed
        //    fall back to their envelope sender.
        if conversation_id.is_none() {
            if let Some(ref subj) = subject_normalized {
                conversation_id = sqlx::query_scalar!(
                    r#"SELECT conversation_id AS "conversation_id!"
                       FROM mail_messages
                       WHERE address_id = $1
                         AND COALESCE(reply_to, envelope_from) = $2
                         AND subject_normalized = $3
                         AND conversation_id IS NOT NULL
                       ORDER BY received_at DESC
                       LIMIT 1"#,
                    address_id,
                    message.reply_address(),
                    subj
                )
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| MailError::StorageError(e.to_string()))?;
            }
        }

        let id = sqlx::query_scalar!(
            "INSERT INTO mail_messages
                (address_id, message_id, in_reply_to, references_header, subject_normalized,
                 conversation_id, envelope_from, envelope_to, reply_to, from_display, subject,
                 body_text, body_html, raw_mime, spf_result, dkim_result)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
             RETURNING id",
            address_id,
            message.message_id,
            message.in_reply_to,
            message.references,
            subject_normalized,
            conversation_id,
            message.envelope_from,
            message.envelope_to,
            message.reply_to,
            message.from_display,
            message.subject,
            message.body_text,
            message.body_html,
            message.raw_data,
            message.spf_result,
            message.dkim_result
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        // A new conversation is its own root: point conversation_id at this id.
        if conversation_id.is_none() {
            sqlx::query!(
                "UPDATE mail_messages SET conversation_id = id WHERE id = $1",
                id
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;

        Ok(id as u64)
    }

    async fn health(&self) -> Result<(), MailError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn list_messages(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<MessageSummary>, MailError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                m.id,
                (a.local_part || '@' || d.domain) AS "address!",
                m.message_id,
                m.envelope_from,
                m.reply_to,
                m.from_display,
                m.subject,
                m.spf_result,
                m.dkim_result,
                m.received_at,
                m.read_at,
                m.replied_at
            FROM mail_messages m
            JOIN mail_addresses a ON a.id = m.address_id
            JOIN mail_domains d ON d.id = a.domain_id
            WHERE a.active = true
              AND ($1::text IS NULL OR d.domain = $1)
              AND ($2::text IS NULL OR (a.local_part || '@' || d.domain) = $2)
              AND ($3::bool = false OR m.read_at IS NULL)
            ORDER BY m.received_at DESC, m.id DESC
            LIMIT $4 OFFSET $5
            "#,
            filter.domain.as_deref(),
            filter.address.as_deref(),
            filter.unread_only,
            filter.limit,
            filter.offset,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| MessageSummary {
                id: r.id,
                address: r.address,
                message_id: r.message_id,
                envelope_from: r.envelope_from,
                reply_to: r.reply_to,
                from_display: r.from_display,
                subject: r.subject,
                spf_result: r.spf_result,
                dkim_result: r.dkim_result,
                received_at: r.received_at,
                read_at: r.read_at,
                replied_at: r.replied_at,
            })
            .collect())
    }

    async fn get_message(&self, id: i64) -> Result<Option<StoredMessage>, MailError> {
        let row = sqlx::query!(
            r#"
            SELECT
                m.id,
                COALESCE(m.conversation_id, m.id) AS "conversation_id!",
                (a.local_part || '@' || d.domain) AS "address!",
                m.message_id,
                m.in_reply_to,
                m.references_header,
                m.envelope_from,
                m.envelope_to,
                m.reply_to,
                m.from_display,
                m.subject,
                m.body_text,
                m.body_html,
                m.spf_result,
                m.dkim_result,
                m.received_at,
                m.read_at,
                m.replied_at
            FROM mail_messages m
            JOIN mail_addresses a ON a.id = m.address_id
            JOIN mail_domains d ON d.id = a.domain_id
            WHERE m.id = $1
            "#,
            id
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        let Some(row) = row else {
            return Ok(None);
        };

        let attachments = sqlx::query!(
            r#"SELECT id, filename, content_type, size_bytes, storage_path
               FROM mail_attachments WHERE message_id = $1 ORDER BY id"#,
            id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?
        .into_iter()
        .map(|a| Attachment {
            id: a.id,
            filename: a.filename,
            content_type: a.content_type,
            size_bytes: a.size_bytes,
            storage_path: a.storage_path,
        })
        .collect();

        let replies = sqlx::query!(
            r#"SELECT id, sent_by, body_text, body_html, sent_at, delivery_status
               FROM mail_replies WHERE message_id = $1 ORDER BY sent_at, id"#,
            id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?
        .into_iter()
        .map(|r| StoredReply {
            id: r.id,
            sent_by: r.sent_by,
            body_text: r.body_text,
            body_html: r.body_html,
            sent_at: r.sent_at,
            delivery_status: r.delivery_status,
        })
        .collect();

        Ok(Some(StoredMessage {
            id: row.id,
            conversation_id: row.conversation_id,
            address: row.address,
            message_id: row.message_id,
            in_reply_to: row.in_reply_to,
            references: row.references_header,
            envelope_from: row.envelope_from,
            envelope_to: row.envelope_to,
            reply_to: row.reply_to,
            from_display: row.from_display,
            subject: row.subject,
            body_text: row.body_text,
            body_html: row.body_html,
            spf_result: row.spf_result,
            dkim_result: row.dkim_result,
            received_at: row.received_at,
            read_at: row.read_at,
            replied_at: row.replied_at,
            attachments,
            replies,
        }))
    }

    async fn mark_read(&self, id: i64) -> Result<(), MailError> {
        sqlx::query!(
            "UPDATE mail_messages SET read_at = now() WHERE id = $1 AND read_at IS NULL",
            id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn save_reply(&self, reply: NewReply) -> Result<i64, MailError> {
        let id = sqlx::query_scalar!(
            "INSERT INTO mail_replies
                (message_id, rfc_message_id, sent_by, body_text, body_html, delivery_status)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id",
            reply.message_id,
            reply.rfc_message_id,
            reply.sent_by,
            reply.body_text,
            reply.body_html,
            reply.delivery_status,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(id)
    }

    async fn mark_replied(&self, id: i64) -> Result<(), MailError> {
        sqlx::query!(
            "UPDATE mail_messages SET replied_at = now() WHERE id = $1",
            id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn list_conversations(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<ConversationSummary>, MailError> {
        let rows = sqlx::query!(
            r#"
            WITH msgs AS (
                SELECT
                    m.id,
                    COALESCE(m.conversation_id, m.id) AS conversation_id,
                    (a.local_part || '@' || d.domain) AS address,
                    d.domain AS domain,
                    m.subject,
                    m.from_display,
                    m.envelope_from,
                    m.reply_to,
                    m.spf_result,
                    m.dkim_result,
                    m.received_at,
                    m.read_at
                FROM mail_messages m
                JOIN mail_addresses a ON a.id = m.address_id
                JOIN mail_domains d ON d.id = a.domain_id
                WHERE a.active = true
            ),
            latest AS (
                SELECT DISTINCT ON (conversation_id)
                    conversation_id, address, domain, subject, from_display,
                    envelope_from, reply_to, spf_result, dkim_result, received_at
                FROM msgs
                ORDER BY conversation_id, received_at DESC, id DESC
            ),
            agg AS (
                SELECT conversation_id,
                       COUNT(*) AS msg_count,
                       COUNT(*) FILTER (WHERE read_at IS NULL) AS unread_count,
                       MAX(received_at) AS last_received_at
                FROM msgs
                GROUP BY conversation_id
            ),
            rep AS (
                SELECT COALESCE(m.conversation_id, m.id) AS conversation_id,
                       COUNT(*) AS reply_count,
                       MAX(r.sent_at) AS last_reply_at
                FROM mail_replies r
                JOIN mail_messages m ON m.id = r.message_id
                GROUP BY COALESCE(m.conversation_id, m.id)
            )
            SELECT
                l.conversation_id AS "conversation_id!",
                l.address AS "address!",
                l.subject,
                l.from_display AS last_from_display,
                l.envelope_from AS "last_envelope_from!",
                l.reply_to AS last_reply_to,
                (a.msg_count + COALESCE(rp.reply_count, 0)) AS "message_count!",
                a.unread_count AS "unread_count!",
                GREATEST(a.last_received_at, COALESCE(rp.last_reply_at, a.last_received_at)) AS "last_activity_at!",
                l.spf_result,
                l.dkim_result,
                (COALESCE(rp.reply_count, 0) > 0) AS "replied!"
            FROM latest l
            JOIN agg a ON a.conversation_id = l.conversation_id
            LEFT JOIN rep rp ON rp.conversation_id = l.conversation_id
            WHERE ($1::text IS NULL OR l.domain = $1)
              AND ($2::text IS NULL OR l.address = $2)
              AND ($3::bool = false OR a.unread_count > 0)
            ORDER BY GREATEST(a.last_received_at, COALESCE(rp.last_reply_at, a.last_received_at)) DESC,
                     l.conversation_id DESC
            LIMIT $4 OFFSET $5
            "#,
            filter.domain.as_deref(),
            filter.address.as_deref(),
            filter.unread_only,
            filter.limit,
            filter.offset,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| ConversationSummary {
                conversation_id: r.conversation_id,
                address: r.address,
                subject: r.subject,
                last_from_display: r.last_from_display,
                last_envelope_from: r.last_envelope_from,
                last_reply_to: r.last_reply_to,
                message_count: r.message_count,
                unread_count: r.unread_count,
                last_activity_at: r.last_activity_at,
                spf_result: r.spf_result,
                dkim_result: r.dkim_result,
                replied: r.replied,
            })
            .collect())
    }

    async fn get_conversation(
        &self,
        conversation_id: i64,
    ) -> Result<Option<ConversationThread>, MailError> {
        // All inbound messages in the conversation, oldest first.
        let msg_rows = sqlx::query!(
            r#"
            SELECT
                m.id,
                COALESCE(m.conversation_id, m.id) AS "conversation_id!",
                (a.local_part || '@' || d.domain) AS "address!",
                m.message_id,
                m.in_reply_to,
                m.references_header,
                m.envelope_from,
                m.envelope_to,
                m.reply_to,
                m.from_display,
                m.subject,
                m.body_text,
                m.body_html,
                m.spf_result,
                m.dkim_result,
                m.received_at,
                m.read_at,
                m.replied_at
            FROM mail_messages m
            JOIN mail_addresses a ON a.id = m.address_id
            JOIN mail_domains d ON d.id = a.domain_id
            WHERE COALESCE(m.conversation_id, m.id) = $1
            ORDER BY m.received_at, m.id
            "#,
            conversation_id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        if msg_rows.is_empty() {
            return Ok(None);
        }

        let msg_ids: Vec<i64> = msg_rows.iter().map(|r| r.id).collect();

        // Attachments + replies for every message in the conversation, batched.
        let att_rows = sqlx::query!(
            r#"SELECT message_id, id, filename, content_type, size_bytes, storage_path
               FROM mail_attachments WHERE message_id = ANY($1) ORDER BY id"#,
            &msg_ids
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        let rep_rows = sqlx::query!(
            r#"SELECT id, sent_by, body_text, body_html, sent_at, delivery_status
               FROM mail_replies WHERE message_id = ANY($1) ORDER BY sent_at, id"#,
            &msg_ids
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        let mut atts: HashMap<i64, Vec<Attachment>> = HashMap::new();
        for a in att_rows {
            atts.entry(a.message_id).or_default().push(Attachment {
                id: a.id,
                filename: a.filename,
                content_type: a.content_type,
                size_bytes: a.size_bytes,
                storage_path: a.storage_path,
            });
        }

        let conv_id = msg_rows[0].conversation_id;
        let subject = msg_rows.first().and_then(|r| r.subject.clone());
        let address = msg_rows
            .last()
            .map(|r| r.address.clone())
            .unwrap_or_default();
        let latest_message_id = msg_rows.last().map(|r| r.id);

        // Interleave inbound messages (by received_at) and our replies (by
        // sent_at); a message sorts before a reply sharing its timestamp.
        let mut ordered: Vec<(DateTime<Utc>, u8, i64, ThreadItem)> = Vec::new();
        for r in msg_rows {
            let attachments = atts.remove(&r.id).unwrap_or_default();
            let msg = StoredMessage {
                id: r.id,
                conversation_id: r.conversation_id,
                address: r.address,
                message_id: r.message_id,
                in_reply_to: r.in_reply_to,
                references: r.references_header,
                envelope_from: r.envelope_from,
                envelope_to: r.envelope_to,
                reply_to: r.reply_to,
                from_display: r.from_display,
                subject: r.subject,
                body_text: r.body_text,
                body_html: r.body_html,
                spf_result: r.spf_result,
                dkim_result: r.dkim_result,
                received_at: r.received_at,
                read_at: r.read_at,
                replied_at: r.replied_at,
                attachments,
                // Replies live at the top level of the thread, not nested here.
                replies: Vec::new(),
            };
            ordered.push((r.received_at, 0, r.id, ThreadItem::Message(Box::new(msg))));
        }
        for r in rep_rows {
            let reply = StoredReply {
                id: r.id,
                sent_by: r.sent_by,
                body_text: r.body_text,
                body_html: r.body_html,
                sent_at: r.sent_at,
                delivery_status: r.delivery_status,
            };
            ordered.push((r.sent_at, 1, r.id, ThreadItem::Reply(reply)));
        }
        ordered.sort_by(|x, y| x.0.cmp(&y.0).then(x.1.cmp(&y.1)).then(x.2.cmp(&y.2)));

        Ok(Some(ConversationThread {
            conversation_id: conv_id,
            address,
            subject,
            items: ordered.into_iter().map(|(_, _, _, item)| item).collect(),
            latest_message_id,
        }))
    }

    async fn mark_conversation_read(&self, conversation_id: i64) -> Result<(), MailError> {
        sqlx::query!(
            "UPDATE mail_messages SET read_at = now()
             WHERE COALESCE(conversation_id, id) = $1 AND read_at IS NULL",
            conversation_id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_conversation(&self, conversation_id: i64) -> Result<(), MailError> {
        // No ON DELETE CASCADE: remove children first, all in one transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!(
            "DELETE FROM mail_replies WHERE message_id IN
                (SELECT id FROM mail_messages WHERE COALESCE(conversation_id, id) = $1)",
            conversation_id
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!(
            "DELETE FROM mail_attachments WHERE message_id IN
                (SELECT id FROM mail_messages WHERE COALESCE(conversation_id, id) = $1)",
            conversation_id
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!(
            "DELETE FROM mail_messages WHERE COALESCE(conversation_id, id) = $1",
            conversation_id
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        tx.commit()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn list_addresses(&self) -> Result<Vec<AddressInfo>, MailError> {
        let rows = sqlx::query!(
            r#"SELECT a.id, d.domain, a.local_part, a.label, a.active, a.created_at
               FROM mail_addresses a
               JOIN mail_domains d ON d.id = a.domain_id
               ORDER BY d.domain, a.local_part"#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| AddressInfo {
                id: r.id,
                address: format!("{}@{}", r.local_part, r.domain),
                domain: r.domain,
                local_part: r.local_part,
                label: r.label,
                active: r.active,
                created_at: r.created_at,
            })
            .collect())
    }

    async fn create_address(
        &self,
        local_part: &str,
        domain: &str,
        label: Option<String>,
    ) -> Result<AddressInfo, MailError> {
        let local_part = local_part.to_lowercase();
        let domain = domain.to_lowercase();

        let domain_id = sqlx::query_scalar!(
            "INSERT INTO mail_domains (domain) VALUES ($1)
             ON CONFLICT (domain) DO UPDATE SET domain = EXCLUDED.domain
             RETURNING id",
            domain
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        // Upsert so re-adding an existing (possibly deactivated) address
        // re-activates it rather than erroring on the unique constraint.
        let row = sqlx::query!(
            r#"INSERT INTO mail_addresses (domain_id, local_part, label, active)
               VALUES ($1, $2, $3, true)
               ON CONFLICT (domain_id, local_part)
               DO UPDATE SET active = true,
                             label = COALESCE(EXCLUDED.label, mail_addresses.label)
               RETURNING id, local_part, label, active, created_at"#,
            domain_id,
            local_part,
            label
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;

        Ok(AddressInfo {
            id: row.id,
            address: format!("{}@{}", row.local_part, domain),
            domain,
            local_part: row.local_part,
            label: row.label,
            active: row.active,
            created_at: row.created_at,
        })
    }

    async fn set_address_active(&self, id: i64, active: bool) -> Result<(), MailError> {
        sqlx::query!(
            "UPDATE mail_addresses SET active = $2 WHERE id = $1",
            id,
            active
        )
        .execute(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_thread(&self, id: i64) -> Result<(), MailError> {
        // No ON DELETE CASCADE in the schema, so remove children first, all in
        // one transaction so a partial failure rolls back.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!("DELETE FROM mail_replies WHERE message_id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!("DELETE FROM mail_attachments WHERE message_id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!("DELETE FROM mail_messages WHERE id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        tx.commit()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn delete_address(&self, id: i64) -> Result<(), MailError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        // Delete this address's messages' children, then the messages, then the
        // address itself. The domain row is left for reuse.
        sqlx::query!(
            "DELETE FROM mail_replies WHERE message_id IN
                (SELECT id FROM mail_messages WHERE address_id = $1)",
            id
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!(
            "DELETE FROM mail_attachments WHERE message_id IN
                (SELECT id FROM mail_messages WHERE address_id = $1)",
            id
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!("DELETE FROM mail_messages WHERE address_id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        sqlx::query!("DELETE FROM mail_addresses WHERE id = $1", id)
            .execute(&mut *tx)
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        tx.commit()
            .await
            .map_err(|e| MailError::StorageError(e.to_string()))?;
        Ok(())
    }
}
