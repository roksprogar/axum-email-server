use super::{
    AddressInfo, Attachment, MessageFilter, MessageSummary, NewReply, RawMessage, Storage,
    StoredMessage, StoredReply,
};
use crate::errors::MailError;
use async_trait::async_trait;
use sqlx::PgPool;

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

        let address_id = sqlx::query_scalar!(
            "SELECT a.id FROM mail_addresses a
             JOIN mail_domains d ON d.id = a.domain_id
             WHERE d.domain = $1 AND a.local_part = $2",
            domain,
            local_part
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MailError::StorageError(e.to_string()))?
        .ok_or_else(|| {
            MailError::StorageError(format!("no address configured for {}", message.address))
        })?;

        let id = sqlx::query_scalar!(
            "INSERT INTO mail_messages
                (address_id, message_id, envelope_from, envelope_to, from_display, subject, body_text, body_html, raw_mime, spf_result, dkim_result)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             RETURNING id",
            address_id,
            message.message_id,
            message.envelope_from,
            message.envelope_to,
            message.from_display,
            message.subject,
            message.body_text,
            message.body_html,
            message.raw_data,
            message.spf_result,
            message.dkim_result
        )
        .fetch_one(&self.pool)
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
                (a.local_part || '@' || d.domain) AS "address!",
                m.message_id,
                m.envelope_from,
                m.envelope_to,
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
            address: row.address,
            message_id: row.message_id,
            envelope_from: row.envelope_from,
            envelope_to: row.envelope_to,
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
            "INSERT INTO mail_replies (message_id, sent_by, body_text, body_html, delivery_status)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING id",
            reply.message_id,
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
