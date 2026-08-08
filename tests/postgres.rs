//! Integration test for `PostgresStorage` against a real database.
//!
//! Runs only when `DATABASE_URL` is set (the CI job provides a Postgres
//! service); otherwise it skips, so `cargo test` on a machine without Postgres
//! stays green. It creates the schema itself, since the crate ships the queries
//! but the consumer owns the migration.
#![cfg(feature = "postgres")]

use axum_email_server::{MessageFilter, PostgresStorage, RawMessage, Storage};
use sqlx::postgres::PgPoolOptions;

const SCHEMA: &str = r#"
DROP TABLE IF EXISTS mail_replies CASCADE;
DROP TABLE IF EXISTS mail_attachments CASCADE;
DROP TABLE IF EXISTS mail_messages CASCADE;
DROP TABLE IF EXISTS mail_addresses CASCADE;
DROP TABLE IF EXISTS mail_domains CASCADE;

CREATE TABLE mail_domains (
    id          BIGSERIAL PRIMARY KEY,
    domain      TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE mail_addresses (
    id          BIGSERIAL PRIMARY KEY,
    domain_id   BIGINT NOT NULL REFERENCES mail_domains(id),
    local_part  TEXT NOT NULL,
    label       TEXT,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (domain_id, local_part)
);
CREATE TABLE mail_messages (
    id             BIGSERIAL PRIMARY KEY,
    address_id     BIGINT NOT NULL REFERENCES mail_addresses(id),
    message_id     TEXT,
    envelope_from  TEXT NOT NULL,
    envelope_to    TEXT NOT NULL,
    from_display   TEXT,
    subject        TEXT,
    body_text      TEXT,
    body_html      TEXT,
    raw_mime       BYTEA NOT NULL,
    spf_result     TEXT,
    dkim_result    TEXT,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at        TIMESTAMPTZ,
    replied_at     TIMESTAMPTZ
);
CREATE TABLE mail_attachments (
    id            BIGSERIAL PRIMARY KEY,
    message_id    BIGINT NOT NULL REFERENCES mail_messages(id),
    filename      TEXT,
    content_type  TEXT,
    size_bytes    INTEGER,
    storage_path  TEXT
);
CREATE TABLE mail_replies (
    id               BIGSERIAL PRIMARY KEY,
    message_id       BIGINT NOT NULL REFERENCES mail_messages(id),
    sent_by          TEXT,
    body_text        TEXT,
    body_html        TEXT,
    sent_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery_status  TEXT
);
"#;

fn sample_message() -> RawMessage {
    RawMessage {
        address: "contact@leanhigh.com".to_string(),
        envelope_from: "someone@example.com".to_string(),
        envelope_to: "contact@leanhigh.com".to_string(),
        message_id: Some("mid-1@example.com".to_string()),
        from_display: Some("Someone".to_string()),
        subject: Some("Hi".to_string()),
        body_text: Some("hello".to_string()),
        body_html: None,
        raw_data: b"raw mime".to_vec(),
        spf_result: Some("pass".to_string()),
        dkim_result: Some("none".to_string()),
    }
}

#[tokio::test]
async fn postgres_storage_roundtrip() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set — skipping Postgres integration test");
        return;
    };

    let pool = PgPoolOptions::new()
        .connect(&url)
        .await
        .expect("connect to Postgres");

    // Fresh schema each run.
    for stmt in SCHEMA.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt)
            .execute(&pool)
            .await
            .expect("apply schema statement");
    }

    let storage = PostgresStorage::new(pool);

    // Unknown recipient is rejected until it's configured.
    assert!(!storage
        .is_valid_recipient("contact@leanhigh.com")
        .await
        .unwrap());

    // Add the address → now a valid recipient.
    let addr = storage
        .create_address("contact", "leanhigh.com", Some("Contact".to_string()))
        .await
        .unwrap();
    assert_eq!(addr.address, "contact@leanhigh.com");
    assert!(storage
        .is_valid_recipient("contact@leanhigh.com")
        .await
        .unwrap());

    // Store a message for it.
    let id = storage.save_message(sample_message()).await.unwrap() as i64;
    assert!(id > 0);

    // List + fetch.
    let list = storage
        .list_messages(&MessageFilter {
            limit: Some(10),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].subject.as_deref(), Some("Hi"));

    let msg = storage.get_message(id).await.unwrap().expect("message");
    assert_eq!(msg.envelope_from, "someone@example.com");
    assert!(msg.read_at.is_none());

    // mark_read stamps read_at.
    storage.mark_read(id).await.unwrap();
    assert!(storage
        .get_message(id)
        .await
        .unwrap()
        .unwrap()
        .read_at
        .is_some());

    // Deactivating the address rejects RCPT and hides it from the inbox.
    storage.set_address_active(addr.id, false).await.unwrap();
    assert!(!storage
        .is_valid_recipient("contact@leanhigh.com")
        .await
        .unwrap());
    assert!(storage
        .list_messages(&MessageFilter::default())
        .await
        .unwrap()
        .is_empty());

    // Permanent thread delete.
    storage.delete_thread(id).await.unwrap();
    assert!(storage.get_message(id).await.unwrap().is_none());
}
