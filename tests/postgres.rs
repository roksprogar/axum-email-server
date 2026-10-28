//! Integration test for `PostgresStorage` against a real database.
//!
//! Runs only when `DATABASE_URL` is set (the CI job provides a Postgres
//! service); otherwise it skips, so `cargo test` on a machine without Postgres
//! stays green. It creates the schema itself, since the crate ships the queries
//! but the consumer owns the migration.
#![cfg(feature = "postgres")]

use axum_email_server::{MessageFilter, NewReply, PostgresStorage, RawMessage, Storage};
use sqlx::postgres::PgPoolOptions;

/// Both Postgres tests DROP/CREATE the same tables in the one shared database,
/// so they must not run concurrently (the default test harness runs them in
/// parallel, and CI does not pass `--test-threads=1`). This lock serializes
/// them. A tokio mutex, so holding it across `.await` is fine.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn apply_schema(pool: &sqlx::PgPool) {
    for stmt in SCHEMA.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt)
            .execute(pool)
            .await
            .expect("apply schema statement");
    }
}

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
    id                 BIGSERIAL PRIMARY KEY,
    address_id         BIGINT NOT NULL REFERENCES mail_addresses(id),
    message_id         TEXT,
    in_reply_to        TEXT,
    references_header  TEXT,
    subject_normalized TEXT,
    conversation_id    BIGINT,
    envelope_from      TEXT NOT NULL,
    envelope_to        TEXT NOT NULL,
    reply_to           TEXT,
    from_display       TEXT,
    subject            TEXT,
    body_text          TEXT,
    body_html          TEXT,
    raw_mime           BYTEA NOT NULL,
    spf_result         TEXT,
    dkim_result        TEXT,
    received_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at            TIMESTAMPTZ,
    replied_at         TIMESTAMPTZ
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
    rfc_message_id   TEXT,
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
        reply_to: Some("visitor@example.org".to_string()),
        message_id: Some("mid-1@example.com".to_string()),
        in_reply_to: None,
        references: None,
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
    // Serialize DB tests: held until the test ends so no other test recreates
    // the shared schema mid-run.
    let _db_guard = DB_LOCK.lock().await;
    apply_schema(&pool).await;

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
    assert_eq!(msg.reply_to.as_deref(), Some("visitor@example.org"));
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

#[tokio::test]
async fn postgres_threads_conversations() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set — skipping Postgres threading test");
        return;
    };

    let pool = PgPoolOptions::new()
        .connect(&url)
        .await
        .expect("connect to Postgres");
    // Serialize DB tests: held until the test ends so no other test recreates
    // the shared schema mid-run.
    let _db_guard = DB_LOCK.lock().await;
    apply_schema(&pool).await;

    let storage = PostgresStorage::new(pool);
    storage
        .create_address("contact", "leanhigh.com", None)
        .await
        .unwrap();

    // Inbound A → new conversation.
    let a = storage
        .save_message(RawMessage {
            message_id: Some("m1@example.com".to_string()),
            subject: Some("Hello".to_string()),
            ..sample_message()
        })
        .await
        .unwrap() as i64;

    // Our reply to A, stamped with Message-ID r1.
    storage
        .save_reply(NewReply {
            message_id: a,
            rfc_message_id: Some("r1@leanhigh.com".to_string()),
            sent_by: Some("op@leanhigh.com".to_string()),
            body_text: Some("hi".to_string()),
            body_html: None,
            delivery_status: "sent".to_string(),
        })
        .await
        .unwrap();

    // Inbound B references our reply r1 → same conversation.
    let b = storage
        .save_message(RawMessage {
            message_id: Some("m2@example.com".to_string()),
            in_reply_to: Some("<r1@leanhigh.com>".to_string()),
            subject: Some("Re: Hello".to_string()),
            ..sample_message()
        })
        .await
        .unwrap() as i64;

    let convs = storage
        .list_conversations(&MessageFilter::default())
        .await
        .unwrap();
    assert_eq!(convs.len(), 1, "A, our reply, and B group into one row");
    let conv_id = convs[0].conversation_id;
    assert_eq!(convs[0].message_count, 3);
    assert_eq!(convs[0].unread_count, 2);
    assert!(convs[0].replied);

    let thread = storage
        .get_conversation(conv_id)
        .await
        .unwrap()
        .expect("thread exists");
    assert_eq!(thread.items.len(), 3);
    assert_eq!(thread.latest_message_id, Some(b));
    assert_ne!(a, b);

    // Marking the conversation read clears the unread count.
    storage.mark_conversation_read(conv_id).await.unwrap();
    let convs = storage
        .list_conversations(&MessageFilter::default())
        .await
        .unwrap();
    assert_eq!(convs[0].unread_count, 0);

    // Deleting the conversation removes every message in it.
    storage.delete_conversation(conv_id).await.unwrap();
    assert!(storage
        .list_conversations(&MessageFilter::default())
        .await
        .unwrap()
        .is_empty());
    assert!(storage.get_message(a).await.unwrap().is_none());
    assert!(storage.get_message(b).await.unwrap().is_none());
}
