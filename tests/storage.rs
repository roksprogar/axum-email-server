use axum_email_server::{InMemoryStorage, MessageFilter, NewReply, RawMessage, Storage};

/// Minimal inbound `RawMessage` for the threading tests.
fn inbound(
    address: &str,
    from: &str,
    message_id: Option<&str>,
    in_reply_to: Option<&str>,
    subject: Option<&str>,
) -> RawMessage {
    RawMessage {
        address: address.to_string(),
        envelope_from: from.to_string(),
        envelope_to: address.to_string(),
        message_id: message_id.map(str::to_string),
        in_reply_to: in_reply_to.map(str::to_string),
        references: None,
        from_display: None,
        subject: subject.map(str::to_string),
        body_text: Some("body".to_string()),
        body_html: None,
        raw_data: b"raw".to_vec(),
        spf_result: Some("pass".to_string()),
        dkim_result: Some("none".to_string()),
    }
}

#[tokio::test]
async fn save_message_assigns_increasing_ids() {
    let storage = InMemoryStorage::new();
    let msg = RawMessage {
        address: "contact@leanhigh.com".to_string(),
        envelope_from: "someone@example.com".to_string(),
        envelope_to: "contact@leanhigh.com".to_string(),
        message_id: None,
        in_reply_to: None,
        references: None,
        from_display: None,
        subject: None,
        body_text: None,
        body_html: None,
        raw_data: b"hello".to_vec(),
        spf_result: None,
        dkim_result: None,
    };

    let first_id = storage.save_message(msg.clone()).await.unwrap();
    let second_id = storage.save_message(msg).await.unwrap();

    assert!(second_id > first_id);
}

#[tokio::test]
async fn health_reports_ok() {
    let storage = InMemoryStorage::new();
    assert!(storage.health().await.is_ok());
}

#[tokio::test]
async fn unconfigured_address_is_rejected() {
    let storage = InMemoryStorage::new();
    assert!(!storage
        .is_valid_recipient("contact@leanhigh.com")
        .await
        .unwrap());
}

#[tokio::test]
async fn configured_address_is_accepted_case_insensitively() {
    let storage = InMemoryStorage::new().with_addresses(["contact@leanhigh.com"]);
    assert!(storage
        .is_valid_recipient("Contact@LeanHigh.com")
        .await
        .unwrap());
}

#[tokio::test]
async fn reply_to_a_reply_threads_into_one_conversation() {
    let storage = InMemoryStorage::new().with_addresses(["contact@leanhigh.com"]);

    // Inbound A → starts a conversation.
    let a = storage
        .save_message(inbound(
            "contact@leanhigh.com",
            "someone@example.com",
            Some("m1@example.com"),
            None,
            Some("Hello"),
        ))
        .await
        .unwrap() as i64;

    // We reply to A, stamping our own Message-ID r1.
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

    // Inbound B references *our reply* r1 (bracketed) → same conversation.
    let b = storage
        .save_message(inbound(
            "contact@leanhigh.com",
            "someone@example.com",
            Some("m2@example.com"),
            Some("<r1@leanhigh.com>"),
            Some("Re: Hello"),
        ))
        .await
        .unwrap() as i64;

    let convs = storage
        .list_conversations(&MessageFilter::default())
        .await
        .unwrap();
    assert_eq!(convs.len(), 1, "A, our reply, and B are one conversation");
    let c = &convs[0];
    assert_eq!(c.message_count, 3); // A + reply + B
    assert_eq!(c.unread_count, 2); // A and B are unread inbound
    assert!(c.replied);

    let thread = storage
        .get_conversation(c.conversation_id)
        .await
        .unwrap()
        .expect("thread exists");
    assert_eq!(thread.items.len(), 3, "interleaved A, reply, B");
    assert_eq!(
        thread.latest_message_id,
        Some(b),
        "a new reply threads onto the latest inbound message"
    );
    assert_ne!(b, a);
}

#[tokio::test]
async fn subject_fallback_threads_headerless_replies() {
    let storage = InMemoryStorage::new().with_addresses(["contact@leanhigh.com"]);

    let _a = storage
        .save_message(inbound(
            "contact@leanhigh.com",
            "someone@example.com",
            Some("m1@example.com"),
            None,
            Some("Invoice #42"),
        ))
        .await
        .unwrap();
    // No In-Reply-To/References, but same sender + inbox + normalized subject.
    storage
        .save_message(inbound(
            "contact@leanhigh.com",
            "someone@example.com",
            Some("m2@example.com"),
            None,
            Some("Re: Invoice #42"),
        ))
        .await
        .unwrap();

    let convs = storage
        .list_conversations(&MessageFilter::default())
        .await
        .unwrap();
    assert_eq!(convs.len(), 1, "subject fallback groups the two messages");
    assert_eq!(convs[0].message_count, 2);

    // A different subject from the same sender is its own conversation.
    storage
        .save_message(inbound(
            "contact@leanhigh.com",
            "someone@example.com",
            Some("m3@example.com"),
            None,
            Some("A totally different topic"),
        ))
        .await
        .unwrap();
    let convs = storage
        .list_conversations(&MessageFilter::default())
        .await
        .unwrap();
    assert_eq!(convs.len(), 2);
}
