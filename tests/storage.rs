use axum_email_server::{InMemoryStorage, RawMessage, Storage};

#[tokio::test]
async fn save_message_assigns_increasing_ids() {
    let storage = InMemoryStorage::new();
    let msg = RawMessage {
        address: "contact@leanhigh.com".to_string(),
        envelope_from: "someone@example.com".to_string(),
        envelope_to: "contact@leanhigh.com".to_string(),
        message_id: None,
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
