use crate::errors::MailError;
use crate::sender::OutgoingReply;
use crate::service::MailService;
use crate::storage::NewReply;

impl MailService {
    /// Send a reply to a stored message and record the outcome.
    ///
    /// Threading: `In-Reply-To`/`References` are set from the original
    /// message's `Message-ID`, so the reply stays in the same thread. `From`
    /// is the address the original mail was addressed to (e.g.
    /// `contact@leanhigh.com`), keeping the conversation on that identity;
    /// `To` is the original sender.
    ///
    /// The delivery outcome is persisted to `mail_replies` either way; on a
    /// successful send the parent message is stamped `replied_at`. A send
    /// failure still records a `failed` reply row, then returns the error.
    /// The actual transport is the injected [`MailSender`](crate::MailSender).
    pub async fn send_reply(
        &self,
        message_id: i64,
        body_text: Option<String>,
        body_html: Option<String>,
        sent_by: Option<String>,
    ) -> Result<i64, MailError> {
        if body_text.is_none() && body_html.is_none() {
            return Err(MailError::ReplyError(
                "reply must have a text or html body".to_string(),
            ));
        }

        let original = self
            .storage
            .get_message(message_id)
            .await?
            .ok_or_else(|| MailError::ReplyError(format!("no message with id {message_id}")))?;

        let subject = match original.subject.as_deref() {
            Some(s) if s.trim().to_lowercase().starts_with("re:") => s.to_string(),
            Some(s) => format!("Re: {s}"),
            None => "Re:".to_string(),
        };

        // Angle-bracket the original Message-ID for the threading headers.
        let bracketed = original.message_id.as_deref().map(|mid| {
            if mid.starts_with('<') {
                mid.to_string()
            } else {
                format!("<{mid}>")
            }
        });

        let reply = OutgoingReply {
            from: original.address,
            to: original.envelope_from,
            subject,
            in_reply_to: bracketed.clone(),
            references: bracketed,
            body_text: body_text.clone(),
            body_html: body_html.clone(),
        };

        let send_result = self.sender.send(reply).await;

        let delivery_status = if send_result.is_ok() {
            "sent"
        } else {
            "failed"
        };
        let reply_id = self
            .storage
            .save_reply(NewReply {
                message_id,
                sent_by,
                body_text,
                body_html,
                delivery_status: delivery_status.to_string(),
            })
            .await?;

        match send_result {
            Ok(_) => {
                self.storage.mark_replied(message_id).await?;
                Ok(reply_id)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MailConfig;
    use crate::sender::MailSender;
    use crate::storage::{MockStorage, StoredMessage};
    use async_trait::async_trait;
    use chrono::Utc;
    use std::sync::{Arc, Mutex};

    /// Test [`MailSender`] that records what it was asked to send and can be
    /// told to fail, so the reply path is exercised without a live relay.
    #[derive(Clone, Default)]
    struct RecordingSender {
        sent: Arc<Mutex<Vec<OutgoingReply>>>,
        fail: bool,
    }

    #[async_trait]
    impl MailSender for RecordingSender {
        async fn send(&self, reply: OutgoingReply) -> Result<(), MailError> {
            self.sent.lock().unwrap().push(reply);
            if self.fail {
                Err(MailError::ReplyError("simulated relay failure".to_string()))
            } else {
                Ok(())
            }
        }
    }

    fn stored_message() -> StoredMessage {
        StoredMessage {
            id: 1,
            address: "contact@leanhigh.com".to_string(),
            message_id: Some("abc@example.com".to_string()),
            envelope_from: "someone@example.com".to_string(),
            envelope_to: "contact@leanhigh.com".to_string(),
            from_display: Some("Someone".to_string()),
            subject: Some("Hello there".to_string()),
            body_text: Some("hi".to_string()),
            body_html: None,
            spf_result: Some("pass".to_string()),
            dkim_result: Some("pass".to_string()),
            received_at: Utc::now(),
            read_at: None,
            replied_at: None,
            attachments: vec![],
            replies: vec![],
        }
    }

    fn service_with(storage: MockStorage, sender: RecordingSender) -> MailService {
        MailService::with_sender(MailConfig::default(), Arc::new(storage), Arc::new(sender))
            .expect("service init")
    }

    #[tokio::test]
    async fn send_reply_builds_threaded_reply_and_marks_replied() {
        let mut storage = MockStorage::new();
        storage
            .expect_get_message()
            .returning(|_| Ok(Some(stored_message())));
        storage.expect_save_reply().returning(|_| Ok(42));
        storage.expect_mark_replied().times(1).returning(|_| Ok(()));

        let sender = RecordingSender::default();
        let recorded = sender.sent.clone();
        let service = service_with(storage, sender);

        let reply_id = service
            .send_reply(
                1,
                Some("my answer".to_string()),
                None,
                Some("op@x.com".to_string()),
            )
            .await
            .expect("reply sent");

        assert_eq!(reply_id, 42);
        let sent = recorded.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let r = &sent[0];
        assert_eq!(r.from, "contact@leanhigh.com");
        assert_eq!(r.to, "someone@example.com");
        assert_eq!(r.subject, "Re: Hello there");
        assert_eq!(r.in_reply_to.as_deref(), Some("<abc@example.com>"));
        assert_eq!(r.references.as_deref(), Some("<abc@example.com>"));
    }

    #[tokio::test]
    async fn send_reply_records_failed_and_does_not_mark_replied_on_send_error() {
        let mut storage = MockStorage::new();
        storage
            .expect_get_message()
            .returning(|_| Ok(Some(stored_message())));
        // A "failed" reply row is still saved...
        storage.expect_save_reply().times(1).returning(|_| Ok(7));
        // ...but the parent is NOT stamped replied_at.
        storage.expect_mark_replied().never();

        let sender = RecordingSender {
            fail: true,
            ..Default::default()
        };
        let service = service_with(storage, sender);

        let result = service
            .send_reply(1, Some("answer".to_string()), None, None)
            .await;
        assert!(matches!(result, Err(MailError::ReplyError(_))));
    }

    #[tokio::test]
    async fn send_reply_rejects_empty_body() {
        let storage = MockStorage::new(); // no calls expected
        let service = service_with(storage, RecordingSender::default());
        let result = service.send_reply(1, None, None, None).await;
        assert!(matches!(result, Err(MailError::ReplyError(_))));
    }
}
