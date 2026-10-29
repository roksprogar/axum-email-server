use crate::errors::MailError;
use crate::sender::OutgoingReply;
use crate::service::MailService;
use crate::storage::NewReply;
use uuid::Uuid;

/// Wrap a Message-ID in angle brackets if it isn't already (header form).
fn bracket_id(mid: &str) -> String {
    let t = mid.trim();
    if t.starts_with('<') && t.ends_with('>') {
        t.to_string()
    } else {
        format!("<{}>", t.trim_start_matches('<').trim_end_matches('>'))
    }
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Append the original message as a quote, the way a mail client does, so the
/// recipient sees the history below our answer. Each side quoting the previous
/// mail is what carries the whole thread forward.
fn quote_original(
    body_text: Option<String>,
    body_html: Option<String>,
    attribution: &str,
    original_text: Option<&str>,
) -> (Option<String>, Option<String>) {
    let Some(original) = original_text.map(str::trim_end).filter(|t| !t.is_empty()) else {
        return (body_text, body_html);
    };

    let text = body_text.map(|t| {
        let quoted: Vec<String> = original
            .lines()
            .map(|l| {
                if l.is_empty() {
                    ">".to_string()
                } else {
                    format!("> {l}")
                }
            })
            .collect();
        format!("{}\n\n{attribution}\n{}", t.trim_end(), quoted.join("\n"))
    });
    let html = body_html.map(|h| {
        format!(
            "{h}<br><div>{}</div><blockquote style=\"margin:0 0 0 .8ex;border-left:1px solid #ccc;padding-left:1ex\">{}</blockquote>",
            escape_html(attribution),
            escape_html(original).replace('\n', "<br>")
        )
    });
    (text, html)
}

impl MailService {
    /// Send a reply to a stored message and record the outcome.
    ///
    /// Threading: `In-Reply-To`/`References` are set from the original
    /// message's `Message-ID`, so the reply stays in the same thread. `From`
    /// is the address the original mail was addressed to (e.g.
    /// `contact@leanhigh.com`), keeping the conversation on that identity;
    /// `To` is the original's reply address (its `Reply-To`/`From`, see
    /// [`RawMessage::reply_to`](crate::RawMessage::reply_to)), falling back to
    /// the envelope sender for messages stored without one.
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

        // In-Reply-To: the original's own Message-ID.
        let original_bracketed = original.message_id.as_deref().map(bracket_id);

        // References: the original's References chain (if any), then the
        // original's own Message-ID, so the reply extends the same thread.
        let mut refs: Vec<String> = Vec::new();
        if let Some(chain) = original.references.as_deref() {
            for tok in chain.split_whitespace() {
                let b = bracket_id(tok);
                if !refs.contains(&b) {
                    refs.push(b);
                }
            }
        }
        if let Some(ref b) = original_bracketed {
            if !refs.contains(b) {
                refs.push(b.clone());
            }
        }
        let references = (!refs.is_empty()).then(|| refs.join(" "));

        // Our own Message-ID, stamped on the outbound reply and recorded so a
        // later inbound reply-to-this-reply threads back into the conversation.
        let reply_domain = original
            .address
            .rsplit_once('@')
            .map(|(_, d)| d)
            .unwrap_or("localhost");
        let reply_mid_bare = format!("{}@{}", Uuid::new_v4(), reply_domain);
        let reply_mid_wire = format!("<{reply_mid_bare}>");

        // Messages stored before `reply_to` existed have none; their envelope
        // sender is the best address we have.
        let to = original.reply_to.unwrap_or(original.envelope_from);

        // Only the outbound mail carries the quote; the stored reply keeps just
        // our new text, since the conversation view already shows the history.
        let who = match original.from_display.as_deref() {
            Some(name) if !name.trim().is_empty() => format!("{} <{to}>", name.trim()),
            _ => to.clone(),
        };
        let attribution = format!(
            "On {}, {who} wrote:",
            original.received_at.format("%a, %-d %b %Y at %H:%M UTC")
        );
        let (quoted_text, quoted_html) = quote_original(
            body_text.clone(),
            body_html.clone(),
            &attribution,
            original.body_text.as_deref(),
        );

        let reply = OutgoingReply {
            from: original.address,
            to,
            subject,
            message_id: Some(reply_mid_wire),
            in_reply_to: original_bracketed,
            references,
            body_text: quoted_text,
            body_html: quoted_html,
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
                rfc_message_id: Some(reply_mid_bare),
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
            conversation_id: 1,
            address: "contact@leanhigh.com".to_string(),
            message_id: Some("abc@example.com".to_string()),
            in_reply_to: None,
            references: None,
            envelope_from: "someone@example.com".to_string(),
            envelope_to: "contact@leanhigh.com".to_string(),
            reply_to: None,
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
    async fn send_reply_quotes_original_but_stores_only_new_text() {
        let mut storage = MockStorage::new();
        storage.expect_get_message().returning(|_| {
            Ok(Some(StoredMessage {
                body_text: Some("first line\n\nsecond <b>".to_string()),
                ..stored_message()
            }))
        });
        storage
            .expect_save_reply()
            .withf(|r| r.body_text.as_deref() == Some("my answer"))
            .returning(|_| Ok(1));
        storage.expect_mark_replied().returning(|_| Ok(()));

        let sender = RecordingSender::default();
        let recorded = sender.sent.clone();
        let service = service_with(storage, sender);

        service
            .send_reply(
                1,
                Some("my answer".to_string()),
                Some("<p>my answer</p>".to_string()),
                None,
            )
            .await
            .expect("reply sent");

        let sent = recorded.lock().unwrap();
        let text = sent[0].body_text.as_deref().unwrap();
        assert!(text.starts_with("my answer\n\nOn "));
        assert!(
            text.contains("Someone <someone@example.com> wrote:\n> first line\n>\n> second <b>")
        );
        let html = sent[0].body_html.as_deref().unwrap();
        assert!(html.starts_with("<p>my answer</p>"));
        assert!(html.contains("<blockquote"));
        assert!(html.contains("first line<br><br>second &lt;b&gt;"));
    }

    #[tokio::test]
    async fn send_reply_goes_to_reply_to_not_envelope_sender() {
        // A Brevo-relayed contact form: the envelope sender is Brevo's bounce
        // address, the visitor is only in Reply-To.
        let mut storage = MockStorage::new();
        storage.expect_get_message().returning(|_| {
            Ok(Some(StoredMessage {
                envelope_from: "bounces-123@gw.d.sender-sib.com".to_string(),
                reply_to: Some("visitor@example.org".to_string()),
                ..stored_message()
            }))
        });
        storage.expect_save_reply().returning(|_| Ok(1));
        storage.expect_mark_replied().returning(|_| Ok(()));

        let sender = RecordingSender::default();
        let recorded = sender.sent.clone();
        let service = service_with(storage, sender);

        service
            .send_reply(1, Some("answer".to_string()), None, None)
            .await
            .expect("reply sent");

        assert_eq!(recorded.lock().unwrap()[0].to, "visitor@example.org");
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
