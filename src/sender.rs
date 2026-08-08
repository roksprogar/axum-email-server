//! Pluggable reply delivery. The core `send_reply` flow builds an
//! [`OutgoingReply`] and hands it to a [`MailSender`], so the transport is a
//! seam: the default [`LettreMailSender`] speaks SMTP to your relay, while a
//! test double can record replies without a live server. Mirrors
//! `axum-email-otp-auth`'s `EmailSender` abstraction.

use crate::config::MailConfig;
use crate::errors::MailError;
use async_trait::async_trait;
use lettre::message::{header::ContentType, Mailbox, MultiPart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

/// A reply ready to deliver. Threading headers (`in_reply_to`/`references`) and
/// the `Re:`-prefixed subject are already resolved by the caller; the sender
/// only turns this into a wire message and delivers it.
#[derive(Debug, Clone)]
pub struct OutgoingReply {
    pub from: String,
    pub to: String,
    pub subject: String,
    /// Angle-bracketed Message-ID of the message being replied to, if any.
    pub in_reply_to: Option<String>,
    pub references: Option<String>,
    pub body_text: Option<String>,
    pub body_html: Option<String>,
}

/// Delivers replies. Implement this to plug in a different transport (or a test
/// double); [`LettreMailSender`] is the batteries-included default.
#[async_trait]
pub trait MailSender: Send + Sync {
    async fn send(&self, reply: OutgoingReply) -> Result<(), MailError>;
}

/// Default [`MailSender`]: sends over an SMTP relay via `lettre`. With no relay
/// username it connects in the clear with no auth (e.g. a local mailpit);
/// otherwise it uses STARTTLS with credentials (e.g. Brevo).
pub struct LettreMailSender {
    host: String,
    port: u16,
    username: String,
    password: String,
}

impl LettreMailSender {
    pub fn from_config(config: &MailConfig) -> Self {
        Self {
            host: config.smtp_relay_host.clone(),
            port: config.smtp_relay_port,
            username: config.smtp_relay_username.clone(),
            password: config.smtp_relay_password.clone(),
        }
    }

    fn transport(&self) -> Result<AsyncSmtpTransport<Tokio1Executor>, MailError> {
        let builder = if self.username.is_empty() {
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&self.host)
        } else {
            AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&self.host)
                .map_err(|e| MailError::ReplyError(format!("SMTP transport setup failed: {e}")))?
                .credentials(Credentials::new(
                    self.username.clone(),
                    self.password.clone(),
                ))
        };
        Ok(builder.port(self.port).build())
    }

    fn build_message(reply: &OutgoingReply) -> Result<Message, MailError> {
        let from: Mailbox = reply.from.parse().map_err(|e| {
            MailError::ReplyError(format!("invalid from address {}: {e}", reply.from))
        })?;
        let to: Mailbox = reply
            .to
            .parse()
            .map_err(|e| MailError::ReplyError(format!("invalid recipient {}: {e}", reply.to)))?;

        let mut builder = Message::builder()
            .from(from)
            .to(to)
            .subject(reply.subject.clone());
        if let Some(mid) = &reply.in_reply_to {
            builder = builder.in_reply_to(mid.clone());
        }
        if let Some(refs) = &reply.references {
            builder = builder.references(refs.clone());
        }

        match (reply.body_text.clone(), reply.body_html.clone()) {
            (Some(text), Some(html)) => {
                builder.multipart(MultiPart::alternative_plain_html(text, html))
            }
            (Some(text), None) => builder.header(ContentType::TEXT_PLAIN).body(text),
            (None, Some(html)) => builder.header(ContentType::TEXT_HTML).body(html),
            (None, None) => {
                return Err(MailError::ReplyError(
                    "reply must have a text or html body".to_string(),
                ))
            }
        }
        .map_err(|e| MailError::ReplyError(format!("failed to build reply message: {e}")))
    }
}

#[async_trait]
impl MailSender for LettreMailSender {
    async fn send(&self, reply: OutgoingReply) -> Result<(), MailError> {
        let email = Self::build_message(&reply)?;
        self.transport()?
            .send(email)
            .await
            .map_err(|e| MailError::ReplyError(format!("SMTP send failed: {e}")))?;
        Ok(())
    }
}
