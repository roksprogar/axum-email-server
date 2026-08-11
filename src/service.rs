use crate::config::MailConfig;
use crate::errors::MailError;
use crate::filter::Greylist;
use crate::sender::{LettreMailSender, MailSender};
use crate::storage::Storage;
use mail_auth::MessageAuthenticator;
use std::sync::Arc;
use std::time::Duration;

/// Core service: holds config, storage, the DNS-backed authenticator used for
/// SPF/DKIM/DMARC verification, the greylist, and the reply sender.
#[derive(Clone)]
pub struct MailService {
    pub config: Arc<MailConfig>,
    pub storage: Arc<dyn Storage>,
    pub authenticator: Arc<MessageAuthenticator>,
    /// Shared across all sessions; enforced only when
    /// `config.greylist_delay_seconds > 0`.
    pub greylist: Arc<Greylist>,
    pub sender: Arc<dyn MailSender>,
}

impl MailService {
    /// Uses the default [`LettreMailSender`] built from `config` for reply
    /// delivery. Fails only if the system DNS resolver config
    /// (`/etc/resolv.conf`) can't be read — SPF/DKIM verification needs it to
    /// look up DNS records.
    pub fn new(config: MailConfig, storage: Arc<dyn Storage>) -> Result<Self, MailError> {
        let sender = Arc::new(LettreMailSender::from_config(&config));
        Self::with_sender(config, storage, sender)
    }

    /// Like [`MailService::new`] but with a caller-supplied [`MailSender`], so
    /// the reply path can be tested (or pointed at a different transport)
    /// without a live SMTP relay.
    pub fn with_sender(
        config: MailConfig,
        storage: Arc<dyn Storage>,
        sender: Arc<dyn MailSender>,
    ) -> Result<Self, MailError> {
        let authenticator = MessageAuthenticator::new_system_conf().map_err(|e| {
            MailError::ConfigError(format!("failed to initialize DNS resolver: {e}"))
        })?;
        let greylist = Arc::new(Greylist::new(Duration::from_secs(
            config.greylist_delay_seconds,
        )));
        Ok(Self {
            config: Arc::new(config),
            storage,
            authenticator: Arc::new(authenticator),
            greylist,
            sender,
        })
    }
}
