use crate::errors::MailError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageType {
    Memory,
    Postgres,
}

/// How replies are delivered. Read from the same `EMAIL_PROVIDER` /
/// `BREVO_API_KEY` env vars as `axum-email-otp-auth`, so one setting switches both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EmailProvider {
    /// SMTP relay (`SMTP_RELAY_*`), e.g. a local mailpit.
    Smtp,
    /// Brevo's HTTPS API — works on hosts that block outbound SMTP ports.
    Brevo,
}

#[derive(Debug, Clone)]
pub struct MailConfig {
    /// Which `Storage` backend the consuming app should construct.
    pub storage_type: StorageType,

    /// Required when `storage_type` is `Postgres`.
    pub database_url: String,

    /// Address the SMTP receive loop binds to, e.g. "0.0.0.0:25".
    pub smtp_bind_addr: String,

    /// This server's own hostname, e.g. "localmail.lifetivation.com".
    /// Passed to SPF verification as the receiving host's identity.
    pub hostname: String,

    /// TLS certificate/key used for STARTTLS.
    pub tls_cert_path: String,
    pub tls_key_path: String,

    /// Transport used to send replies.
    pub email_provider: EmailProvider,
    /// Required when `email_provider` is `Brevo`.
    pub brevo_api_key: Option<String>,

    /// Relay used to send replies when `email_provider` is `Smtp`.
    pub smtp_relay_host: String,
    pub smtp_relay_port: u16,
    pub smtp_relay_username: String,
    pub smtp_relay_password: String,

    /// Protocol-level limits.
    pub max_message_size_bytes: usize,
    pub max_concurrent_connections: usize,
    pub command_timeout_seconds: u64,
    pub session_timeout_seconds: u64,
    pub max_recipients_per_message: usize,
    /// Max new connections accepted per remote IP per minute (0 = unlimited).
    pub max_connections_per_ip_per_minute: u32,

    // --- Spam / abuse filtering. All opt-in: the defaults below preserve the
    // pre-filtering behavior (accept + label), so enabling any of these is a
    // deliberate config choice. None of them load models or hold significant
    // state — the cost is at most a cached DNS lookup or a small in-memory map.
    /// DNSBL zones queried against the connecting IP, e.g.
    /// ["zen.spamhaus.org"]. A connection whose IP is listed in ANY zone is
    /// refused at greeting time with a 554. Empty disables DNSBL.
    pub dnsbl_zones: Vec<String>,

    /// Reject at `MAIL FROM` when SPF hard-fails — the sender domain publishes
    /// `-all` and this IP isn't authorized. SoftFail/Neutral/None/errors are
    /// never rejected on this alone. Default false (SPF result is only
    /// recorded, as before).
    pub reject_on_spf_fail: bool,

    /// Honor the sender domain's published DMARC policy: when it is `p=reject`
    /// and the message fails DMARC (neither SPF nor DKIM aligns), reject at
    /// DATA. `p=quarantine`/`p=none` are never rejected. Default false.
    pub enforce_dmarc: bool,

    /// Reject `HELO`/`EHLO` whose argument is syntactically invalid (absent, a
    /// bare IP without brackets, or not domain-shaped). Pure string checks, no
    /// DNS. Default false.
    pub reject_invalid_helo: bool,

    /// Greylisting delay in seconds: the first delivery attempt from an unseen
    /// (IP, MAIL FROM, RCPT) triplet is temp-rejected with 451, and admitted
    /// once the sender retries after this delay. In-memory (resets on restart).
    /// 0 disables greylisting. Default 0.
    pub greylist_delay_seconds: u64,
}

impl Default for MailConfig {
    fn default() -> Self {
        Self {
            storage_type: StorageType::Memory,
            database_url: "".to_string(),
            smtp_bind_addr: "0.0.0.0:25".to_string(),
            hostname: "mail.example.com".to_string(),
            tls_cert_path: "/etc/mail/certs/fullchain.pem".to_string(),
            tls_key_path: "/etc/mail/certs/privkey.pem".to_string(),
            email_provider: EmailProvider::Smtp,
            brevo_api_key: None,
            smtp_relay_host: "localhost".to_string(),
            smtp_relay_port: 587,
            smtp_relay_username: "".to_string(),
            smtp_relay_password: "".to_string(),
            max_message_size_bytes: 25 * 1024 * 1024,
            max_concurrent_connections: 50,
            command_timeout_seconds: 30,
            session_timeout_seconds: 300,
            max_recipients_per_message: 10,
            max_connections_per_ip_per_minute: 60,
            dnsbl_zones: Vec::new(),
            reject_on_spf_fail: false,
            enforce_dmarc: false,
            reject_invalid_helo: false,
            greylist_delay_seconds: 0,
        }
    }
}

/// Parses a boolean env value leniently: `1`/`true`/`yes`/`on` (any case) are
/// true, everything else false.
fn env_bool(val: &str) -> bool {
    matches!(
        val.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl MailConfig {
    pub fn from_env() -> Result<Self, MailError> {
        let mut config = Self::default();

        // Namespaced (`MAIL_STORAGE_TYPE`, not `STORAGE_TYPE`) so this crate can
        // sit alongside other libs that read their own `STORAGE_TYPE` — e.g.
        // `axum-email-otp-auth`, which uses it for `memory`/`redis`.
        if let Ok(val) = std::env::var("MAIL_STORAGE_TYPE") {
            config.storage_type = match val.to_lowercase().as_str() {
                "memory" => StorageType::Memory,
                "postgres" => StorageType::Postgres,
                _ => {
                    return Err(MailError::ConfigError(
                        "Invalid MAIL_STORAGE_TYPE: must be 'memory' or 'postgres'".to_string(),
                    ))
                }
            };
        }
        if config.storage_type == StorageType::Postgres {
            config.database_url = std::env::var("DATABASE_URL").map_err(|_| {
                MailError::ConfigError(
                    "DATABASE_URL must be set when STORAGE_TYPE=postgres".to_string(),
                )
            })?;
        }

        if let Ok(val) = std::env::var("SMTP_BIND_ADDR") {
            config.smtp_bind_addr = val;
        }
        if let Ok(val) = std::env::var("MAIL_HOSTNAME") {
            config.hostname = val;
        }
        if let Ok(val) = std::env::var("TLS_CERT_PATH") {
            config.tls_cert_path = val;
        }
        if let Ok(val) = std::env::var("TLS_KEY_PATH") {
            config.tls_key_path = val;
        }
        if let Ok(val) = std::env::var("EMAIL_PROVIDER") {
            config.email_provider = match val.to_lowercase().as_str() {
                "smtp" => EmailProvider::Smtp,
                "brevo" => EmailProvider::Brevo,
                _ => {
                    return Err(MailError::ConfigError(
                        "Invalid EMAIL_PROVIDER: must be 'smtp' or 'brevo'".to_string(),
                    ))
                }
            };
        }
        if let Ok(val) = std::env::var("BREVO_API_KEY") {
            config.brevo_api_key = Some(val);
        }
        if config.email_provider == EmailProvider::Brevo && config.brevo_api_key.is_none() {
            return Err(MailError::ConfigError(
                "BREVO_API_KEY must be set when EMAIL_PROVIDER is 'brevo'".to_string(),
            ));
        }
        if let Ok(val) = std::env::var("SMTP_RELAY_HOST") {
            config.smtp_relay_host = val;
        }
        if let Ok(val) = std::env::var("SMTP_RELAY_PORT") {
            config.smtp_relay_port = val
                .parse()
                .map_err(|_| MailError::ConfigError("Invalid SMTP_RELAY_PORT".to_string()))?;
        }
        if let Ok(val) = std::env::var("SMTP_RELAY_USERNAME") {
            config.smtp_relay_username = val;
        }
        if let Ok(val) = std::env::var("SMTP_RELAY_PASSWORD") {
            config.smtp_relay_password = val;
        }
        if let Ok(val) = std::env::var("MAX_MESSAGE_SIZE_BYTES") {
            config.max_message_size_bytes = val.parse().map_err(|_| {
                MailError::ConfigError("Invalid MAX_MESSAGE_SIZE_BYTES".to_string())
            })?;
        }
        if let Ok(val) = std::env::var("MAX_CONCURRENT_CONNECTIONS") {
            config.max_concurrent_connections = val.parse().map_err(|_| {
                MailError::ConfigError("Invalid MAX_CONCURRENT_CONNECTIONS".to_string())
            })?;
        }
        if let Ok(val) = std::env::var("COMMAND_TIMEOUT_SECONDS") {
            config.command_timeout_seconds = val.parse().map_err(|_| {
                MailError::ConfigError("Invalid COMMAND_TIMEOUT_SECONDS".to_string())
            })?;
        }
        if let Ok(val) = std::env::var("SESSION_TIMEOUT_SECONDS") {
            config.session_timeout_seconds = val.parse().map_err(|_| {
                MailError::ConfigError("Invalid SESSION_TIMEOUT_SECONDS".to_string())
            })?;
        }
        if let Ok(val) = std::env::var("MAX_RECIPIENTS_PER_MESSAGE") {
            config.max_recipients_per_message = val.parse().map_err(|_| {
                MailError::ConfigError("Invalid MAX_RECIPIENTS_PER_MESSAGE".to_string())
            })?;
        }
        if let Ok(val) = std::env::var("MAX_CONNECTIONS_PER_IP_PER_MINUTE") {
            config.max_connections_per_ip_per_minute = val.parse().map_err(|_| {
                MailError::ConfigError("Invalid MAX_CONNECTIONS_PER_IP_PER_MINUTE".to_string())
            })?;
        }

        // Spam / abuse filtering (all optional; absent env vars keep the
        // defaults above).
        if let Ok(val) = std::env::var("DNSBL_ZONES") {
            // Comma- or whitespace-separated list of DNSBL zones.
            config.dnsbl_zones = val
                .split([',', ' ', '\t'])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        if let Ok(val) = std::env::var("REJECT_ON_SPF_FAIL") {
            config.reject_on_spf_fail = env_bool(&val);
        }
        if let Ok(val) = std::env::var("ENFORCE_DMARC") {
            config.enforce_dmarc = env_bool(&val);
        }
        if let Ok(val) = std::env::var("REJECT_INVALID_HELO") {
            config.reject_invalid_helo = env_bool(&val);
        }
        if let Ok(val) = std::env::var("GREYLIST_DELAY_SECONDS") {
            config.greylist_delay_seconds = val.parse().map_err(|_| {
                MailError::ConfigError("Invalid GREYLIST_DELAY_SECONDS".to_string())
            })?;
        }

        Ok(config)
    }
}
