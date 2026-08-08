use crate::errors::MailError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StorageType {
    Memory,
    Postgres,
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

    /// Relay used to send replies.
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
        }
    }
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

        Ok(config)
    }
}
