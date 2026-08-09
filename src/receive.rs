use crate::config::MailConfig;
use crate::errors::MailError;
use crate::service::MailService;
use crate::storage::{RawMessage, Storage};
use mail_auth::spf::verify::SpfParameters;
use mail_auth::{AuthenticatedMessage, DkimOutput, DkimResult, MessageAuthenticator, SpfResult};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::{timeout, Duration};
use tokio_rustls::TlsAcceptor;

/// Starts the SMTP receive loop. Runs until the listener errors.
pub async fn run(service: MailService) -> Result<(), MailError> {
    let config = service.config.clone();
    let listener = tokio::net::TcpListener::bind(&config.smtp_bind_addr)
        .await
        .map_err(|e| MailError::Io(format!("failed to bind {}: {}", config.smtp_bind_addr, e)))?;
    tracing::info!("SMTP receive loop listening on {}", config.smtp_bind_addr);

    // TLS is loaded lazily, per STARTTLS (see run_session), so a certbot-renewed
    // cert on disk is served without restarting the process. Here we only probe
    // that a cert/key exist, to warn when STARTTLS won't be offered yet.
    if !tls_configured(&config) {
        tracing::warn!("TLS not configured (no cert/key at the configured paths) — STARTTLS will not be offered until they exist");
    }

    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_connections));
    let ip_limiter = IpRateLimiter::new(
        config.max_connections_per_ip_per_minute,
        Duration::from_secs(60),
    );

    loop {
        let (socket, peer_addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!("accept failed: {}", e);
                continue;
            }
        };

        // Per-IP rate limit before spending a global connection slot, so one
        // peer can't monopolize the pool or hammer the server.
        if !ip_limiter.allow(peer_addr.ip()) {
            tracing::warn!(
                "per-IP connection rate limit reached, dropping connection from {}",
                peer_addr
            );
            continue;
        }

        let permit = match semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!(
                    "connection limit reached, dropping connection from {}",
                    peer_addr
                );
                continue;
            }
        };

        let service = service.clone();
        let session_timeout = Duration::from_secs(service.config.session_timeout_seconds);

        tokio::spawn(async move {
            let _permit = permit;
            if timeout(
                session_timeout,
                handle_connection(socket, peer_addr, service),
            )
            .await
            .is_err()
            {
                tracing::warn!("session with {} exceeded the session timeout", peer_addr);
            }
        });
    }
}

/// Fixed-window per-IP connection rate limiter for the accept loop. In-memory
/// — SMTP accepts are far too frequent to round-trip a store — and pruned
/// opportunistically so the map stays bounded by recently-seen peers. A limit
/// of 0 disables it.
struct IpRateLimiter {
    max_per_window: u32,
    window: Duration,
    seen: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl IpRateLimiter {
    fn new(max_per_window: u32, window: Duration) -> Self {
        Self {
            max_per_window,
            window,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Records a connection attempt from `ip` and returns whether it's allowed.
    fn allow(&self, ip: IpAddr) -> bool {
        if self.max_per_window == 0 {
            return true;
        }
        let now = Instant::now();
        let mut seen = self.seen.lock().unwrap();
        // Bound the map under a churn of one-off peers.
        if seen.len() > 10_000 {
            seen.retain(|_, (start, _)| now.duration_since(*start) < self.window);
        }
        let entry = seen.entry(ip).or_insert((now, 0));
        if now.duration_since(entry.0) >= self.window {
            *entry = (now, 0);
        }
        entry.1 += 1;
        entry.1 <= self.max_per_window
    }
}

/// Cheap existence probe used to decide whether to advertise STARTTLS. The
/// actual parse/load happens per-STARTTLS in `load_tls_acceptor`.
fn tls_configured(config: &MailConfig) -> bool {
    std::path::Path::new(&config.tls_cert_path).exists()
        && std::path::Path::new(&config.tls_key_path).exists()
}

fn load_tls_acceptor(config: &MailConfig) -> Option<TlsAcceptor> {
    let cert_file = std::fs::File::open(&config.tls_cert_path).ok()?;
    let key_file = std::fs::File::open(&config.tls_key_path).ok()?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let mut key_reader = std::io::BufReader::new(key_file);

    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        tracing::warn!("no certificates parsed from {}", config.tls_cert_path);
        return None;
    }

    let key = match rustls_pemfile::private_key(&mut key_reader) {
        Ok(Some(k)) => k,
        _ => {
            tracing::warn!("no private key parsed from {}", config.tls_key_path);
            return None;
        }
    };

    match tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
    {
        Ok(cfg) => Some(TlsAcceptor::from(Arc::new(cfg))),
        Err(e) => {
            tracing::warn!("failed to build TLS server config: {}", e);
            None
        }
    }
}

async fn handle_connection(socket: TcpStream, peer_addr: SocketAddr, service: MailService) {
    tracing::info!("connection from {}", peer_addr);

    match run_session(socket, peer_addr, &service, false).await {
        Ok(LoopResult::Closed) => {}
        Ok(LoopResult::StartTls(acceptor, plain)) => match acceptor.accept(plain).await {
            Ok(tls_stream) => {
                if let Err(e) = run_session(tls_stream, peer_addr, &service, true).await {
                    tracing::warn!("TLS session with {} ended: {}", peer_addr, e);
                }
            }
            Err(e) => tracing::warn!("TLS handshake with {} failed: {}", peer_addr, e),
        },
        Err(e) => tracing::warn!("session with {} ended: {}", peer_addr, e),
    }
}

enum LoopResult<S> {
    Closed,
    StartTls(TlsAcceptor, S),
}

async fn run_session<S>(
    stream: S,
    peer_addr: SocketAddr,
    service: &MailService,
    already_tls: bool,
) -> Result<LoopResult<S>, MailError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let config = service.config.as_ref();
    // Offer STARTTLS only when we're not already inside TLS and a cert/key exist.
    let tls_available = !already_tls && tls_configured(config);
    let mut stream = BufStream::new(stream);
    // Send the 220 greeting only on the initial connection. After STARTTLS the
    // client (per RFC 3207) issues a fresh EHLO and expects the 250 capability
    // reply — re-sending a 220 banner here desyncs the dialogue by one response.
    if !already_tls {
        write_reply(&mut stream, 220, &format!("{} ESMTP", config.hostname)).await?;
    }

    let mut helo_domain: Option<String> = None;
    let mut mail_from: Option<String> = None;
    let mut recipients: Vec<String> = Vec::new();

    loop {
        let line = read_line_timeout(&mut stream, config.command_timeout_seconds).await?;
        let line = line.trim_end();
        if line.is_empty() {
            continue;
        }

        let (verb, rest) = split_command(line);
        match verb.to_ascii_uppercase().as_str() {
            "EHLO" => {
                helo_domain = rest.split_whitespace().next().map(str::to_string);
                mail_from = None;
                recipients.clear();
                write_ehlo(&mut stream, tls_available, config).await?;
            }
            "HELO" => {
                helo_domain = rest.split_whitespace().next().map(str::to_string);
                mail_from = None;
                recipients.clear();
                // RFC 5321 §4.1.1.1: HELO gets a single-line greeting with no
                // ESMTP extension list — those (SIZE/8BITMIME/STARTTLS) are
                // EHLO-only. A HELO-only client can mis-parse a multiline reply.
                write_reply(&mut stream, 250, &config.hostname).await?;
            }
            "STARTTLS" => {
                if already_tls {
                    write_reply(&mut stream, 503, "Already using TLS").await?;
                } else if !tls_available {
                    write_reply(&mut stream, 502, "Command not implemented").await?;
                } else {
                    // Load the cert fresh, here at STARTTLS time, so a renewed
                    // cert on disk is served without restarting the process. If
                    // it can't be loaded right now (e.g. a mid-renewal file
                    // swap), stay in plaintext rather than promise TLS we can't
                    // then deliver.
                    match load_tls_acceptor(config) {
                        Some(acceptor) => {
                            write_reply(&mut stream, 220, "Ready to start TLS").await?;
                            return Ok(LoopResult::StartTls(acceptor, stream.into_inner()));
                        }
                        None => {
                            write_reply(&mut stream, 454, "TLS not available").await?;
                        }
                    }
                }
            }
            "MAIL" => match extract_address(rest, "FROM:") {
                Some(addr) => {
                    mail_from = Some(addr);
                    recipients.clear();
                    write_reply(&mut stream, 250, "OK").await?;
                }
                None => write_reply(&mut stream, 501, "Syntax error in parameters").await?,
            },
            "RCPT" => {
                if mail_from.is_none() {
                    write_reply(&mut stream, 503, "Need MAIL command first").await?;
                    continue;
                }
                match extract_address(rest, "TO:") {
                    Some(addr) => {
                        if recipients.len() >= config.max_recipients_per_message {
                            write_reply(&mut stream, 452, "Too many recipients").await?;
                        } else if service
                            .storage
                            .is_valid_recipient(&addr)
                            .await
                            .unwrap_or(false)
                        {
                            recipients.push(addr);
                            write_reply(&mut stream, 250, "OK").await?;
                        } else {
                            write_reply(&mut stream, 550, "No such user here").await?;
                        }
                    }
                    None => write_reply(&mut stream, 501, "Syntax error in parameters").await?,
                }
            }
            "DATA" => {
                if mail_from.is_none() || recipients.is_empty() {
                    write_reply(&mut stream, 503, "Need MAIL and RCPT first").await?;
                    continue;
                }
                write_reply(&mut stream, 354, "Start mail input; end with <CRLF>.<CRLF>").await?;
                match read_data(
                    &mut stream,
                    config.max_message_size_bytes,
                    config.command_timeout_seconds,
                )
                .await
                {
                    Ok(raw) => {
                        let sender = mail_from.as_deref().unwrap();
                        let (spf_result, dkim_result) = verify_message(
                            &service.authenticator,
                            peer_addr.ip(),
                            helo_domain.as_deref().unwrap_or(""),
                            &config.hostname,
                            sender,
                            &raw,
                        )
                        .await;
                        match store_message(
                            &raw,
                            sender,
                            &recipients,
                            spf_result,
                            dkim_result,
                            &service.storage,
                        )
                        .await
                        {
                            Ok(()) => write_reply(&mut stream, 250, "Message accepted").await?,
                            // Do NOT ACK mail we failed to persist: a 250 here
                            // would tell the sending MTA the message is safely
                            // ours and stop it retrying, silently losing it.
                            // Reply with a transient error so it requeues.
                            Err(_) => {
                                write_reply(
                                    &mut stream,
                                    451,
                                    "4.3.0 Temporary storage failure, try again later",
                                )
                                .await?
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("failed reading DATA from {}: {}", peer_addr, e);
                        write_reply(&mut stream, 552, "Message too large or transfer failed")
                            .await?;
                    }
                }
                mail_from = None;
                recipients.clear();
            }
            "RSET" => {
                mail_from = None;
                recipients.clear();
                write_reply(&mut stream, 250, "OK").await?;
            }
            "NOOP" => write_reply(&mut stream, 250, "OK").await?,
            "QUIT" => {
                write_reply(&mut stream, 221, "Bye").await?;
                return Ok(LoopResult::Closed);
            }
            _ => write_reply(&mut stream, 500, "Command not recognized").await?,
        }
    }
}

/// Runs SPF (against the connecting IP + `MAIL FROM` domain) and DKIM
/// (against the message content) verification, returning normalized
/// `spf_result`/`dkim_result` strings for storage.
async fn verify_message(
    authenticator: &MessageAuthenticator,
    peer_ip: IpAddr,
    helo_domain: &str,
    hostname: &str,
    mail_from: &str,
    raw: &[u8],
) -> (String, String) {
    let spf_params = SpfParameters::verify_mail_from(peer_ip, helo_domain, hostname, mail_from);
    let spf_result =
        spf_result_str(authenticator.verify_spf(spf_params).await.result()).to_string();

    let dkim_result = match AuthenticatedMessage::parse(raw) {
        Some(auth_message) => {
            let outputs = authenticator.verify_dkim(&auth_message).await;
            summarize_dkim(&outputs).to_string()
        }
        None => "none".to_string(),
    };

    (spf_result, dkim_result)
}

fn spf_result_str(result: SpfResult) -> &'static str {
    match result {
        SpfResult::Pass => "pass",
        SpfResult::Fail => "fail",
        SpfResult::SoftFail => "softfail",
        SpfResult::Neutral => "neutral",
        SpfResult::TempError => "temperror",
        SpfResult::PermError => "permerror",
        SpfResult::None => "none",
    }
}

fn dkim_result_str(result: &DkimResult) -> &'static str {
    match result {
        DkimResult::Pass => "pass",
        DkimResult::Fail(_) => "fail",
        DkimResult::Neutral(_) => "neutral",
        DkimResult::TempError(_) => "temperror",
        DkimResult::PermError(_) => "permerror",
        DkimResult::None => "none",
    }
}

/// Summarizes the per-signature DKIM outputs into a single result: "none"
/// when the message carries no DKIM-Signature header, "pass" if any
/// signature verifies, otherwise the first signature's failure reason.
fn summarize_dkim(outputs: &[DkimOutput]) -> &'static str {
    if outputs.is_empty() {
        return "none";
    }
    if outputs
        .iter()
        .any(|o| matches!(o.result(), DkimResult::Pass))
    {
        return "pass";
    }
    dkim_result_str(outputs[0].result())
}

/// Persists the message for each recipient. Returns `Err` if any recipient's
/// insert fails, so the caller can signal a transient SMTP error rather than
/// falsely acknowledging mail it didn't store. (On partial failure the sender
/// retries the whole message — at-least-once delivery, with possible duplicates
/// for already-stored recipients, which the pluggable store can dedupe on
/// `message_id` if it cares.)
async fn store_message(
    raw: &[u8],
    from: &str,
    recipients: &[String],
    spf_result: String,
    dkim_result: String,
    storage: &Arc<dyn Storage>,
) -> Result<(), MailError> {
    let parsed = mail_parser::MessageParser::default().parse(raw);
    let message_id = parsed
        .as_ref()
        .and_then(|m| m.message_id())
        .map(str::to_string);
    let from_display = parsed
        .as_ref()
        .and_then(|m| m.from())
        .and_then(|a| a.first())
        .and_then(|a| a.name())
        .map(str::to_string);
    let subject = parsed
        .as_ref()
        .and_then(|m| m.subject())
        .map(str::to_string);
    let body_text = parsed
        .as_ref()
        .and_then(|m| m.body_text(0))
        .map(|s| s.to_string());
    let body_html = parsed
        .as_ref()
        .and_then(|m| m.body_html(0))
        .map(|s| s.to_string());

    for rcpt in recipients {
        let msg = RawMessage {
            address: rcpt.clone(),
            envelope_from: from.to_string(),
            envelope_to: rcpt.clone(),
            message_id: message_id.clone(),
            from_display: from_display.clone(),
            subject: subject.clone(),
            body_text: body_text.clone(),
            body_html: body_html.clone(),
            raw_data: raw.to_vec(),
            spf_result: Some(spf_result.clone()),
            dkim_result: Some(dkim_result.clone()),
        };
        let id = storage.save_message(msg).await.map_err(|e| {
            tracing::error!("failed to store message for {}: {}", rcpt, e);
            e
        })?;
        tracing::info!(
            storage_id = id,
            from = %from,
            to = %rcpt,
            subject = ?subject,
            "received message"
        );
    }
    Ok(())
}

async fn write_reply<S>(stream: &mut BufStream<S>, code: u16, text: &str) -> Result<(), MailError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let line = format!("{} {}\r\n", code, text);
    stream
        .write_all(line.as_bytes())
        .await
        .map_err(|e| MailError::Io(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| MailError::Io(e.to_string()))?;
    Ok(())
}

async fn write_ehlo<S>(
    stream: &mut BufStream<S>,
    offer_starttls: bool,
    config: &MailConfig,
) -> Result<(), MailError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut lines = vec![
        format!("250-{}", config.hostname),
        format!("250-SIZE {}", config.max_message_size_bytes),
        "250-8BITMIME".to_string(),
    ];
    if offer_starttls {
        lines.push("250-STARTTLS".to_string());
    }
    let last = lines.len() - 1;
    lines[last] = lines[last].replacen("250-", "250 ", 1);
    let joined = lines.join("\r\n") + "\r\n";
    stream
        .write_all(joined.as_bytes())
        .await
        .map_err(|e| MailError::Io(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| MailError::Io(e.to_string()))?;
    Ok(())
}

/// Upper bound on a single SMTP command line (verb + parameters + CRLF).
/// RFC 5321 §4.5.3.1 caps command lines at 512 octets; we allow extra headroom
/// for long `MAIL`/`RCPT` parameter lists while still bounding the buffer, so a
/// client that never sends a newline can't grow it without limit.
const MAX_COMMAND_LINE_BYTES: usize = 4096;

async fn read_line_timeout<S>(
    stream: &mut BufStream<S>,
    timeout_secs: u64,
) -> Result<String, MailError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut line = Vec::new();
    // `take` bounds the read: at most MAX_COMMAND_LINE_BYTES are buffered, even
    // if the peer never sends a `\n`.
    let n = timeout(
        Duration::from_secs(timeout_secs),
        (&mut *stream)
            .take(MAX_COMMAND_LINE_BYTES as u64)
            .read_until(b'\n', &mut line),
    )
    .await
    .map_err(|_| MailError::ProtocolError("command timed out".to_string()))?
    .map_err(|e| MailError::Io(e.to_string()))?;
    if n == 0 {
        return Err(MailError::ProtocolError("connection closed".to_string()));
    }
    // Hit the byte cap without a terminator: an over-long (or unterminated)
    // command line. Reject rather than keep buffering.
    if !line.ends_with(b"\n") && line.len() >= MAX_COMMAND_LINE_BYTES {
        return Err(MailError::ProtocolError(
            "command line exceeds maximum length".to_string(),
        ));
    }
    String::from_utf8(line)
        .map_err(|_| MailError::ProtocolError("invalid UTF-8 in command".to_string()))
}

/// Reads DATA content until a line containing only ".", undoing dot-stuffing
/// and enforcing `max_size`. Each line read is capped to the remaining size
/// budget, so neither a single never-terminated line nor a flood of lines can
/// buffer more than `max_size` (plus one terminator) in memory — the previous
/// unbounded `read_until` let a newline-less stream grow without limit.
async fn read_data<S>(
    stream: &mut BufStream<S>,
    max_size: usize,
    timeout_secs: u64,
) -> Result<Vec<u8>, MailError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut data = Vec::new();
    loop {
        // Cap this read to what's left of the size budget, with a small floor so
        // the terminating ".<CRLF>" can still be read when the budget is exactly
        // spent. If the cap is hit without a `\n`, the message ran past max_size.
        let remaining = max_size.saturating_sub(data.len());
        let cap = remaining.max(3) as u64 + 1;
        let mut line = Vec::new();
        let n = timeout(
            Duration::from_secs(timeout_secs),
            (&mut *stream).take(cap).read_until(b'\n', &mut line),
        )
        .await
        .map_err(|_| MailError::ProtocolError("timed out reading DATA".to_string()))?
        .map_err(|e| MailError::Io(e.to_string()))?;

        if n == 0 {
            return Err(MailError::ProtocolError(
                "connection closed during DATA".to_string(),
            ));
        }
        if line == b".\r\n" || line == b".\n" {
            break;
        }
        // No terminator within the capped read → the message exceeds max_size.
        if !line.ends_with(b"\n") {
            return Err(MailError::ProtocolError(
                "message exceeds maximum size".to_string(),
            ));
        }
        // RFC 5321 §4.5.2 transparency: a DATA line beginning with '.' had a
        // dot prepended by the sender; strip one leading dot from ANY such line
        // (not only "..") — the lone-"." terminator was already handled above.
        let line = if line.starts_with(b".") {
            line[1..].to_vec()
        } else {
            line
        };
        data.extend_from_slice(&line);
        if data.len() > max_size {
            return Err(MailError::ProtocolError(
                "message exceeds maximum size".to_string(),
            ));
        }
    }
    Ok(data)
}

fn split_command(line: &str) -> (&str, &str) {
    match line.split_once(' ') {
        Some((verb, rest)) => (verb, rest.trim()),
        None => (line, ""),
    }
}

/// Extracts the address from `FROM:<addr>`/`TO:<addr>` style parameters,
/// tolerating the bracket-less form some clients send.
fn extract_address(rest: &str, prefix: &str) -> Option<String> {
    let rest = rest.trim();
    if !rest
        .to_ascii_uppercase()
        .starts_with(&prefix.to_ascii_uppercase())
    {
        return None;
    }
    let after_prefix = rest[prefix.len()..].trim();

    if let Some(start) = after_prefix.find('<') {
        let end = after_prefix[start..].find('>')?;
        // Includes the empty address `<>` — the null reverse-path bounces and
        // DSNs use as their envelope sender. Callers treat "" as the null sender
        // for `MAIL FROM` (accepted) and as an invalid recipient for `RCPT TO`
        // (rejected downstream by the recipient lookup).
        let addr = &after_prefix[start + 1..start + end];
        return Some(addr.to_string());
    }

    let token = after_prefix.split_whitespace().next()?;
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_address_handles_brackets() {
        assert_eq!(
            extract_address("FROM:<someone@example.com>", "FROM:"),
            Some("someone@example.com".to_string())
        );
    }

    #[test]
    fn extract_address_handles_no_brackets() {
        assert_eq!(
            extract_address("TO:someone@example.com", "TO:"),
            Some("someone@example.com".to_string())
        );
    }

    #[test]
    fn extract_address_rejects_wrong_prefix() {
        assert_eq!(extract_address("TO:<someone@example.com>", "FROM:"), None);
    }

    #[test]
    fn extract_address_accepts_null_reverse_path() {
        // `MAIL FROM:<>` is the null sender used by bounces/DSNs — extracted as
        // the empty string, not rejected as a syntax error.
        assert_eq!(extract_address("FROM:<>", "FROM:"), Some(String::new()));
    }

    #[test]
    fn extract_address_rejects_missing_address() {
        // No brackets and nothing after the prefix is malformed, not the null
        // sender.
        assert_eq!(extract_address("FROM:", "FROM:"), None);
    }

    #[test]
    fn split_command_separates_verb_and_rest() {
        assert_eq!(
            split_command("MAIL FROM:<a@b.com>"),
            ("MAIL", "FROM:<a@b.com>")
        );
        assert_eq!(split_command("QUIT"), ("QUIT", ""));
    }

    #[test]
    fn ip_rate_limiter_caps_per_window() {
        use std::net::Ipv4Addr;
        let limiter = IpRateLimiter::new(2, Duration::from_secs(60));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(limiter.allow(ip));
        assert!(limiter.allow(ip));
        assert!(!limiter.allow(ip)); // third within the window is blocked
                                     // a different IP has its own budget
        assert!(limiter.allow(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
    }

    #[test]
    fn ip_rate_limiter_zero_is_unlimited() {
        use std::net::Ipv4Addr;
        let limiter = IpRateLimiter::new(0, Duration::from_secs(60));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        for _ in 0..1000 {
            assert!(limiter.allow(ip));
        }
    }

    // Drives a reader helper against an in-memory duplex pipe: the writer half
    // sends `input` then closes (EOF); the reader half is what the helper reads.
    async fn stream_of(input: Vec<u8>) -> BufStream<tokio::io::DuplexStream> {
        let (mut client, server) = tokio::io::duplex(1 << 20);
        tokio::spawn(async move {
            let _ = client.write_all(&input).await;
            // drop client → EOF for the reader
        });
        BufStream::new(server)
    }

    #[tokio::test]
    async fn read_line_reads_a_normal_command() {
        let mut stream = stream_of(b"EHLO example.com\r\n".to_vec()).await;
        let line = read_line_timeout(&mut stream, 5).await.unwrap();
        assert_eq!(line.trim_end(), "EHLO example.com");
    }

    #[tokio::test]
    async fn read_line_rejects_an_overlong_command() {
        // A single line longer than the cap with no terminator must not be
        // buffered without limit — it's rejected.
        let junk = vec![b'A'; MAX_COMMAND_LINE_BYTES + 100];
        let mut stream = stream_of(junk).await;
        assert!(read_line_timeout(&mut stream, 5).await.is_err());
    }

    #[tokio::test]
    async fn read_data_unstuffs_and_terminates() {
        let mut stream = stream_of(b"Subject: hi\r\n..dotted\r\n.\r\n".to_vec()).await;
        let data = read_data(&mut stream, 1000, 5).await.unwrap();
        assert_eq!(data, b"Subject: hi\r\n.dotted\r\n");
    }

    #[tokio::test]
    async fn read_data_unstuffs_single_and_double_dots() {
        // Per RFC 5321 §4.5.2 a leading dot is stripped from any line: `.line2`
        // -> `line2`, `..line3` -> `.line3`; the lone `.` terminates.
        let mut stream = stream_of(b"line1\r\n.line2\r\n..line3\r\n.\r\n".to_vec()).await;
        let data = read_data(&mut stream, 1000, 5).await.unwrap();
        assert_eq!(data, b"line1\r\nline2\r\n.line3\r\n");
    }

    #[tokio::test]
    async fn read_data_rejects_oversized_unterminated_line() {
        // 500 KB with no newline against a 1 KB limit: the old unbounded
        // read_until would have buffered all of it; now it's capped and errors.
        let junk = vec![b'a'; 500_000];
        let mut stream = stream_of(junk).await;
        assert!(read_data(&mut stream, 1000, 5).await.is_err());
    }

    #[tokio::test]
    async fn read_data_rejects_message_over_size_across_lines() {
        // Many small terminated lines whose total exceeds the limit.
        let mut input = Vec::new();
        for _ in 0..50 {
            input.extend_from_slice(&[b'x'; 100]);
            input.extend_from_slice(b"\r\n");
        }
        input.extend_from_slice(b".\r\n");
        let mut stream = stream_of(input).await;
        assert!(read_data(&mut stream, 1000, 5).await.is_err());
    }

    // ---- session greeting / STARTTLS re-entry (regression for the banner desync) ----

    use crate::sender::{MailSender, OutgoingReply};
    use crate::storage::InMemoryStorage;

    /// Reply sender that does nothing — the greeting/STARTTLS tests never send.
    struct NoopSender;

    #[async_trait::async_trait]
    impl MailSender for NoopSender {
        async fn send(&self, _reply: OutgoingReply) -> Result<(), MailError> {
            Ok(())
        }
    }

    /// Minimal service for driving `run_session` in-memory. `with_sender` reads
    /// the system DNS config for the SPF/DKIM authenticator, so this needs a
    /// readable `/etc/resolv.conf` (present in CI/containers).
    fn test_service() -> MailService {
        MailService::with_sender(
            MailConfig::default(),
            Arc::new(InMemoryStorage::new()),
            Arc::new(NoopSender),
        )
        .expect("failed to build test MailService (needs a readable /etc/resolv.conf)")
    }

    #[tokio::test]
    async fn plain_leg_opens_with_the_220_banner() {
        // The initial (plaintext) connection greets first, before any command.
        let (client, server) = tokio::io::duplex(1 << 16);
        let peer: SocketAddr = "127.0.0.1:2525".parse().unwrap();
        let service = test_service();
        let handle = tokio::spawn(async move { run_session(server, peer, &service, false).await });

        let mut client = BufStream::new(client);
        let mut greeting = String::new();
        client.read_line(&mut greeting).await.unwrap();
        assert!(
            greeting.starts_with("220"),
            "initial connection must open with a 220 banner, got {greeting:?}"
        );

        client.write_all(b"QUIT\r\n").await.unwrap();
        client.flush().await.unwrap();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn starttls_leg_does_not_resend_the_banner() {
        // After STARTTLS, run_session re-enters with already_tls=true. Per RFC
        // 3207 it must NOT re-send a 220 banner: it waits for the fresh EHLO and
        // answers 250. Re-sending 220 desyncs every later reply by one — the bug
        // this guards against (it broke DATA over TLS).
        let (client, server) = tokio::io::duplex(1 << 16);
        let peer: SocketAddr = "127.0.0.1:2525".parse().unwrap();
        let service = test_service();
        let handle = tokio::spawn(async move { run_session(server, peer, &service, true).await });

        let mut client = BufStream::new(client);
        // Client speaks first — no unsolicited banner should arrive.
        client.write_all(b"EHLO test.local\r\n").await.unwrap();
        client.flush().await.unwrap();

        let mut first = String::new();
        client.read_line(&mut first).await.unwrap();
        assert!(
            first.starts_with("250"),
            "post-STARTTLS EHLO must be answered with 250 capabilities, got {first:?}"
        );
        assert!(
            !first.starts_with("220"),
            "server re-sent a 220 banner after STARTTLS — the desync bug, got {first:?}"
        );

        client.write_all(b"QUIT\r\n").await.unwrap();
        client.flush().await.unwrap();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn helo_gets_a_single_line_reply_without_extensions() {
        // RFC 5321 §4.1.1.1: HELO must NOT carry the ESMTP extension list
        // (SIZE/8BITMIME/STARTTLS) — that's EHLO-only. It gets one `250 <host>`
        // line: a space after the code, not a continuation dash.
        let (client, server) = tokio::io::duplex(1 << 16);
        let peer: SocketAddr = "127.0.0.1:2525".parse().unwrap();
        let service = test_service();
        let handle = tokio::spawn(async move { run_session(server, peer, &service, false).await });

        let mut client = BufStream::new(client);
        let mut greeting = String::new();
        client.read_line(&mut greeting).await.unwrap(); // consume the 220 banner

        client.write_all(b"HELO test.local\r\n").await.unwrap();
        client.flush().await.unwrap();

        let mut reply = String::new();
        client.read_line(&mut reply).await.unwrap();
        assert!(
            reply.starts_with("250 ") && !reply.starts_with("250-"),
            "HELO must get a single-line 250 reply with no extensions, got {reply:?}"
        );

        client.write_all(b"QUIT\r\n").await.unwrap();
        client.flush().await.unwrap();
        let _ = handle.await;
    }
}
