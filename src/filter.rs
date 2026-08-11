//! Lightweight, opt-in spam/abuse filters that reject at SMTP time rather than
//! analyzing message content. Everything here is cheap: the greylist is a small
//! bounded in-memory map, the HELO check is pure string work, and the DNSBL
//! check is a (resolver-cached) DNS lookup per zone. No models, no corpora, no
//! per-message allocation of significance — see `MailConfig` for the knobs that
//! turn each one on.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mail_auth::common::cache::NoCache;
use mail_auth::{MessageAuthenticator, RecordSet};

/// Returns the first DNSBL zone that lists `ip`, or `None` when it's listed in
/// none of `zones`. A zone lists an IP when `<reversed-ip>.<zone>` resolves to
/// any A record (DNSBLs conventionally answer in 127.0.0.0/8); an NXDOMAIN or
/// any lookup error is treated as "not listed" so a flaky blocklist can't wedge
/// the mail flow. IPv6 peers are looked up under the nibble-reversed form.
pub async fn dnsbl_listed(
    authenticator: &MessageAuthenticator,
    ip: IpAddr,
    zones: &[String],
) -> Option<String> {
    let reversed = reverse_ip(ip);
    for zone in zones {
        let query = format!("{}.{}", reversed, zone.trim_end_matches('.'));
        match authenticator
            .ipv4_lookup(
                query.as_str(),
                None::<&NoCache<Box<str>, RecordSet<Ipv4Addr>>>,
            )
            .await
        {
            Ok(records) if !records.rrset.is_empty() => return Some(zone.clone()),
            _ => {}
        }
    }
    None
}

/// Formats `ip` in the reversed form DNSBLs (and PTR) use as the DNS label:
/// dotted reverse octets for IPv4, nibble-reversed hex for IPv6.
pub fn reverse_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            format!("{}.{}.{}.{}", o[3], o[2], o[1], o[0])
        }
        IpAddr::V6(v6) => {
            // Full 32 nibbles, least-significant first, each as its own label.
            let mut out = String::with_capacity(63);
            for byte in v6.octets().iter().rev() {
                out.push(nibble_hex(byte & 0x0f));
                out.push('.');
                out.push(nibble_hex(byte >> 4));
                out.push('.');
            }
            out.pop(); // trailing '.'
            out
        }
    }
}

fn nibble_hex(n: u8) -> char {
    char::from_digit(n as u32, 16).expect("nibble is always 0..=15")
}

/// Whether a `HELO`/`EHLO` argument is syntactically plausible. This is a pure
/// syntax gate (no DNS): it rejects an absent argument, a bare IP without
/// brackets, and anything that isn't domain-shaped, while accepting bracketed
/// address literals (`[192.0.2.1]`, `[IPv6:...]`) as RFC 5321 requires.
pub fn valid_helo(domain: &str) -> bool {
    if domain.is_empty() {
        return false;
    }
    // Address literal, e.g. "[192.0.2.1]" or "[IPv6:2001:db8::1]".
    if domain.starts_with('[') && domain.ends_with(']') {
        return true;
    }
    // A bare IP with no brackets is not a valid HELO name.
    if domain.parse::<IpAddr>().is_ok() {
        return false;
    }
    // Otherwise require something domain-shaped: a dot and no internal spaces.
    domain.contains('.') && !domain.chars().any(char::is_whitespace)
}

/// In-memory greylist keyed by the (client IP, envelope sender, recipient)
/// triplet. The first contact for a triplet is deferred; the sender is admitted
/// once it retries after `delay` — legitimate MTAs retry, most spam bots don't.
///
/// State lives only in memory: it resets on restart (worst case, one extra
/// deferral per triplet afterward) and is not shared across processes, which
/// suits a single small instance. The map is bounded by pruning idle triplets.
pub struct Greylist {
    delay: Duration,
    entries: Mutex<HashMap<(IpAddr, String, String), Entry>>,
}

struct Entry {
    first_seen: Instant,
    last_seen: Instant,
}

impl Greylist {
    /// Idle triplets older than this are pruned (and would be greylisted
    /// afresh). Long enough that an active correspondent is never re-deferred.
    const RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
    /// Only prune once the map grows past this, to bound memory without walking
    /// it on every message.
    const MAX_ENTRIES: usize = 100_000;

    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Records an attempt for the triplet and returns whether it's admitted.
    /// Admitted once at least `delay` has elapsed since the triplet was first
    /// seen. A zero delay admits everything (greylisting disabled).
    pub fn admit(&self, ip: IpAddr, mail_from: &str, rcpt: &str) -> bool {
        if self.delay.is_zero() {
            return true;
        }
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap();
        if entries.len() > Self::MAX_ENTRIES {
            entries.retain(|_, e| now.duration_since(e.last_seen) < Self::RETENTION);
        }
        let entry = entries
            .entry((ip, mail_from.to_string(), rcpt.to_string()))
            .or_insert(Entry {
                first_seen: now,
                last_seen: now,
            });
        entry.last_seen = now;
        now.duration_since(entry.first_seen) >= self.delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn reverse_ipv4_reverses_octets() {
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        assert_eq!(reverse_ip(ip), "4.3.2.1");
    }

    #[test]
    fn reverse_ipv6_is_nibble_reversed() {
        // ::1 -> loopback: all-zero except the final nibble is 1.
        let ip = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let expected = format!("1{}", ".0".repeat(31));
        assert_eq!(reverse_ip(ip), expected);
    }

    #[test]
    fn valid_helo_accepts_domains_and_literals() {
        assert!(valid_helo("mail.example.com"));
        assert!(valid_helo("[192.0.2.1]"));
        assert!(valid_helo("[IPv6:2001:db8::1]"));
    }

    #[test]
    fn valid_helo_rejects_junk() {
        assert!(!valid_helo("")); // absent
        assert!(!valid_helo("192.0.2.1")); // bare IP, no brackets
        assert!(!valid_helo("localhost")); // no dot, not a literal
    }

    #[test]
    fn greylist_defers_first_then_admits_after_delay() {
        let gl = Greylist::new(Duration::from_millis(30));
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5));
        // First contact is deferred.
        assert!(!gl.admit(ip, "a@example.com", "me@here.test"));
        // A retry before the delay elapses is still deferred.
        assert!(!gl.admit(ip, "a@example.com", "me@here.test"));
        std::thread::sleep(Duration::from_millis(40));
        // After the delay, the same triplet is admitted.
        assert!(gl.admit(ip, "a@example.com", "me@here.test"));
    }

    #[test]
    fn greylist_zero_delay_admits_immediately() {
        let gl = Greylist::new(Duration::ZERO);
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(gl.admit(ip, "a@example.com", "me@here.test"));
    }

    #[test]
    fn greylist_tracks_triplets_independently() {
        let gl = Greylist::new(Duration::from_secs(60));
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5));
        assert!(!gl.admit(ip, "a@example.com", "me@here.test"));
        // A different recipient is its own triplet — deferred on its own first
        // contact, not admitted by the first triplet's record.
        assert!(!gl.admit(ip, "a@example.com", "other@here.test"));
    }
}
