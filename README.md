# axum-email-server

A framework-agnostic Rust library for receiving and replying to SMTP mail via a
pluggable store, with address-based routing, SPF/DKIM verification, and reply
sending. Structured the same way as
[`axum-email-otp-auth`](https://github.com/roksprogar/axum-email-otp-auth): a
core service you can use standalone, plus an optional `axum` feature exposing
ready-made HTTP handlers.

It is an **inbound MTA**: it accepts mail on SMTP for the addresses you've
configured as active and stores it — it is not an open relay and never forwards
mail. Replies are sent out separately, through an SMTP relay you configure.

The crate ships the `Storage` trait, an in-memory implementation
(`InMemoryStorage`) for tests, and — behind the optional `postgres` feature — a
batteries-included `PostgresStorage`, the same relationship `axum-email-otp-auth`
has with its `RedisStorage`.

## Features

Both are off by default:

- **`postgres`** — a ready-to-use `PostgresStorage` implementing `Storage`
  against the `mail_domains` / `mail_addresses` / `mail_messages` /
  `mail_attachments` / `mail_replies` schema (below). The consuming app owns the
  migration that creates the schema and hands in a `sqlx::PgPool`; the crate
  supplies all the routing, read, reply, and address-management queries.
- **`axum`** — a ready-made Axum router (`axum_api::mail_router`) for the
  read/reply HTTP API, to nest under your own prefix and protect with your own
  auth middleware.

## Usage

Enable the backends you want:

```toml
[dependencies]
axum-email-server = { version = "0.3", features = ["postgres", "axum"] }
```

### In-memory (tests)

```rust
use axum_email_server::{InMemoryStorage, MailConfig, MailService, Storage};
use std::sync::Arc;

let config = MailConfig::from_env().expect("failed to load mail config");
let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
let service = MailService::new(config, storage).expect("failed to initialize mail service");
```

### Postgres (`postgres` feature)

Bring your own `PgPool` (and apply the schema migration); the crate provides the
`Storage` impl. `receive::run` is the SMTP loop and runs until the listener
errors — spawn it alongside your HTTP server rather than awaiting it inline:

```rust,ignore
use axum_email_server::{MailConfig, MailService, PostgresStorage, Storage};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

let pool = PgPoolOptions::new().connect(&database_url).await?;
let config = MailConfig::from_env()?;
let storage: Arc<dyn Storage> = Arc::new(PostgresStorage::new(pool));
let service = MailService::new(config, storage)?;

tokio::spawn(axum_email_server::receive::run(service.clone()));
```

## Configuration

`MailConfig::from_env()` reads the variables below, or build `MailConfig`
directly — every field is public and `Default` supplies the values shown.
`MAIL_STORAGE_TYPE` / `DATABASE_URL` are hints for *you* to pick a `Storage` and
build the `PgPool`; the rest drive the receive/reply logic.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MAIL_STORAGE_TYPE` | `memory` | `memory` or `postgres` — which store you construct |
| `DATABASE_URL` | — | required when `MAIL_STORAGE_TYPE=postgres` |
| `SMTP_BIND_ADDR` | `0.0.0.0:25` | address the SMTP receive loop binds |
| `MAIL_HOSTNAME` | `mail.example.com` | this server's identity, used in SPF checks |
| `TLS_CERT_PATH` / `TLS_KEY_PATH` | `/etc/mail/certs/fullchain.pem` / `privkey.pem` | STARTTLS cert/key, re-read per connection |
| `SMTP_RELAY_HOST` / `SMTP_RELAY_PORT` | `localhost` / `587` | relay used to send replies |
| `SMTP_RELAY_USERNAME` / `SMTP_RELAY_PASSWORD` | — | relay creds; empty username → connect in the clear with no auth (e.g. a local mailpit), otherwise STARTTLS + auth |
| `MAX_MESSAGE_SIZE_BYTES` | `26214400` (25 MiB) | largest message accepted |
| `MAX_CONCURRENT_CONNECTIONS` | `50` | connections served at once |
| `COMMAND_TIMEOUT_SECONDS` | `30` | per-command read timeout |
| `SESSION_TIMEOUT_SECONDS` | `300` | max session duration |
| `MAX_RECIPIENTS_PER_MESSAGE` | `10` | `RCPT TO` cap per message |
| `MAX_CONNECTIONS_PER_IP_PER_MINUTE` | `60` | new connections accepted per remote IP per minute (`0` = unlimited) |

## Postgres schema (`postgres` feature)

`PostgresStorage` runs against exactly these tables — you own the migration that
creates them:

```sql
CREATE TABLE mail_domains (
    id          BIGSERIAL PRIMARY KEY,
    domain      TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE mail_addresses (
    id          BIGSERIAL PRIMARY KEY,
    domain_id   BIGINT NOT NULL REFERENCES mail_domains(id),
    local_part  TEXT NOT NULL,
    label       TEXT,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (domain_id, local_part)
);

CREATE TABLE mail_messages (
    id             BIGSERIAL PRIMARY KEY,
    address_id     BIGINT NOT NULL REFERENCES mail_addresses(id),
    message_id     TEXT,
    envelope_from  TEXT NOT NULL,
    envelope_to    TEXT NOT NULL,
    reply_to       TEXT,
    from_display   TEXT,
    subject        TEXT,
    body_text      TEXT,
    body_html      TEXT,
    raw_mime       BYTEA NOT NULL,
    spf_result     TEXT,
    dkim_result    TEXT,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at        TIMESTAMPTZ,
    replied_at     TIMESTAMPTZ
);

CREATE TABLE mail_attachments (
    id            BIGSERIAL PRIMARY KEY,
    message_id    BIGINT NOT NULL REFERENCES mail_messages(id),
    filename      TEXT,
    content_type  TEXT,
    size_bytes    INTEGER,
    storage_path  TEXT
);

CREATE TABLE mail_replies (
    id               BIGSERIAL PRIMARY KEY,
    message_id       BIGINT NOT NULL REFERENCES mail_messages(id),
    sent_by          TEXT,
    body_text        TEXT,
    body_html        TEXT,
    sent_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery_status  TEXT
);
```

`mail_domains` / `mail_addresses` are the routing table: only addresses present
here with `active = true` are accepted at `RCPT TO`; everything else is rejected
before `DATA`. Add them through `Storage::create_address` (or the `axum`
feature's `POST /addresses`) — a fresh store accepts no mail until an address
exists.

## HTTP API (`axum` feature)

`axum_api::mail_router()` returns a `Router<MailService>`. Nest it under your own
prefix and layer your auth on top:

```rust,ignore
let mail = axum_email_server::axum_api::mail_router()
    .route_layer(/* your auth middleware */)
    .with_state(service);
let app = axum::Router::new().nest("/api/mail", mail);
```

Routes, relative to your prefix:

| Method & path | Action |
| --- | --- |
| `GET /messages` | list (query: `domain`, `address`, `unread`, `limit`, `offset`) |
| `GET /messages/:id` | one message with attachments + replies |
| `POST /messages/:id/read` | mark read |
| `POST /messages/:id/reply` | send a reply (`body_text` and/or `body_html`) |
| `DELETE /threads/:id` | delete a message with its replies/attachments |
| `GET /addresses` · `POST /addresses` | list · add a routing entry |
| `POST /addresses/:id/active` | activate/deactivate an address |
| `DELETE /addresses/:id` | purge an address and all its mail |

To record who sent a reply, insert an `Operator(String)` request extension from
your auth middleware; it is stored as `sent_by` (absent → recorded with no
author). Replies are delivered by `MailService::send_reply`, which the reply
route wraps — call it directly if you don't use the `axum` feature.

## Pluggable reply delivery

Reply sending goes through a `MailSender`. `MailService::new` uses the default
`LettreMailSender` (SMTP to your configured relay); `MailService::with_sender`
takes any `Arc<dyn MailSender>` instead, so you can swap the transport or inject
a test double and exercise the reply path without a live relay.

## Behavior notes

- **Inbound only.** Mail is accepted for configured, active addresses and
  stored; the crate never relays or forwards. Replies go out through the SMTP
  relay in your config.
- **SPF/DKIM are recorded, not enforced.** Per-message `spf_result` /
  `dkim_result` are stored for you to act on; failing mail is still accepted.
  Verification uses the system DNS resolver (`/etc/resolv.conf`), so
  `MailService::new` fails if that can't be read.
- **STARTTLS is opportunistic.** It's advertised only when the cert/key exist at
  the configured paths, and the cert is re-read per connection so renewals need
  no restart; without them the session stays plaintext.
- **Failed persistence is never silently accepted.** If the store errors after
  `DATA`, the session replies `451` so the sending MTA retries, instead of
  acknowledging mail it didn't keep.

## License

MIT — see [LICENSE](LICENSE).
