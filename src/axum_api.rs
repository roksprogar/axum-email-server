//! Ready-made Axum router for the read/reply HTTP API. Enabled by the `axum`
//! feature. Mirrors `axum-email-otp-auth`'s `axum_api` module: the crate
//! ships the handlers and route table; the consuming app supplies state and
//! layers its own auth on top.
//!
//! The router's state is [`MailService`], which carries both the storage and
//! the reply-sending config. Mount it under a prefix (e.g. `/api/mail`) and
//! protect it with your own auth middleware:
//!
//! ```ignore
//! let mail = axum_email_server::axum_api::mail_router()
//!     .route_layer(axum::middleware::from_fn_with_state(app_state, auth_middleware))
//!     .with_state(mail_service);
//! let app = axum::Router::new().nest("/api/mail", mail);
//! ```

use crate::errors::MailError;
use crate::service::MailService;
use crate::storage::{
    AddressInfo, ConversationSummary, ConversationThread, MessageFilter, MessageSummary,
    StoredMessage,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Extension, Json, Router,
};
use serde::{Deserialize, Serialize};

/// Operator identity for the `sent_by` field on replies. The consuming app's
/// auth middleware inserts this as a request extension; when absent, replies
/// are recorded with no author.
#[derive(Clone, Debug)]
pub struct Operator(pub String);

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub domain: Option<String>,
    pub address: Option<String>,
    #[serde(default)]
    pub unread: bool,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ReplyRequest {
    pub body_text: Option<String>,
    pub body_html: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReplyResponse {
    pub reply_id: i64,
}

/// The read/reply routes, stated on [`MailService`]. Paths are relative — nest
/// this under whatever prefix the app uses (e.g. `/api/mail`).
pub fn mail_router() -> Router<MailService> {
    Router::new()
        .route("/messages", get(list_messages))
        .route("/messages/:id", get(get_message))
        .route("/messages/:id/read", post(mark_read))
        .route("/messages/:id/reply", post(reply))
        .route("/threads/:id", delete(delete_thread))
        .route("/conversations", get(list_conversations))
        .route(
            "/conversations/:id",
            get(get_conversation).delete(delete_conversation),
        )
        .route("/conversations/:id/read", post(mark_conversation_read))
        .route("/addresses", get(list_addresses).post(create_address))
        .route("/addresses/:id", delete(delete_address))
        .route("/addresses/:id/active", post(set_address_active))
}

async fn list_messages(
    State(service): State<MailService>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<MessageSummary>>, ApiError> {
    let filter = MessageFilter {
        domain: q.domain,
        address: q.address,
        unread_only: q.unread,
        limit: q.limit,
        offset: q.offset.unwrap_or(0),
    };
    Ok(Json(service.storage.list_messages(&filter).await?))
}

async fn get_message(
    State(service): State<MailService>,
    Path(id): Path<i64>,
) -> Result<Json<StoredMessage>, ApiError> {
    service
        .storage
        .get_message(id)
        .await?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

async fn mark_read(
    State(service): State<MailService>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    service.storage.mark_read(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_thread(
    State(service): State<MailService>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    service.storage.delete_thread(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_conversations(
    State(service): State<MailService>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<ConversationSummary>>, ApiError> {
    let filter = MessageFilter {
        domain: q.domain,
        address: q.address,
        unread_only: q.unread,
        limit: q.limit,
        offset: q.offset.unwrap_or(0),
    };
    Ok(Json(service.storage.list_conversations(&filter).await?))
}

async fn get_conversation(
    State(service): State<MailService>,
    Path(id): Path<i64>,
) -> Result<Json<ConversationThread>, ApiError> {
    service
        .storage
        .get_conversation(id)
        .await?
        .map(Json)
        .ok_or(ApiError::NotFound)
}

async fn mark_conversation_read(
    State(service): State<MailService>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    service.storage.mark_conversation_read(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_conversation(
    State(service): State<MailService>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    service.storage.delete_conversation(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn reply(
    State(service): State<MailService>,
    operator: Option<Extension<Operator>>,
    Path(id): Path<i64>,
    Json(body): Json<ReplyRequest>,
) -> Result<Json<ReplyResponse>, ApiError> {
    if body.body_text.is_none() && body.body_html.is_none() {
        return Err(ApiError::BadRequest("reply body required".to_string()));
    }
    let sent_by = operator.map(|Extension(Operator(who))| who);
    let reply_id = service
        .send_reply(id, body.body_text, body.body_html, sent_by)
        .await?;
    Ok(Json(ReplyResponse { reply_id }))
}

#[derive(Debug, Deserialize)]
pub struct CreateAddressRequest {
    /// Full `local_part@domain` to accept mail for.
    pub address: String,
    pub label: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SetAddressActiveRequest {
    pub active: bool,
}

async fn list_addresses(
    State(service): State<MailService>,
) -> Result<Json<Vec<AddressInfo>>, ApiError> {
    Ok(Json(service.storage.list_addresses().await?))
}

async fn create_address(
    State(service): State<MailService>,
    Json(body): Json<CreateAddressRequest>,
) -> Result<Json<AddressInfo>, ApiError> {
    let (local_part, domain) = parse_address(&body.address)?;
    let label = body
        .label
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty());
    Ok(Json(
        service
            .storage
            .create_address(&local_part, &domain, label)
            .await?,
    ))
}

async fn set_address_active(
    State(service): State<MailService>,
    Path(id): Path<i64>,
    Json(body): Json<SetAddressActiveRequest>,
) -> Result<StatusCode, ApiError> {
    service.storage.set_address_active(id, body.active).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_address(
    State(service): State<MailService>,
    Path(id): Path<i64>,
) -> Result<StatusCode, ApiError> {
    service.storage.delete_address(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Validate a full address and split it into lowercased `(local_part, domain)`.
/// Deliberately conservative — a single `@`, non-empty sides, a dotted domain,
/// and no internal whitespace — since these become `RCPT TO` routing entries.
fn parse_address(address: &str) -> Result<(String, String), ApiError> {
    let trimmed = address.trim();
    let (local, domain) = trimmed
        .rsplit_once('@')
        .ok_or_else(|| ApiError::BadRequest("address must be local@domain".to_string()))?;
    if local.is_empty()
        || domain.is_empty()
        || !domain.contains('.')
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(ApiError::BadRequest(format!("invalid address: {address}")));
    }
    Ok((local.to_lowercase(), domain.to_lowercase()))
}

/// HTTP-facing error for the mail API.
pub enum ApiError {
    NotFound,
    BadRequest(String),
    Mail(MailError),
}

impl From<MailError> for ApiError {
    fn from(e: MailError) -> Self {
        ApiError::Mail(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            // Reply delivery is an upstream (relay) failure, not the caller's fault.
            ApiError::Mail(MailError::ReplyError(msg)) => {
                tracing::error!("reply delivery failed: {msg}");
                (StatusCode::BAD_GATEWAY, "reply delivery failed").into_response()
            }
            ApiError::Mail(e) => {
                tracing::error!("mail api error: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}
