//! Read-only images explicitly referenced by persisted assistant/tool output in a visible session.
//! The transcript owns the allowlist; there is no unauthenticated or arbitrary-file route.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use std::io::Read;

use crate::{AppState, api};

const MAX_IMAGE_BYTES: u64 = 24 * 1024 * 1024;

#[derive(Deserialize)]
pub(crate) struct MediaQuery {
    path: String,
}

pub(crate) async fn read(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<MediaQuery>,
) -> Response {
    // Unlike the general dashboard, file delivery always requires an actual paired credential,
    // even if an operator disabled pairing requirements for a local dashboard.
    if api::extract_bearer_token(&headers).is_none_or(|token| {
        let hash = zeroclaw_runtime::security::pairing::PairingGuard::token_hash(token);
        token.is_empty() || !state.pairing.tokens().contains(&hash)
    }) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(backend) = state.session_backend.as_ref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(key) = zeroclaw_infra::session_backend::resolve_session_key(backend.as_ref(), &id)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if api::session_hidden_from_device(&state, &headers, &key) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = std::path::Path::new(&query.path);
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let marker = format!("[IMAGE:{}]", query.path);
    let allowed = backend.load_with_timestamps(&key).iter().any(|row| {
        matches!(row.message.role.as_str(), "assistant" | "tool")
            && references_image(&row.message.content, &marker)
    });
    if !allowed {
        return StatusCode::NOT_FOUND.into_response();
    }
    match tokio::task::spawn_blocking(move || read_image(&query.path)).await {
        Ok(Ok((mime, bytes))) => (
            [
                (header::CONTENT_TYPE, mime),
                (header::CACHE_CONTROL, "private, no-store"),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            bytes,
        )
            .into_response(),
        Ok(Err(StatusCode::NOT_FOUND)) => (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error":"image_file_unavailable"})),
        )
            .into_response(),
        Ok(Err(status)) => status.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn references_image(content: &str, marker: &str) -> bool {
    fn visit(value: &serde_json::Value, marker: &str) -> bool {
        match value {
            serde_json::Value::String(text) => text.contains(marker),
            serde_json::Value::Array(items) => items.iter().any(|v| visit(v, marker)),
            serde_json::Value::Object(fields) => fields.values().any(|v| visit(v, marker)),
            _ => false,
        }
    }
    match serde_json::from_str::<serde_json::Value>(content) {
        Ok(value) => visit(&value, marker),
        Err(_) => content.contains(marker),
    }
}

pub(crate) fn read_image(path: &str) -> Result<(&'static str, Vec<u8>), StatusCode> {
    let path = std::path::Path::new(path);
    // Reject symlinks and non-regular files before opening; paths cannot redirect the allowlist.
    let canonical = path.canonicalize().map_err(|_| StatusCode::NOT_FOUND)?;
    if canonical != path {
        return Err(StatusCode::NOT_FOUND);
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| StatusCode::NOT_FOUND)?;
    if !metadata.is_file() {
        return Err(StatusCode::NOT_FOUND);
    }
    if metadata.len() > MAX_IMAGE_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| StatusCode::NOT_FOUND)?
        .take(MAX_IMAGE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if bytes.len() as u64 > MAX_IMAGE_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    let mime = zeroclaw_api::media::image_mime_from_magic(&bytes)
        .filter(|mime| {
            matches!(
                *mime,
                "image/png" | "image/jpeg" | "image/webp" | "image/gif"
            )
        })
        .ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE)?;
    Ok((mime, bytes))
}
