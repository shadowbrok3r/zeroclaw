//! Session-scoped access to render A/B experiments, for the app's rating card.
//!
//! The helper owns what an experiment means — which candidate is the baseline,
//! what a pick implies, when the feedback ledger learns about it. The gateway
//! owns paired-device authorization and nothing else, exactly as `session_jobs`
//! does for the job observer.
//!
//! The session segment in the path carries AUTHORIZATION, not filtering:
//! experiments live in the comfy agent's workspace and are not owned by a chat
//! session, so every paired session sees the same list. That is deliberate —
//! the person rating a render on their phone is the same person whichever
//! session they happen to have open.
use crate::{AppState, session_jobs};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::path::Path as FsPath;

fn error(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "private, no-store")],
        Json(json!({"error":message})),
    )
        .into_response()
}

fn json_response(value: Value) -> Response {
    ([(header::CACHE_CONTROL, "private, no-store")], Json(value)).into_response()
}

/// An experiment name is a directory name under the configured experiment root.
/// Dots are allowed because names come from directories, but never a name that
/// IS a traversal, and never a separator — the helper resolves it against the
/// root by exact match, and this keeps a crafted name from reaching it at all.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name != "."
        && name != ".."
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}

/// Fixed argument arrays; never a shell and never a caller-selected executable.
async fn invoke(args: &[String]) -> Result<Value, Response> {
    let Some(exe) = std::env::var_os("ZEROCLAW_COMFY_EXPERIMENTS") else {
        return Err(error(
            StatusCode::NOT_IMPLEMENTED,
            "Render experiments are not enabled on this gateway",
        ));
    };
    if !FsPath::new(&exe).is_absolute() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Render experiment helper must be an absolute path",
        ));
    }
    session_jobs::run_args(FsPath::new(&exe), args)
        .await
        .map_err(|message| error(StatusCode::BAD_GATEWAY, &message))
}

pub(crate) async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(status) = session_jobs::authorize(&state, &headers, &id) {
        return status.into_response();
    }
    match invoke(&["list".to_owned()]).await {
        Ok(value) if value["experiments"].is_array() => json_response(value),
        Ok(_) => error(
            StatusCode::BAD_GATEWAY,
            "Render experiment helper returned an unsupported response",
        ),
        Err(response) => response,
    }
}

/// The helper's arguments for one tap: a winner, one verdict for both
/// candidates (`keep` rates them equal, `reject` rates both bad), or a clear.
fn pick_args(name: String, body: &Value) -> Result<Vec<String>, &'static str> {
    let mut args = vec!["pick".to_owned(), name];
    match (body.get("winner"), body["verdict"].as_str()) {
        (Some(winner), _) => match winner.as_u64() {
            Some(index @ (0 | 1)) => {
                args.push("--winner".to_owned());
                args.push(index.to_string());
            }
            _ => return Err("winner must be 0 or 1"),
        },
        (None, Some("unrated")) => args.push("--clear".to_owned()),
        (None, Some(verdict @ ("keep" | "reject"))) => {
            args.push("--both".to_owned());
            args.push(verdict.to_owned());
        }
        (None, _) => {
            return Err(
                "send {\"winner\":0|1} to pick one, {\"verdict\":\"keep\"|\"reject\"} to rate both alike, or {\"verdict\":\"unrated\"} to clear",
            );
        }
    }
    if let Some(notes) = body["notes"].as_str().filter(|n| !n.is_empty()) {
        args.push("--notes".to_owned());
        args.push(notes.chars().take(1800).collect());
    }
    Ok(args)
}

/// One whole tap: winner kept and loser rejected in a single call, both
/// candidates given one verdict, or both cleared. Two requests from a phone on
/// a flaky connection is how an experiment ends up half-rated, and a half-rated
/// comparison reads to the promotion gate as a conflict that blocks the trial
/// rather than advancing it.
pub(crate) async fn pick(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(status) = session_jobs::authorize(&state, &headers, &session) {
        return status.into_response();
    }
    if !valid_name(&name) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let args = match pick_args(name, &body) {
        Ok(args) => args,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    match invoke(&args).await {
        Ok(value) => json_response(value),
        Err(response) => response,
    }
}

#[cfg(test)]
mod tests {
    use super::{pick_args, valid_name};
    use serde_json::json;

    #[test]
    fn a_tap_becomes_one_fixed_helper_call() {
        let args = |body| pick_args("e".to_owned(), &body);
        assert_eq!(
            args(json!({"winner": 1})).unwrap(),
            ["pick", "e", "--winner", "1"]
        );
        assert_eq!(
            args(json!({"verdict": "unrated"})).unwrap(),
            ["pick", "e", "--clear"]
        );
        assert_eq!(
            args(json!({"verdict": "keep"})).unwrap(),
            ["pick", "e", "--both", "keep"]
        );
        assert_eq!(
            args(json!({"verdict": "reject"})).unwrap(),
            ["pick", "e", "--both", "reject"]
        );
        assert_eq!(
            args(json!({"winner": 0, "notes": "sharper"})).unwrap(),
            ["pick", "e", "--winner", "0", "--notes", "sharper"]
        );
        for bad in [
            json!({"winner": 2}),
            json!({"winner": "1"}),
            json!({"verdict": "maybe"}),
            json!({"verdict": "--clear"}),
            json!({}),
            json!([]),
        ] {
            assert!(args(bad.clone()).is_err(), "{bad} should be refused");
        }
    }

    #[test]
    fn names_are_directory_names_and_never_traversals() {
        assert!(valid_name("landscape_cfg_discovery"));
        assert!(valid_name("cfg-2.0_holdout"));
        for bad in [
            "",
            ".",
            "..",
            ".hidden",
            "../escape",
            "a/b",
            "a\\b",
            "name with space",
            "naïve",
        ] {
            assert!(!valid_name(bad), "{bad} should be rejected");
        }
        assert!(!valid_name(&"x".repeat(129)));
        assert!(valid_name(&"x".repeat(128)));
    }
}
