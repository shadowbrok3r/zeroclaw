//! The phone's Films cards: casting sheets Logan keeps or drops women from, the women he kept, and
//! the films he orders (one of his women, a place, one to three acts in order) for the nightly
//! film pipeline.
//!
//! The helper (`ZEROCLAW_FILM_CARDS`, comfy-gate `director/cards.py`) owns what all of that means
//! and prints the whole snapshot after every call, so a phone that taps once on a flaky
//! connection simply replaces its state with the answer. The gateway owns paired-device
//! authorization and argument hygiene and nothing else, exactly as `session_experiments` does
//! for the rating card. The session segment authorizes; it does not filter — the films are
//! Logan's, whichever session he has open.
use crate::{AppState, session_jobs};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::path::Path as FsPath;

const MAX_ACTS: usize = 3;
const MAX_PLACE: usize = 240;
const MAX_NOTES: usize = 600;

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

/// A sheet, order, woman or act id: the helper's own shapes (`casting-20261124-004100`,
/// `o-20261124-004100`, `mara`, `footjob-a`), checked before any of them reaches its argv.
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && !id.starts_with('-')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Free text from the phone (a place, notes) as one line, bounded.
fn line(value: &Value, limit: usize) -> Option<String> {
    let text = value
        .as_str()?
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    Some(text.chars().take(limit).collect())
}

/// Fixed argument arrays; never a shell and never a caller-selected executable. A helper that
/// exits non-zero explains itself on stderr ("Ivy is not one of the women you kept"): that is
/// the phone's message. Anything else is the gateway's problem, said as such.
#[allow(clippy::result_large_err)]
async fn invoke(args: &[String]) -> Result<Value, Response> {
    let Some(exe) = std::env::var_os("ZEROCLAW_FILM_CARDS") else {
        return Err(error(
            StatusCode::NOT_IMPLEMENTED,
            "Film cards are not enabled on this gateway",
        ));
    };
    if !FsPath::new(&exe).is_absolute() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Film card helper must be an absolute path",
        ));
    }
    match session_jobs::run_args(FsPath::new(&exe), args).await {
        Ok(value) if value["tonight"].is_object() => Ok(value),
        Ok(_) => Err(error(
            StatusCode::BAD_GATEWAY,
            "Film card helper returned an unsupported response",
        )),
        Err(message) => match message.strip_prefix("Comfy job observer: ") {
            Some(reason) if !reason.is_empty() => Err(error(StatusCode::BAD_REQUEST, reason)),
            _ => Err(error(
                StatusCode::BAD_GATEWAY,
                &message.replacen("Comfy job observer", "Film card helper", 1),
            )),
        },
    }
}

async fn answer(
    state: &AppState,
    headers: &HeaderMap,
    session: &str,
    args: Result<Vec<String>, &str>,
) -> Response {
    if let Err(status) = session_jobs::authorize(state, headers, session) {
        return status.into_response();
    }
    let args = match args {
        Ok(args) => args,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    match invoke(&args).await {
        Ok(value) => json_response(value),
        Err(response) => response,
    }
}

pub(crate) async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    answer(&state, &headers, &id, Ok(vec!["list".to_owned()])).await
}

/// A new casting call; the helper draws it in the background and answers at once.
pub(crate) async fn cast(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    answer(&state, &headers, &id, Ok(vec!["cast".to_owned()])).await
}

/// `{"index": n, "verdict": "keep"|"drop"|"unrated"}` for one woman of a sheet.
fn pick_args(sheet: &str, body: &Value) -> Result<Vec<String>, &'static str> {
    if !valid_id(sheet) {
        return Err("no such casting sheet");
    }
    let index = body["index"]
        .as_u64()
        .filter(|i| *i < 64)
        .ok_or("send {\"index\": n, \"verdict\": \"keep\"|\"drop\"|\"unrated\"}")?;
    let verdict = match body["verdict"].as_str() {
        Some(v @ ("keep" | "drop" | "unrated")) => v,
        _ => return Err("verdict must be keep, drop or unrated"),
    };
    Ok(vec![
        "keep".to_owned(),
        sheet.to_owned(),
        format!("--index={index}"),
        format!("--verdict={verdict}"),
    ])
}

pub(crate) async fn pick(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, sheet)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Response {
    answer(&state, &headers, &id, pick_args(&sheet, &body)).await
}

/// `{"woman": slug, "place": text, "acts": [key, ...], "notes": text}`. Every value goes in as
/// `--name=value`, so nothing he types can be read as another option.
fn order_args(body: &Value) -> Result<Vec<String>, &'static str> {
    let woman = body["woman"]
        .as_str()
        .filter(|w| valid_id(w))
        .ok_or("pick one of your women")?;
    let place = line(&body["place"], MAX_PLACE)
        .filter(|p| p.chars().count() >= 3)
        .ok_or("say where it happens")?;
    let acts: Vec<&str> = body["acts"]
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if acts.is_empty()
        || acts.len() > MAX_ACTS
        || acts.len() != body["acts"].as_array().map_or(0, Vec::len)
    {
        return Err("pick one to three acts");
    }
    if !acts.iter().all(|a| valid_id(a)) {
        return Err("an act is not on the menu");
    }
    let mut args = vec![
        "order".to_owned(),
        format!("--woman={woman}"),
        format!("--place={place}"),
    ];
    args.extend(acts.iter().map(|a| format!("--act={a}")));
    if let Some(notes) = line(&body["notes"], MAX_NOTES).filter(|n| !n.is_empty()) {
        args.push(format!("--notes={notes}"));
    }
    Ok(args)
}

pub(crate) async fn order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    answer(&state, &headers, &id, order_args(&body)).await
}

pub(crate) async fn cancel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, order)): Path<(String, String)>,
) -> Response {
    let args = if valid_id(&order) {
        Ok(vec!["cancel".to_owned(), order])
    } else {
        Err("no such order")
    };
    answer(&state, &headers, &id, args).await
}

#[cfg(test)]
mod tests {
    use super::{order_args, pick_args, valid_id};
    use serde_json::json;

    #[test]
    fn a_keep_or_drop_is_one_fixed_helper_call() {
        assert_eq!(
            pick_args(
                "casting-20261124-004100",
                &json!({"index": 2, "verdict": "keep"})
            )
            .unwrap(),
            [
                "keep",
                "casting-20261124-004100",
                "--index=2",
                "--verdict=keep"
            ]
        );
        assert!(pick_args("casting-1", &json!({"index": 2, "verdict": "maybe"})).is_err());
        assert!(pick_args("casting-1", &json!({"verdict": "keep"})).is_err());
        assert!(pick_args("../roster", &json!({"index": 0, "verdict": "keep"})).is_err());
        assert!(pick_args("--help", &json!({"index": 0, "verdict": "keep"})).is_err());
    }

    #[test]
    fn an_order_cannot_smuggle_options_or_run_long() {
        let args = order_args(&json!({"woman": "mara", "place": "  a sunlit\n greenhouse ", "acts": ["footjob-a", "riding"],
                                      "notes": "--force slow"}))
        .unwrap();
        assert_eq!(
            args,
            [
                "order",
                "--woman=mara",
                "--place=a sunlit greenhouse",
                "--act=footjob-a",
                "--act=riding",
                "--notes=--force slow"
            ]
        );
        let long = "x".repeat(900);
        let args =
            order_args(&json!({"woman": "mara", "place": long, "acts": ["riding"]})).unwrap();
        assert_eq!(args[2].len(), "--place=".len() + 240);
        for bad in [
            json!({"place": "a bed", "acts": ["riding"]}),
            json!({"woman": "-x", "place": "a bed", "acts": ["riding"]}),
            json!({"woman": "mara", "place": "ab", "acts": ["riding"]}),
            json!({"woman": "mara", "place": "a bed", "acts": []}),
            json!({"woman": "mara", "place": "a bed", "acts": ["a", "b", "c", "d"]}),
            json!({"woman": "mara", "place": "a bed", "acts": ["riding", 3]}),
            json!({"woman": "mara", "place": "a bed", "acts": ["../x"]}),
        ] {
            assert!(order_args(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn ids_are_the_helpers_own_shapes() {
        for good in [
            "casting-20261124-004100",
            "o-20261124-004100",
            "mara",
            "footjob-a",
            "the_freckled_woman",
        ] {
            assert!(valid_id(good), "{good}");
        }
        for bad in ["", "-x", "a/b", "..", "a b", &"x".repeat(65)] {
            assert!(!valid_id(bad), "{bad}");
        }
    }
}
