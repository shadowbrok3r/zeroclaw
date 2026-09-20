//! Session-scoped access to the installed Comfy job observer. The observer's durable receipts
//! own job identity and outputs; the gateway owns paired-device/session authorization.
use crate::{AppState, api};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{path::Path as FsPath, process::Stdio, time::Duration};
use tokio::io::AsyncReadExt;

const MAX_REPLY: usize = 2 * 1024 * 1024;

pub(crate) fn authorize(
    state: &AppState,
    headers: &HeaderMap,
    id: &str,
) -> Result<String, StatusCode> {
    let paired = api::extract_bearer_token(headers).is_some_and(|token| {
        !token.is_empty()
            && state
                .pairing
                .tokens()
                .contains(&zeroclaw_runtime::security::pairing::PairingGuard::token_hash(token))
    });
    if !paired {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let backend = state
        .session_backend
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?;
    let key = zeroclaw_infra::session_backend::resolve_session_key(backend.as_ref(), id)
        .ok_or(StatusCode::NOT_FOUND)?;
    if api::session_hidden_from_device(state, headers, &key) {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(key)
}

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
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Whether the observer is wired in at all (`ZEROCLAW_COMFY_GEN` in the service environment).
pub(crate) fn enabled() -> bool {
    std::env::var_os("ZEROCLAW_COMFY_GEN").is_some()
}

/// One render output the receipts can vouch for: an existing absolute file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Deliverable {
    pub job: String,
    pub index: u64,
    pub label: String,
    pub kind: String,
    pub path: String,
    pub source: String,
}

/// Renders this session began at or after `since`, newest first, from
/// `comfy-gen where --deliverable`. Used by `render_delivery` at turn end.
pub(crate) async fn deliverables(session: &str, since: u64) -> Result<Vec<Deliverable>, String> {
    let Some(exe) = std::env::var_os("ZEROCLAW_COMFY_GEN") else {
        return Err("Comfy job tracking is not enabled on this gateway".into());
    };
    let exe = std::path::PathBuf::from(exe);
    if !exe.is_absolute() {
        return Err("Comfy job tracking executable must be an absolute path".into());
    }
    let args = [
        "where",
        "--session",
        session,
        "--since",
        &since.to_string(),
        "--latest",
        "64",
        "--deliverable",
    ]
    .map(str::to_owned);
    let value = run_args(&exe, &args).await?;
    deliverables_from(&value, session)
}

/// Validate a `where --deliverable` reply: the receipts are trusted for identity,
/// but every path still has to be absolute and clean before it becomes a marker.
pub(crate) fn deliverables_from(value: &Value, session: &str) -> Result<Vec<Deliverable>, String> {
    if value["version"] != 1 || value["session_id"].as_str() != Some(session) {
        return Err(
            "Comfy job observer returned a different session or unsupported response".into(),
        );
    }
    let results = value["results"]
        .as_array()
        .ok_or("Comfy job observer returned no results array")?;
    let mut out = Vec::new();
    for r in results {
        let Some(path) = r["deliver_path"].as_str() else {
            continue;
        };
        let p = FsPath::new(path);
        if !p.is_absolute()
            || p.components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            continue;
        }
        let job = r["job"].as_str().unwrap_or_default();
        let index = r["index"].as_u64().unwrap_or(u64::MAX);
        if !valid_id(job) || index >= 64 {
            continue;
        }
        out.push(Deliverable {
            job: job.to_owned(),
            index,
            label: r["label"].as_str().unwrap_or_default().to_owned(),
            kind: r["kind"].as_str().unwrap_or("image").to_owned(),
            path: path.to_owned(),
            source: r["deliver_source"].as_str().unwrap_or_default().to_owned(),
        });
    }
    // `where` sorts newest first; a reply reads better oldest first.
    out.reverse();
    Ok(out)
}

/// Fixed subcommands and argument arrays; never a shell or a caller-selected executable.
async fn invoke(
    command: &str,
    session: &str,
    job: Option<&str>,
    index: Option<usize>,
) -> Result<Value, Response> {
    let Some(exe) = std::env::var_os("ZEROCLAW_COMFY_GEN") else {
        return Err(error(
            StatusCode::NOT_IMPLEMENTED,
            "Comfy job tracking is not enabled on this gateway",
        ));
    };
    if !FsPath::new(&exe).is_absolute() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Comfy job tracking executable must be an absolute path",
        ));
    }
    run(FsPath::new(&exe), command, session, job, index)
        .await
        .map_err(|message| error(StatusCode::BAD_GATEWAY, &message))
}

async fn run(
    exe: &FsPath,
    command: &str,
    session: &str,
    job: Option<&str>,
    index: Option<usize>,
) -> Result<Value, String> {
    let mut args = vec![
        command.to_owned(),
        "--session".to_owned(),
        session.to_owned(),
    ];
    if let Some(id) = job {
        args.push("--job".to_owned());
        args.push(id.to_owned());
    }
    if let Some(i) = index {
        args.push("--index".to_owned());
        args.push(i.to_string());
    }
    run_args(exe, &args).await
}

/// Shared with `session_experiments`: the same no-shell, capped-output,
/// JSON-on-stdout contract serves both helpers.
pub(crate) async fn run_args(exe: &FsPath, args: &[String]) -> Result<Value, String> {
    let mut cmd = tokio::process::Command::new(exe);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Comfy job observer could not start: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or("Comfy job observer has no stdout")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("Comfy job observer has no stderr")?;
    let result = tokio::time::timeout(Duration::from_secs(12), async {
        async fn bounded(
            stream: impl tokio::io::AsyncRead + Unpin,
            limit: usize,
        ) -> std::io::Result<Vec<u8>> {
            let mut data = Vec::new();
            stream
                .take((limit + 1) as u64)
                .read_to_end(&mut data)
                .await?;
            if data.len() > limit {
                return Err(std::io::Error::other(
                    "Comfy job response exceeded its size limit",
                ));
            }
            Ok(data)
        }
        tokio::try_join!(
            bounded(stdout, MAX_REPLY),
            bounded(stderr, 8192),
            child.wait()
        )
    })
    .await
    .map_err(|_| "Comfy job observer timed out; refresh to check the outcome".to_string())?
    .map_err(|e| format!("Comfy job observer I/O failed: {e}"))?;
    let (out, err, status) = result;
    if out.len() > MAX_REPLY {
        return Err("Comfy job response exceeded its size limit".into());
    }
    if !status.success() {
        let reason = String::from_utf8_lossy(&err);
        return Err(format!(
            "Comfy job observer: {}",
            reason.chars().take(2000).collect::<String>().trim()
        ));
    }
    serde_json::from_slice(&out).map_err(|_| "Comfy job observer returned invalid JSON".into())
}

pub(crate) async fn list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let key = match authorize(&state, &headers, &id) {
        Ok(k) => k,
        Err(status) => return status.into_response(),
    };
    match invoke("jobs", &key, None, None).await {
        Ok(value)
            if value["version"] == 1
                && value["session_id"].as_str() == Some(&key)
                && value["jobs"].is_array() =>
        {
            json_response(value)
        }
        Ok(_) => error(
            StatusCode::BAD_GATEWAY,
            "Comfy job observer returned a different session or unsupported response",
        ),
        Err(response) => response,
    }
}
pub(crate) async fn cancel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session, id)): Path<(String, String)>,
) -> Response {
    let key = match authorize(&state, &headers, &session) {
        Ok(k) => k,
        Err(status) => return status.into_response(),
    };
    if !valid_id(&id) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match invoke("job-cancel", &key, Some(&id), None).await {
        Ok(value) => json_response(value),
        Err(response) => response,
    }
}
pub(crate) async fn output(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((session, id, index)): Path<(String, String, usize)>,
) -> Response {
    let key = match authorize(&state, &headers, &session) {
        Ok(k) => k,
        Err(status) => return status.into_response(),
    };
    if !valid_id(&id) || index >= 64 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let value = match invoke("job-output", &key, Some(&id), Some(index)).await {
        Ok(v) => v,
        Err(response) => return response,
    };
    let Some(path) = value["path"].as_str().map(str::to_owned) else {
        return error(StatusCode::BAD_GATEWAY, "Job output path is missing");
    };
    match tokio::task::spawn_blocking(move || crate::session_media::read_image(&path)).await {
        Ok(Ok((mime, bytes))) => (
            [
                (header::CONTENT_TYPE, mime),
                (header::CACHE_CONTROL, "private, no-store"),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            bytes,
        )
            .into_response(),
        Ok(Err(status)) => error(status, "Saved job image is unavailable"),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Job image could not be read",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use zeroclaw_runtime::security::pairing::PairingGuard;

    fn state() -> (tempfile::TempDir, AppState) {
        let temp = tempfile::tempdir().unwrap();
        let backend = zeroclaw_infra::make_session_backend(temp.path(), "sqlite").unwrap();
        backend
            .append(
                "gw_ui-jobs",
                &zeroclaw_providers::ChatMessage::assistant(
                    "Job tracking test fixture. No messages are sent to an AI.",
                ),
            )
            .unwrap();
        backend
            .set_session_agent_alias("gw_ui-jobs", "comfy")
            .unwrap();
        backend
            .set_session_origin_principal(
                "gw_ui-jobs",
                &format!("device:{}", PairingGuard::token_hash("fixture-owner")),
            )
            .unwrap();
        let mut state = api::test_state(Default::default());
        state.session_backend = Some(backend);
        state.pairing = Arc::new(PairingGuard::new(
            true,
            &["fixture-owner".into(), "fixture-other".into()],
        ));
        state.config.write().gateway.scope_sessions_to_device = true;
        (temp, state)
    }
    fn headers(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        h
    }
    #[test]
    fn authorization_resolves_canonical_session_and_enforces_device_visibility() {
        let (_temp, mut state) = state();
        assert_eq!(
            authorize(&state, &headers("fixture-owner"), "ui-jobs").unwrap(),
            "gw_ui-jobs"
        );
        assert_eq!(
            authorize(&state, &headers("fixture-other"), "ui-jobs"),
            Err(StatusCode::NOT_FOUND)
        );
        assert_eq!(
            authorize(&state, &headers("fixture-owner"), "missing"),
            Err(StatusCode::NOT_FOUND)
        );
        state.pairing = Arc::new(PairingGuard::new(false, &["fixture-owner".into()]));
        assert_eq!(
            authorize(&state, &HeaderMap::new(), "ui-jobs"),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            authorize(&state, &headers("unpaired"), "ui-jobs"),
            Err(StatusCode::UNAUTHORIZED)
        );
    }
    #[tokio::test]
    async fn every_handler_authorizes_before_invoking_the_observer() {
        let (_temp, state) = state();
        assert_eq!(
            list(
                State(state.clone()),
                headers("fixture-other"),
                Path("ui-jobs".into())
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            cancel(
                State(state.clone()),
                HeaderMap::new(),
                Path(("ui-jobs".into(), "a-job".into()))
            )
            .await
            .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            output(
                State(state.clone()),
                headers("fixture-other"),
                Path(("ui-jobs".into(), "a-job".into(), 0))
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            cancel(
                State(state.clone()),
                headers("fixture-owner"),
                Path(("ui-jobs".into(), "../escape".into()))
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            output(
                State(state),
                headers("fixture-owner"),
                Path(("ui-jobs".into(), "a-job".into(), 64))
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }
    #[cfg(unix)]
    fn executable(dir: &FsPath, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join("observer");
        std::fs::write(&path, format!("#!/usr/bin/python3\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        path
    }
    #[tokio::test]
    #[cfg(unix)]
    async fn subprocess_arguments_are_literal_and_output_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let exe = executable(
            temp.path(),
            "import json,sys; print(json.dumps(sys.argv[1:]))",
        );
        let args = run(
            &exe,
            "job-cancel",
            "gw_$(exit 42); session",
            Some("job-one"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            args,
            json!([
                "job-cancel",
                "--session",
                "gw_$(exit 42); session",
                "--job",
                "job-one"
            ])
        );
        let exe = executable(temp.path(), "import sys; sys.stdout.write('x' * 4000000)");
        assert!(
            run(&exe, "jobs", "gw_test", None, None)
                .await
                .unwrap_err()
                .contains("size limit")
        );
        let exe = executable(temp.path(), "print('invalid json')");
        assert!(
            run(&exe, "jobs", "gw_test", None, None)
                .await
                .unwrap_err()
                .contains("invalid JSON")
        );
        let exe = executable(
            temp.path(),
            "import sys; sys.stderr.write('targeted cancellation unsupported'); sys.exit(1)",
        );
        assert!(
            run(&exe, "job-cancel", "gw_test", Some("job-one"), None)
                .await
                .unwrap_err()
                .contains("targeted cancellation unsupported")
        );
    }

    /// Local-only Android fixture: real job REST handlers + a chat socket that cannot run tools.
    /// Start comfy_jobs_smoke.py --serve first and pass its environment here. Never uses a GPU.
    #[test]
    fn deliverables_keep_only_clean_absolute_files_and_read_oldest_first() {
        let value = json!({"version":1,"session_id":"gw_s","matched":4,"results":[
            {"job":"cg-2","index":0,"label":"cg_two","kind":"image","deliver_source":"cache","deliver_path":"/records/x/outputs/cg-2-0.png"},
            {"job":"cg-1","index":0,"label":"cg_one","kind":"image","deliver_source":"gallery","deliver_path":"/gallery/a/cg_one_1_00001_.png"},
            {"job":"cg-0","index":0,"label":"cg_none","kind":"image","deliver_source":null,"deliver_path":null},
            {"job":"../x","index":0,"label":"bad","kind":"image","deliver_source":"cache","deliver_path":"/records/../etc/passwd"},
            {"job":"cg-3","index":99,"label":"bad","kind":"image","deliver_source":"cache","deliver_path":"/records/x/outputs/cg-3-99.png"},
            {"job":"cg-4","index":0,"label":"rel","kind":"image","deliver_source":"workspace","deliver_path":"renders/cg_rel.png"}
        ]});
        let got = deliverables_from(&value, "gw_s").unwrap();
        assert_eq!(
            got.iter().map(|d| d.path.as_str()).collect::<Vec<_>>(),
            vec![
                "/gallery/a/cg_one_1_00001_.png",
                "/records/x/outputs/cg-2-0.png"
            ]
        );
        assert_eq!(got[0].source, "gallery");
        assert_eq!(got[1].label, "cg_two");
        assert!(deliverables_from(&value, "gw_other").is_err());
        assert!(
            deliverables_from(
                &json!({"version":2,"session_id":"gw_s","results":[]}),
                "gw_s"
            )
            .is_err()
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn deliverables_invoke_where_with_a_window_and_delivery_mode() {
        let temp = tempfile::tempdir().unwrap();
        let exe = executable(
            temp.path(),
            "import json,sys; print(json.dumps({'version':1,'session_id':sys.argv[3],'argv':sys.argv[1:],'results':[]}))",
        );
        let args = [
            "where",
            "--session",
            "gw_s",
            "--since",
            "1789535400",
            "--latest",
            "64",
            "--deliverable",
        ]
        .map(str::to_owned);
        let value = run_args(&exe, &args).await.unwrap();
        assert_eq!(
            value["argv"],
            json!([
                "where",
                "--session",
                "gw_s",
                "--since",
                "1789535400",
                "--latest",
                "64",
                "--deliverable"
            ])
        );
        assert_eq!(deliverables_from(&value, "gw_s").unwrap(), vec![]);
    }

    #[tokio::test]
    #[ignore = "manual Android fixture; requires the local comfy_jobs_smoke.py --serve environment"]
    async fn serve_android_jobs_fixture() {
        use axum::{
            extract::{Query, WebSocketUpgrade, ws::Message},
            routing::{get, post},
        };
        assert!(
            std::env::var_os("CG_JOBS_DIR").is_some(),
            "start the smoke fixture first"
        );
        async fn socket(
            Query(q): Query<std::collections::HashMap<String, String>>,
            ws: WebSocketUpgrade,
        ) -> Response {
            if q.get("token").map(String::as_str) != Some("fixture-owner") {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            ws.on_upgrade(|mut ws| async move {
                let _ = ws
                    .send(Message::Text(
                        json!({"type":"session_start","session_id":"ui-jobs"})
                            .to_string()
                            .into(),
                    ))
                    .await;
                while let Some(Ok(message)) = ws.recv().await {
                    match message {
                        Message::Ping(data) => {
                            let _ = ws.send(Message::Pong(data)).await;
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
            })
            .into_response()
        }
        let (_temp, state) = state();
        let app = axum::Router::new()
            .route("/ws/chat", get(socket))
            .route("/api/sessions", get(api::handle_api_sessions_list))
            .route(
                "/api/sessions/{id}/messages",
                get(api::handle_api_session_messages),
            )
            .route(
                "/api/sessions/{id}/state",
                get(api::handle_api_session_state),
            )
            .route("/api/sessions/{id}/jobs", get(list))
            .route("/api/sessions/{id}/jobs/{job}/cancel", post(cancel))
            .route("/api/sessions/{id}/jobs/{job}/outputs/{index}", get(output))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:42619")
            .await
            .unwrap();
        println!("ANDROID_JOBS_FIXTURE_READY 127.0.0.1:42619 session=ui-jobs token=fixture-owner");
        axum::serve(listener, app).await.unwrap();
    }
}
