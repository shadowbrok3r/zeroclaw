//! Read/write endpoints for per-agent personality markdown files
//! (`SOUL.md`, `IDENTITY.md`, `USER.md`, `AGENTS.md`, `TOOLS.md`,
//! `HEARTBEAT.md`, `MEMORY.md`), plus any other existing `.md` file in the
//! agent's workspace, addressed by its relative path (`OWNER.md`,
//! `notes/plan.md`).

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use zeroclaw_runtime::agent::personality::{EDITABLE_PERSONALITY_FILES, MAX_FILE_CHARS};
use zeroclaw_runtime::agent::personality_templates::{TemplateContext, render_preset_default};
use zeroclaw_runtime::rpc::types::{
    PersonalityFileEntry, PersonalityGetResult, PersonalityListResult, PersonalityPutResult,
    PersonalityTemplatesResult, TemplateFileEntry,
};

use super::AppState;
use super::api::require_auth;

// ── HTTP-specific request/response shapes (not shared) ──────────────

#[derive(Debug, Deserialize, Default)]
pub struct AgentQuery {
    #[serde(default)]
    pub agent: Option<String>,
    /// Index only: also list every other `.md` file in the workspace.
    #[serde(default)]
    pub all: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct TemplateQuery {
    #[serde(default)]
    pub preset: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub user_name: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub communication_style: Option<String>,
    #[serde(default)]
    pub include_memory: Option<bool>,
    #[serde(default)]
    pub agent: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PersonalityPutBody {
    pub content: String,
    #[serde(default)]
    pub expected_mtime_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct PersonalityConflict {
    pub error: &'static str,
    pub filename: String,
    pub current_content: String,
    pub current_mtime_ms: Option<i64>,
}

// ── Sandbox helpers ─────────────────────────────────────────────────

/// Route for one file; the wildcard admits relative paths with `/`.
pub(crate) const FILE_ROUTE: &str = "/api/personality/{*filename}";

/// Directories never listed or served, besides hidden ones.
const SKIPPED_DIRS: &[&str] = &["node_modules", "target", "__pycache__"];
/// Deepest folder below the workspace root that is listed or served.
const MAX_WORKSPACE_DEPTH: usize = 8;
/// Most extra files one index lists.
const MAX_WORKSPACE_FILES: usize = 2_000;

/// A personality request's file, before it is resolved against a workspace.
#[derive(Debug, PartialEq, Eq)]
enum Target {
    /// An allowlisted file at the workspace root; it may not exist yet.
    Allowlisted(&'static str),
    /// Any other existing `.md` file in the workspace, by relative path.
    Workspace(String),
}

impl Target {
    fn name(&self) -> &str {
        match self {
            Self::Allowlisted(name) => name,
            Self::Workspace(path) => path,
        }
    }
}

fn has_markdown_extension(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("md"))
}

/// A `/`-separated relative `.md` path with no hidden, skipped, empty or
/// parent components.
fn is_workspace_markdown_path(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    path.len() <= 1024
        && parts.len() <= MAX_WORKSPACE_DEPTH + 1
        && parts.iter().all(|part| {
            !part.is_empty()
                && !part.starts_with('.')
                && !part.contains(['\\', '\0'])
                && !SKIPPED_DIRS.contains(part)
        })
        && parts
            .last()
            .is_some_and(|name| has_markdown_extension(name))
}

fn validate_filename(filename: &str) -> Result<Target, (StatusCode, Json<serde_json::Value>)> {
    if let Some(allowed) = EDITABLE_PERSONALITY_FILES
        .iter()
        .copied()
        .find(|allowed| *allowed == filename)
    {
        return Ok(Target::Allowlisted(allowed));
    }
    if is_workspace_markdown_path(filename) {
        return Ok(Target::Workspace(filename.to_string()));
    }
    Err((
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "error": "filename is neither in the personality allowlist nor a workspace Markdown path",
            "filename": filename,
            "allowed": EDITABLE_PERSONALITY_FILES,
        })),
    ))
}

/// Resolve an existing workspace file, following symlinks only while they
/// stay inside the workspace.
fn resolve_workspace_file(workspace_dir: &Path, relative: &str) -> std::io::Result<PathBuf> {
    let root = workspace_dir.canonicalize()?;
    let path = root.join(relative).canonicalize()?;
    if !path.starts_with(&root) || !std::fs::metadata(&path)?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not a file inside the workspace",
        ));
    }
    Ok(path)
}

/// Resolve a target to its path on disk; a missing workspace file is a 404.
fn target_path(
    workspace_dir: &Path,
    target: &Target,
) -> Result<PathBuf, (StatusCode, Json<serde_json::Value>)> {
    match target {
        Target::Allowlisted(name) => Ok(workspace_dir.join(name)),
        Target::Workspace(relative) => {
            resolve_workspace_file(workspace_dir, relative).map_err(|_| {
                (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": "personality file not found in the agent workspace",
                        "filename": relative,
                    })),
                )
            })
        }
    }
}

/// Every non-allowlisted `.md` file in the workspace, shallowest first.
/// Hidden entries, [`SKIPPED_DIRS`] and symlinks are not listed. The flag is
/// false when [`MAX_WORKSPACE_FILES`] cut the listing short.
fn list_workspace_markdown(workspace_dir: &Path) -> (Vec<PersonalityFileEntry>, bool) {
    let mut files = Vec::new();
    let mut complete = true;
    let mut dirs = vec![(workspace_dir.to_path_buf(), String::new(), 0usize)];
    'walk: while let Some((dir, prefix, depth)) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            let relative = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            if name.starts_with('.') || SKIPPED_DIRS.contains(&name.as_str()) {
                continue;
            }
            if kind.is_dir() {
                if depth < MAX_WORKSPACE_DEPTH {
                    dirs.push((entry.path(), relative, depth + 1));
                }
                continue;
            }
            if !kind.is_file()
                || !is_workspace_markdown_path(&relative)
                || (depth == 0 && EDITABLE_PERSONALITY_FILES.contains(&name.as_str()))
            {
                continue;
            }
            if files.len() == MAX_WORKSPACE_FILES {
                complete = false;
                break 'walk;
            }
            let meta = entry.metadata().ok();
            files.push(PersonalityFileEntry {
                filename: relative,
                exists: true,
                size: meta.as_ref().map(|m| m.len()).unwrap_or(0),
                mtime_ms: meta.as_ref().and_then(mtime_ms_of),
            });
        }
    }
    files.sort_by(|a, b| {
        (a.filename.matches('/').count(), &a.filename)
            .cmp(&(b.filename.matches('/').count(), &b.filename))
    });
    (files, complete)
}

/// Resolve the per-agent workspace directory for personality I/O. Returns
/// an error response when `agent` is missing or unknown so callers can
/// short-circuit before touching disk.
fn resolve_agent_workspace(
    state: &AppState,
    agent: Option<&str>,
) -> Result<PathBuf, (StatusCode, Json<serde_json::Value>)> {
    let Some(alias) = agent.map(str::trim).filter(|s| !s.is_empty()) else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "missing required `agent` query parameter",
            })),
        ));
    };
    let cfg = state.config.read();
    if !cfg.agents.contains_key(alias) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "unknown agent alias",
                "agent": alias,
            })),
        ));
    }
    Ok(cfg.agent_workspace_dir(alias))
}

fn mtime_ms_of(meta: &std::fs::Metadata) -> Option<i64> {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .and_then(|d| i64::try_from(d.as_millis()).ok())
}

fn truncate_to_chars(content: &str, max: usize) -> (String, bool) {
    if content.chars().count() <= max {
        return (content.to_string(), false);
    }
    let cut = content
        .char_indices()
        .nth(max)
        .map(|(idx, _)| &content[..idx])
        .unwrap_or(content);
    (cut.to_string(), true)
}

// ── Handlers ────────────────────────────────────────────────────────

/// GET /api/personality?agent=`<alias>` — index of all allowlist files in the
/// named agent's workspace. With `&all=true` it also lists every other `.md`
/// file there by relative path, and reports `complete: false` when the
/// listing was capped.
pub async fn handle_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<AgentQuery>,
) -> impl IntoResponse {
    if let Err(e) = require_auth(&state, &headers) {
        return e.into_response();
    }

    let workspace_dir = match resolve_agent_workspace(&state, q.agent.as_deref()) {
        Ok(p) => p,
        Err(resp) => return resp.into_response(),
    };

    let files: Vec<PersonalityFileEntry> = EDITABLE_PERSONALITY_FILES
        .iter()
        .copied()
        .map(|filename| {
            let path = workspace_dir.join(filename);
            match std::fs::metadata(&path) {
                Ok(meta) => PersonalityFileEntry {
                    filename: filename.to_string(),
                    exists: meta.is_file(),
                    size: meta.len(),
                    mtime_ms: mtime_ms_of(&meta),
                },
                Err(_) => PersonalityFileEntry {
                    filename: filename.to_string(),
                    exists: false,
                    size: 0,
                    mtime_ms: None,
                },
            }
        })
        .collect();

    if !q.all {
        return Json(PersonalityListResult {
            files,
            max_chars: MAX_FILE_CHARS,
        })
        .into_response();
    }
    let (extra, complete) = list_workspace_markdown(&workspace_dir);
    let mut files = files;
    files.extend(extra);
    Json(serde_json::json!({
        "files": files,
        "max_chars": MAX_FILE_CHARS,
        "complete": complete,
    }))
    .into_response()
}

/// GET /api/personality/{*filename} — read one file's full content.
pub async fn handle_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(filename): axum::extract::Path<String>,
    Query(q): Query<AgentQuery>,
) -> impl IntoResponse {
    if let Err(e) = require_auth(&state, &headers) {
        return e.into_response();
    }

    let target = match validate_filename(&filename) {
        Ok(f) => f,
        Err(e) => return e.into_response(),
    };

    let workspace_dir = match resolve_agent_workspace(&state, q.agent.as_deref()) {
        Ok(p) => p,
        Err(resp) => return resp.into_response(),
    };
    let path = match target_path(&workspace_dir, &target) {
        Ok(p) => p,
        Err(resp) => return resp.into_response(),
    };
    let allowed = target.name();

    match std::fs::read_to_string(&path) {
        Ok(raw) => {
            let (content, truncated) = truncate_to_chars(&raw, MAX_FILE_CHARS);
            let mtime_ms = std::fs::metadata(&path).ok().and_then(|m| mtime_ms_of(&m));
            Json(PersonalityGetResult {
                filename: allowed.to_string(),
                content: Some(content),
                exists: true,
                truncated,
                mtime_ms,
            })
            .into_response()
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Json(PersonalityGetResult {
            filename: allowed.to_string(),
            content: Some(String::new()),
            exists: false,
            truncated: false,
            mtime_ms: None,
        })
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "failed to read personality file",
                "filename": allowed,
                "detail": err.to_string(),
            })),
        )
            .into_response(),
    }
}

/// PUT /api/personality/{*filename} — overwrite the file. A non-allowlisted
/// workspace file must already exist and needs `expected_mtime_ms`.
pub async fn handle_put(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(filename): axum::extract::Path<String>,
    Query(q): Query<AgentQuery>,
    Json(body): Json<PersonalityPutBody>,
) -> impl IntoResponse {
    if let Err(e) = require_auth(&state, &headers) {
        return e.into_response();
    }

    let target = match validate_filename(&filename) {
        Ok(f) => f,
        Err(e) => return e.into_response(),
    };
    let allowed = target.name();

    if matches!(target, Target::Workspace(_)) && body.expected_mtime_ms.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "expected_mtime_ms is required to save a workspace file",
                "filename": allowed,
            })),
        )
            .into_response();
    }

    if body.content.chars().count() > MAX_FILE_CHARS {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "error": "content exceeds MAX_FILE_CHARS",
                "max_chars": MAX_FILE_CHARS,
            })),
        )
            .into_response();
    }

    let workspace_dir = match resolve_agent_workspace(&state, q.agent.as_deref()) {
        Ok(p) => p,
        Err(resp) => return resp.into_response(),
    };
    let path = match target_path(&workspace_dir, &target) {
        Ok(p) => p,
        Err(resp) => return resp.into_response(),
    };

    // Disk-drift guard: if the editor told us what mtime it saw, reject
    // the write when disk has moved since.
    if let Some(expected) = body.expected_mtime_ms {
        let current = std::fs::metadata(&path).ok().and_then(|m| mtime_ms_of(&m));
        if current != Some(expected) {
            let current_content = std::fs::read_to_string(&path).unwrap_or_default();
            return (
                StatusCode::CONFLICT,
                Json(PersonalityConflict {
                    error: "personality_disk_drift",
                    filename: allowed.to_string(),
                    current_content,
                    current_mtime_ms: current,
                }),
            )
                .into_response();
        }
    }

    if let Some(parent) = path.parent()
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "failed to create workspace dir",
                "detail": err.to_string(),
            })),
        )
            .into_response();
    }

    if let Err(err) = std::fs::write(&path, body.content.as_bytes()) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "failed to write personality file",
                "filename": allowed,
                "detail": err.to_string(),
            })),
        )
            .into_response();
    }

    let meta = std::fs::metadata(&path).ok();
    let bytes_written = meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let mtime_ms = meta.as_ref().and_then(mtime_ms_of);

    Json(PersonalityPutResult {
        bytes_written,
        mtime_ms,
    })
    .into_response()
}

pub async fn handle_templates(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TemplateQuery>,
) -> impl IntoResponse {
    if let Err(e) = require_auth(&state, &headers) {
        return e.into_response();
    }

    let (memory_default_enabled, agent_display_default) = {
        let cfg = state.config.read();
        let mem = cfg.memory.backend.as_str() != "none";
        let display = q
            .agent
            .as_deref()
            .map(str::to_string)
            .filter(|alias| cfg.agents.contains_key(alias));
        (mem, display)
    };

    let defaults = TemplateContext::default();
    let ctx = TemplateContext {
        agent: q
            .agent_name
            .or(agent_display_default)
            .unwrap_or(defaults.agent),
        user: q.user_name.unwrap_or(defaults.user),
        timezone: q.timezone.unwrap_or(defaults.timezone),
        communication_style: q
            .communication_style
            .unwrap_or(defaults.communication_style),
        include_memory: q.include_memory.unwrap_or(memory_default_enabled),
    };

    let files = render_preset_default(&ctx)
        .into_iter()
        .map(|(filename, content)| TemplateFileEntry {
            filename: filename.to_string(),
            content,
        })
        .collect();

    Json(PersonalityTemplatesResult {
        preset: "default".to_string(),
        files,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_filename_accepts_allowlist() {
        for f in EDITABLE_PERSONALITY_FILES {
            assert!(validate_filename(f).is_ok());
        }
    }

    #[test]
    fn validate_filename_admits_workspace_markdown_paths_only() {
        assert_eq!(
            validate_filename("SOUL.md").unwrap(),
            Target::Allowlisted("SOUL.md")
        );
        for ok in [
            "OWNER.md",
            "identity.md",
            "queue/2026-10-24-sofa/report.md",
            "notes/Plan.MD",
        ] {
            assert_eq!(
                validate_filename(ok).unwrap(),
                Target::Workspace(ok.to_string())
            );
        }
        let deep = format!("{}x.md", "d/".repeat(MAX_WORKSPACE_DEPTH + 1));
        for bad in [
            "../etc/passwd",
            "../OWNER.md",
            "notes/../SOUL.md",
            "./OWNER.md",
            "IDENTITY.md/foo",
            ".prev-20260920/SOUL.md",
            ".hidden.md",
            "/abs.md",
            "notes//plan.md",
            "notes/",
            "node_modules/pkg/README.md",
            "target/doc.md",
            "a\\b.md",
            "nul\0.md",
            "notes.txt",
            "",
            deep.as_str(),
        ] {
            assert!(validate_filename(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn allowlisted_targets_join_the_workspace_root_even_when_missing() {
        let p = target_path(Path::new("/tmp/ws"), &Target::Allowlisted("SOUL.md")).unwrap();
        assert_eq!(p, Path::new("/tmp/ws/SOUL.md"));
        let missing = target_path(Path::new("/tmp/ws"), &Target::Workspace("OWNER.md".into()));
        assert_eq!(missing.unwrap_err().0, StatusCode::NOT_FOUND);
    }

    #[cfg(unix)]
    fn workspace() -> (tempfile::TempDir, tempfile::TempDir) {
        use std::os::unix::fs::symlink;
        let ws = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let root = ws.path();
        for (path, body) in [
            ("SOUL.md", "soul"),
            ("OWNER.md", "owner"),
            ("readme.txt", "text"),
            ("notes/plan.md", "plan"),
            ("notes/deep/x.MD", "deep"),
            ("sub/SOUL.md", "nested soul"),
            (".prev-20260920/SOUL.md", "backup"),
            ("node_modules/pkg/README.md", "dependency"),
        ] {
            let file = root.join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, body).unwrap();
        }
        std::fs::write(outside.path().join("secret.md"), "secret").unwrap();
        symlink(outside.path().join("secret.md"), root.join("escape.md")).unwrap();
        symlink(outside.path(), root.join("linked")).unwrap();
        symlink(root.join("OWNER.md"), root.join("alias.md")).unwrap();
        (ws, outside)
    }

    #[cfg(unix)]
    #[test]
    fn workspace_listing_skips_hidden_skipped_symlinked_and_root_allowlisted_files() {
        let (ws, _outside) = workspace();
        let (files, complete) = list_workspace_markdown(ws.path());
        let names: Vec<&str> = files.iter().map(|f| f.filename.as_str()).collect();
        assert_eq!(
            names,
            [
                "OWNER.md",
                "notes/plan.md",
                "sub/SOUL.md",
                "notes/deep/x.MD"
            ]
        );
        assert!(complete);
        assert!(files.iter().all(|f| f.exists && f.mtime_ms.is_some()));
        assert_eq!(files[0].size, 5);
        for name in names {
            assert!(validate_filename(name).is_ok(), "{name}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn workspace_files_resolve_only_inside_the_workspace() {
        let (ws, _outside) = workspace();
        let root = ws.path().canonicalize().unwrap();
        assert_eq!(
            resolve_workspace_file(ws.path(), "notes/plan.md").unwrap(),
            root.join("notes/plan.md")
        );
        assert_eq!(
            resolve_workspace_file(ws.path(), "alias.md").unwrap(),
            root.join("OWNER.md")
        );
        for bad in ["missing.md", "escape.md", "linked/secret.md", "notes"] {
            assert!(resolve_workspace_file(ws.path(), bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn file_route_carries_nested_paths_and_leaves_templates_static() {
        use axum::{Router, routing::get};
        use tower::ServiceExt as _;
        let app = Router::new()
            .route("/api/personality/templates", get(|| async { "templates" }))
            .route(
                FILE_ROUTE,
                get(|axum::extract::Path(f): axum::extract::Path<String>| async move { f }),
            );
        for (uri, expected) in [
            ("/api/personality/templates", "templates"),
            ("/api/personality/OWNER.md?agent=director", "OWNER.md"),
            ("/api/personality/queue/a/report.md", "queue/a/report.md"),
            (
                "/api/personality/queue%2Fa%2Freport.md",
                "queue/a/report.md",
            ),
        ] {
            let request = axum::http::Request::builder()
                .uri(uri)
                .body(axum::body::Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(body, expected.as_bytes(), "{uri}");
        }
    }

    #[test]
    fn truncate_at_max_chars() {
        let s = "x".repeat(MAX_FILE_CHARS + 100);
        let (out, trunc) = truncate_to_chars(&s, MAX_FILE_CHARS);
        assert!(trunc);
        assert_eq!(out.chars().count(), MAX_FILE_CHARS);
    }

    #[test]
    fn no_truncation_when_under_limit() {
        let (out, trunc) = truncate_to_chars("hello", MAX_FILE_CHARS);
        assert!(!trunc);
        assert_eq!(out, "hello");
    }
}
