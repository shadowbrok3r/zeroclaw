//! CLI for inspecting and managing persisted sessions:
//! `zeroclaw sessions {list,show,search,rename,delete}`.
//!
//! Read-mostly surface over the unified session backend — the same store the
//! gateway WS handler and channel orchestrator write to. The SQLite backend
//! runs in WAL mode, so opening it while the daemon is running is safe
//! (mirrors `alias_cli`'s posture); rename/delete are the only writes.

use anyhow::{Context, Result, bail};
use std::io::Write as _;
use zeroclaw::SessionsCommands;
use zeroclaw_config::schema::Config;
use zeroclaw_infra::session_backend::{SessionBackend, SessionMetadata, SessionQuery};

/// Resolve a `cli-*` Fluent key for sessions CLI output. Under `agent-runtime`
/// (default + what CI/release build) this routes through Fluent; without it the
/// runtime i18n crate is absent, so the English `fallback` is used.
#[allow(unused_variables)]
fn mt(key: &str, fallback: &str) -> String {
    #[cfg(feature = "agent-runtime")]
    {
        zeroclaw_runtime::i18n::get_required_cli_string(key)
    }
    #[cfg(not(feature = "agent-runtime"))]
    {
        fallback.to_string() // i18n-exempt: English fallback when Fluent (agent-runtime) is disabled
    }
}

/// `mt` with `{$name}` arguments.
#[allow(unused_variables)]
fn mta(key: &str, args: &[(&str, &str)], fallback: &str) -> String {
    #[cfg(feature = "agent-runtime")]
    {
        zeroclaw_runtime::i18n::get_required_cli_string_with_args(key, args)
    }
    #[cfg(not(feature = "agent-runtime"))]
    {
        fallback.to_string() // i18n-exempt: English fallback when Fluent (agent-runtime) is disabled
    }
}

/// Resolve a user-supplied session id to a stored key: verbatim first, then
/// the `gw_<id>` (gateway WS) and `rpc_<id>` (RPC chat) prefixed forms.
fn resolve_session_key(backend: &dyn SessionBackend, id: &str) -> Option<String> {
    [id.to_string(), format!("gw_{id}"), format!("rpc_{id}")]
        .into_iter()
        .find(|candidate| backend.session_exists(candidate))
}

/// Whether the configured backend name selects the legacy JSONL store.
/// Mirrors `zeroclaw_infra::make_session_backend`: `"jsonl"` is JSONL,
/// everything else (including unknown values) falls back to SQLite.
fn is_jsonl_backend(backend_name: &str) -> bool {
    backend_name == "jsonl"
}

fn metadata_json(m: &SessionMetadata) -> serde_json::Value {
    serde_json::json!({
        "key": m.key,
        "name": m.name,
        "created_at": m.created_at.to_rfc3339(),
        "last_activity": m.last_activity.to_rfc3339(),
        "message_count": m.message_count,
        "agent_alias": m.agent_alias,
        "channel_id": m.channel_id,
        "room_id": m.room_id,
        "sender_id": m.sender_id,
    })
}

fn metadata_row(m: &SessionMetadata) -> [String; 6] {
    let dash = || "-".to_string();
    [
        m.key.clone(),
        m.name.clone().unwrap_or_else(dash),
        m.agent_alias.clone().unwrap_or_else(dash),
        m.channel_id.clone().unwrap_or_else(dash),
        m.message_count.to_string(),
        m.last_activity.format("%Y-%m-%d %H:%M:%S").to_string(),
    ]
}

/// Render an aligned table (two-space column gap): header line, then one line
/// per row. Column widths fit the widest cell; no trailing padding.
fn render_table(headers: &[String; 6], rows: &[[String; 6]]) -> Vec<String> {
    let mut widths = [0usize; 6];
    for (i, h) in headers.iter().enumerate() {
        widths[i] = h.chars().count();
    }
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let fmt_line = |cells: &[String; 6]| -> String {
        let mut line = String::new();
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                line.push_str("  ");
            }
            line.push_str(cell);
            for _ in 0..widths[i].saturating_sub(cell.chars().count()) {
                line.push(' ');
            }
        }
        line.trim_end().to_string()
    };
    let mut lines = Vec::with_capacity(rows.len() + 1);
    lines.push(fmt_line(headers));
    for row in rows {
        lines.push(fmt_line(row));
    }
    lines
}

fn print_metadata_table(sessions: &[SessionMetadata]) {
    if sessions.is_empty() {
        println!("{}", mt("cli-sessions-none", "(no sessions)"));
        return;
    }
    let headers = [
        mt("cli-sessions-col-key", "key"),
        mt("cli-sessions-col-name", "name"),
        mt("cli-sessions-col-agent", "agent"),
        mt("cli-sessions-col-channel", "channel"),
        mt("cli-sessions-col-msgs", "msgs"),
        mt("cli-sessions-col-last-activity", "last activity"),
    ];
    let rows: Vec<[String; 6]> = sessions.iter().map(metadata_row).collect();
    for line in render_table(&headers, &rows) {
        println!("{line}");
    }
}

fn resolve_or_bail(backend: &dyn SessionBackend, id: &str) -> Result<String> {
    match resolve_session_key(backend, id) {
        Some(key) => Ok(key),
        None => bail!(
            "{}",
            mta(
                "cli-sessions-not-found",
                &[("id", id)],
                "no session found for `{$id}` (tried `{$id}`, `gw_{$id}`, `rpc_{$id}`)"
            )
        ),
    }
}

pub fn handle_sessions(cmd: SessionsCommands, config: &Config) -> Result<()> {
    let backend =
        zeroclaw_infra::make_session_backend(&config.data_dir, &config.channels.session_backend)
            .context("open session backend")?;
    match cmd {
        SessionsCommands::List {
            agent,
            channel,
            limit,
            json,
        } => {
            let mut sessions = backend.list_sessions_with_metadata();
            if let Some(agent) = &agent {
                sessions.retain(|m| m.agent_alias.as_deref() == Some(agent.as_str()));
            }
            if let Some(prefix) = &channel {
                sessions.retain(|m| {
                    m.channel_id
                        .as_deref()
                        .is_some_and(|c| c.starts_with(prefix.as_str()))
                });
            }
            // SQLite already returns newest-first; the JSONL backend does not.
            sessions.sort_by_key(|m| std::cmp::Reverse(m.last_activity));
            sessions.truncate(limit);
            if json {
                let out: Vec<serde_json::Value> = sessions.iter().map(metadata_json).collect();
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                print_metadata_table(&sessions);
            }
            Ok(())
        }

        SessionsCommands::Show { id, limit, json } => {
            let key = resolve_or_bail(backend.as_ref(), &id)?;
            let all = backend.load_with_timestamps(&key);
            let total = all.len();
            let shown = if limit == 0 || limit >= total {
                &all[..]
            } else {
                &all[total - limit..]
            };
            if json {
                let messages: Vec<serde_json::Value> = shown
                    .iter()
                    .map(|m| {
                        serde_json::json!({
                            "role": m.message.role,
                            "content": m.message.content,
                            "created_at": m.created_at.map(|t| t.to_rfc3339()),
                        })
                    })
                    .collect();
                let out = serde_json::json!({
                    "key": key,
                    "message_count": total,
                    "messages": messages,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
                return Ok(());
            }
            let total_s = total.to_string();
            let shown_s = shown.len().to_string();
            println!(
                "{}",
                mta(
                    "cli-sessions-show-header",
                    &[
                        ("key", key.as_str()),
                        ("total", total_s.as_str()),
                        ("shown", shown_s.as_str())
                    ],
                    "session {$key} — {$total} message(s), showing {$shown}"
                )
            );
            for m in shown {
                let ts = m
                    .created_at
                    .map_or_else(|| "-".to_string(), |t| t.to_rfc3339());
                println!(
                    "{}",
                    mta(
                        "cli-sessions-show-message",
                        &[
                            ("role", m.message.role.as_str()),
                            ("timestamp", ts.as_str())
                        ],
                        "[{$role}] {$timestamp}"
                    )
                );
                println!("{}", m.message.content);
            }
            Ok(())
        }

        SessionsCommands::Search {
            keyword,
            limit,
            json,
        } => {
            let jsonl = is_jsonl_backend(&config.channels.session_backend);
            let results = backend.search(&SessionQuery {
                keyword: Some(keyword.clone()),
                limit,
            });
            if json {
                // stdout stays pure JSON; the JSONL-backend caveat goes to stderr.
                if jsonl {
                    eprintln!(
                        "{}",
                        mt(
                            "cli-sessions-search-jsonl",
                            "search needs the sqlite session backend; the jsonl backend has no full-text index (0 results)"
                        )
                    );
                }
                let out: Vec<serde_json::Value> = results.iter().map(metadata_json).collect();
                println!("{}", serde_json::to_string_pretty(&out)?);
                return Ok(());
            }
            if jsonl {
                println!(
                    "{}",
                    mt(
                        "cli-sessions-search-jsonl",
                        "search needs the sqlite session backend; the jsonl backend has no full-text index (0 results)"
                    )
                );
                return Ok(());
            }
            if results.is_empty() {
                println!(
                    "{}",
                    mta(
                        "cli-sessions-search-empty",
                        &[("keyword", keyword.as_str())],
                        "no sessions match `{$keyword}`"
                    )
                );
            } else {
                print_metadata_table(&results);
            }
            Ok(())
        }

        SessionsCommands::Rename { id, name } => {
            let key = resolve_or_bail(backend.as_ref(), &id)?;
            backend
                .set_session_name(&key, &name)
                .context("set session name")?;
            println!(
                "{}",
                mta(
                    "cli-sessions-renamed",
                    &[("key", key.as_str()), ("name", name.as_str())],
                    "renamed session {$key} → \"{$name}\""
                )
            );
            Ok(())
        }

        SessionsCommands::Delete { id, yes } => {
            let key = resolve_or_bail(backend.as_ref(), &id)?;
            if !yes {
                let count = backend
                    .get_session_metadata(&key)
                    .map_or(0, |m| m.message_count)
                    .to_string();
                print!(
                    "{} ",
                    mta(
                        "cli-sessions-delete-confirm",
                        &[("key", key.as_str()), ("count", count.as_str())],
                        "Delete session {$key} ({$count} message(s))? [y/N]"
                    )
                );
                std::io::stdout().flush()?;
                let mut line = String::new();
                std::io::stdin().read_line(&mut line)?;
                let answer = line.trim().to_ascii_lowercase();
                if answer != "y" && answer != "yes" {
                    println!(
                        "{}",
                        mt("cli-sessions-delete-aborted", "aborted — no changes made")
                    );
                    return Ok(());
                }
            }
            if backend.delete_session(&key).context("delete session")? {
                println!(
                    "{}",
                    mta(
                        "cli-sessions-deleted",
                        &[("key", key.as_str())],
                        "deleted session {$key}"
                    )
                );
            } else {
                // Ok(false) after a successful existence check: the backend
                // declined the delete (trait default) — report it distinctly.
                println!(
                    "{}",
                    mta(
                        "cli-sessions-delete-unsupported",
                        &[("key", key.as_str())],
                        "the session backend reported nothing deleted for {$key} — it may not support delete"
                    )
                );
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeroclaw_api::model_provider::ChatMessage;

    struct FakeBackend(Vec<String>);

    impl SessionBackend for FakeBackend {
        fn load(&self, _session_key: &str) -> Vec<ChatMessage> {
            Vec::new()
        }
        fn append(&self, _session_key: &str, _message: &ChatMessage) -> std::io::Result<()> {
            Ok(())
        }
        fn remove_last(&self, _session_key: &str) -> std::io::Result<bool> {
            Ok(false)
        }
        fn list_sessions(&self) -> Vec<String> {
            self.0.clone()
        }
        fn session_exists(&self, session_key: &str) -> bool {
            self.0.iter().any(|k| k == session_key)
        }
    }

    #[test]
    fn resolve_session_key_tries_verbatim_then_gw_then_rpc() {
        let backend = FakeBackend(vec![
            "discord.clamps_room_alice".to_string(),
            "gw_1234".to_string(),
            "rpc_abcd".to_string(),
        ]);
        assert_eq!(
            resolve_session_key(&backend, "discord.clamps_room_alice").as_deref(),
            Some("discord.clamps_room_alice")
        );
        // Verbatim match wins for already-prefixed ids.
        assert_eq!(
            resolve_session_key(&backend, "gw_1234").as_deref(),
            Some("gw_1234")
        );
        assert_eq!(
            resolve_session_key(&backend, "1234").as_deref(),
            Some("gw_1234")
        );
        assert_eq!(
            resolve_session_key(&backend, "abcd").as_deref(),
            Some("rpc_abcd")
        );
        assert_eq!(resolve_session_key(&backend, "missing"), None);
    }

    #[test]
    fn render_table_aligns_columns_and_trims_trailing_space() {
        let headers = [
            "key".to_string(),
            "name".to_string(),
            "agent".to_string(),
            "channel".to_string(),
            "msgs".to_string(),
            "last activity".to_string(),
        ];
        let rows = vec![
            [
                "gw_1234".to_string(),
                "-".to_string(),
                "default".to_string(),
                "-".to_string(),
                "7".to_string(),
                "2026-08-17 10:00:00".to_string(),
            ],
            [
                "discord.clamps_room_alice".to_string(),
                "triage".to_string(),
                "-".to_string(),
                "discord.clamps".to_string(),
                "12".to_string(),
                "2026-08-16 09:30:00".to_string(),
            ],
        ];
        let lines = render_table(&headers, &rows);
        assert_eq!(lines.len(), 3);
        // Widest key is 25 chars + 2-space gap → the name column starts at 27
        // on every line.
        assert_eq!(lines[0].find("name"), Some(27));
        assert_eq!(lines[1].find('-'), Some(27)); // row 1 name cell is "-"
        assert_eq!(lines[2].find("triage"), Some(27));
        // No trailing padding on any line.
        for line in &lines {
            assert_eq!(line.trim_end(), line);
        }
    }
}
