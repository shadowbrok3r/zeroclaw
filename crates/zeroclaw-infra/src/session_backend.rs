//! Trait abstraction for session persistence backends.

use chrono::{DateTime, Utc};
use zeroclaw_api::model_provider::ChatMessage;

/// Metadata about a persisted session.
#[derive(Debug, Clone)]
pub struct SessionMetadata {
    /// Session key (e.g. `telegram_user123`).
    pub key: String,
    /// Optional human-readable name (e.g. `eyrie-commander-briefing`).
    pub name: Option<String>,
    /// When the session was first created.
    pub created_at: DateTime<Utc>,
    /// When the last message was appended.
    pub last_activity: DateTime<Utc>,
    /// Total number of messages in the session.
    pub message_count: usize,
    /// Alias of the agent that owned this session (HashMap key in
    /// `config.agents`). `None` for sessions persisted before per-agent
    /// attribution landed, or for backends that don't track it.
    pub agent_alias: Option<String>,
    /// Dotted ChannelRef the session belongs to (`<type>.<alias>`,
    /// e.g. `discord.clamps`). `None` for non-channel sessions (CLI,
    /// internal cron runs) or backends without routing columns.
    pub channel_id: Option<String>,
    /// Platform-side room / thread identifier (Discord channel id,
    /// Matrix room id, Slack thread ts, ...). `None` for direct messages
    /// or backends that don't track it.
    pub room_id: Option<String>,
    /// Inbound sender id verbatim (Discord username, phone number, ...).
    /// Not an FK — sessions can survive deletion of the upstream user.
    pub sender_id: Option<String>,
    /// Principal that first touched the session over an authenticated
    /// transport (`device:<sha256(bearer token)>`). First-writer wins;
    /// never overwritten once set. `None` for sessions created before
    /// stamping landed, unauthenticated transports, or backends that
    /// don't track it.
    pub origin_principal: Option<String>,
}

/// Structured routing context recorded alongside a session. Mirrors the
/// `ChannelMessage` fields the orchestrator uses to compose
/// `conversation_history_key` so the session row can be queried by
/// channel / room / sender without re-parsing the synthetic key.
#[derive(Debug, Clone, Default)]
pub struct SessionContext<'a> {
    /// `<type>.<alias>` ChannelRef (`discord.clamps`).
    pub channel_id: Option<&'a str>,
    /// Platform-side room / thread id.
    pub room_id: Option<&'a str>,
    /// Inbound sender id (channel-native username, phone, ...).
    pub sender_id: Option<&'a str>,
}

/// Query parameters for listing sessions.
#[derive(Debug, Clone, Default)]
pub struct SessionQuery {
    /// Keyword to search in session messages (FTS5 if available).
    pub keyword: Option<String>,
    /// Maximum number of sessions to return.
    pub limit: Option<usize>,
}

/// Which session family a scoped TTL sweep may delete from. Sweeps are
/// owned per subsystem (gateway vs. channel orchestrator), so each scope
/// must never touch the other family's rows — nor RPC chat sessions,
/// which have no TTL owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionCleanupScope {
    /// Gateway WebSocket sessions (`gw_`-prefixed keys).
    Gateway,
    /// Channel-driven sessions (rows with a recorded `channel_id`).
    Channel,
}

/// One persisted message with the optional `created_at` the backend
/// stamped on it. JSONL / in-memory backends return `None`; SQLite
/// returns the row's `created_at` column.
#[derive(Debug, Clone)]
pub struct TimestampedMessage {
    pub message: ChatMessage,
    pub created_at: Option<DateTime<Utc>>,
}

/// Trait for session persistence backends.
/// Implementations must be `Send + Sync` for sharing across async tasks.
pub trait SessionBackend: Send + Sync {
    /// Load all messages for a session. Returns empty vec if session doesn't exist.
    fn load(&self, session_key: &str) -> Vec<ChatMessage>;

    /// Same as `load`, but each row carries its persisted `created_at`
    /// when the backend has one. Default impl falls back to `load`
    /// without timestamps so non-SQLite backends keep working.
    fn load_with_timestamps(&self, session_key: &str) -> Vec<TimestampedMessage> {
        self.load(session_key)
            .into_iter()
            .map(|message| TimestampedMessage {
                message,
                created_at: None,
            })
            .collect()
    }

    /// Append a single message to a session.
    fn append(&self, session_key: &str, message: &ChatMessage) -> std::io::Result<()>;

    /// Remove the last message from a session. Returns `true` if a message was removed.
    fn remove_last(&self, session_key: &str) -> std::io::Result<bool>;

    fn update_last(&self, session_key: &str, message: &ChatMessage) -> std::io::Result<bool> {
        if self.remove_last(session_key)? {
            self.append(session_key, message)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all session keys.
    fn list_sessions(&self) -> Vec<String>;

    /// List sessions with metadata.
    fn list_sessions_with_metadata(&self) -> Vec<SessionMetadata> {
        // Default: construct metadata from messages (backends can override for efficiency)
        self.list_sessions()
            .into_iter()
            .map(|key| {
                let messages = self.load(&key);
                SessionMetadata {
                    key,
                    name: None,
                    created_at: Utc::now(),
                    last_activity: Utc::now(),
                    message_count: messages.len(),
                    agent_alias: None,
                    channel_id: None,
                    room_id: None,
                    sender_id: None,
                    origin_principal: None,
                }
            })
            .collect()
    }

    /// Compact a session file (remove duplicates/corruption). No-op by default.
    fn compact(&self, _session_key: &str) -> std::io::Result<()> {
        Ok(())
    }

    /// Remove sessions that haven't been active within the given TTL hours.
    fn cleanup_stale(&self, _ttl_hours: u32) -> std::io::Result<usize> {
        Ok(0)
    }

    /// Remove stale sessions belonging to one [`SessionCleanupScope`] only.
    /// Backends without per-family metadata (JSONL) skip the sweep entirely.
    fn cleanup_stale_scoped(
        &self,
        _ttl_hours: u64,
        _scope: SessionCleanupScope,
    ) -> std::io::Result<usize> {
        Ok(0)
    }

    /// Search sessions by keyword. Default returns empty (backends with FTS override).
    fn search(&self, _query: &SessionQuery) -> Vec<SessionMetadata> {
        Vec::new()
    }

    fn clear_messages(&self, session_key: &str) -> std::io::Result<usize> {
        let mut count = 0;
        while self.remove_last(session_key)? {
            count += 1;
        }
        Ok(count)
    }

    /// Delete all messages for a session. Returns `true` if the session existed.
    fn delete_session(&self, _session_key: &str) -> std::io::Result<bool> {
        Ok(false)
    }

    fn clear_agent_attribution(&self, _agent_alias: &str) -> std::io::Result<usize> {
        Ok(0)
    }

    fn rename_agent_attribution(&self, _from: &str, _to: &str) -> std::io::Result<usize> {
        Ok(0)
    }

    fn count_agent_attribution(&self, _agent_alias: &str) -> std::io::Result<usize> {
        Ok(0)
    }

    fn session_exists(&self, session_key: &str) -> bool {
        self.get_session_metadata(session_key).is_some()
    }

    /// Set or update the human-readable name for a session.
    fn set_session_name(&self, _session_key: &str, _name: &str) -> std::io::Result<()> {
        Ok(())
    }

    /// Get the human-readable name for a session (if set).
    fn get_session_name(&self, _session_key: &str) -> std::io::Result<Option<String>> {
        Ok(None)
    }

    /// Record the agent alias that owns a session. Called on WebSocket
    /// handshake when the alias is known. No-op for backends that don't
    /// track per-agent attribution.
    fn set_session_agent_alias(
        &self,
        _session_key: &str,
        _agent_alias: &str,
    ) -> std::io::Result<()> {
        Ok(())
    }

    /// Get the agent alias associated with a session, if recorded.
    fn get_session_agent_alias(&self, _session_key: &str) -> std::io::Result<Option<String>> {
        Ok(None)
    }

    /// Record the principal that created a session (e.g.
    /// `device:<sha256(token)>`). First-writer wins: once a principal is
    /// stored it is never overwritten. Backends create the metadata row if
    /// the session has not persisted any messages yet. No-op for backends
    /// that don't track it.
    fn set_session_origin_principal(
        &self,
        _session_key: &str,
        _principal: &str,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn set_session_context(
        &self,
        _session_key: &str,
        _context: SessionContext<'_>,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn get_session_metadata(&self, session_key: &str) -> Option<SessionMetadata> {
        let messages = self.load(session_key);
        if messages.is_empty() {
            return None;
        }
        Some(SessionMetadata {
            key: session_key.to_string(),
            name: self.get_session_name(session_key).ok().flatten(),
            created_at: Utc::now(),
            last_activity: Utc::now(),
            message_count: messages.len(),
            agent_alias: None,
            channel_id: None,
            room_id: None,
            sender_id: None,
            origin_principal: None,
        })
    }

    /// Refresh a session's `last_activity` to now without touching anything
    /// else. Called on WS resume so a just-resumed thread is never near the
    /// TTL cutoff. Must not create a row for an unknown key. No-op for
    /// backends that don't track activity timestamps.
    fn touch_session(&self, _session_key: &str) -> std::io::Result<()> {
        Ok(())
    }

    /// Set the session state (e.g. "idle", "running", "error").
    /// `turn_id` identifies the current turn (set when running, cleared on idle).
    fn set_session_state(
        &self,
        _session_key: &str,
        _state: &str,
        _turn_id: Option<&str>,
    ) -> std::io::Result<()> {
        Ok(())
    }

    /// Get the current session state. Returns `None` if the backend doesn't track state.
    fn get_session_state(&self, _session_key: &str) -> std::io::Result<Option<SessionState>> {
        Ok(None)
    }

    /// List sessions currently in "running" state.
    fn list_running_sessions(&self) -> Vec<SessionMetadata> {
        Vec::new()
    }

    /// List sessions stuck in "running" state longer than `threshold_secs`.
    fn list_stuck_sessions(&self, _threshold_secs: u64) -> Vec<SessionMetadata> {
        Vec::new()
    }
}

/// Resolve a caller-supplied session id to the full stored session key.
///
/// The one policy point for id → key resolution shared by the gateway API,
/// the sessions CLI, and the inter-agent session tools. Tries the id
/// verbatim first (channel-composite keys like `discord.clamps_room_alice`
/// and already-prefixed `gw_`/`rpc_` keys), then the `gw_` gateway-WS
/// prefix, then the `rpc_` RPC-chat prefix. Returns `None` when no session
/// exists under any candidate key.
pub fn resolve_session_key(backend: &dyn SessionBackend, id: &str) -> Option<String> {
    if backend.session_exists(id) {
        return Some(id.to_string());
    }
    let gw = format!("gw_{id}");
    if backend.session_exists(&gw) {
        return Some(gw);
    }
    let rpc = format!("rpc_{id}");
    if backend.session_exists(&rpc) {
        return Some(rpc);
    }
    None
}

/// Session state information.
#[derive(Debug, Clone)]
pub struct SessionState {
    /// Current state: "idle", "running", or "error".
    pub state: String,
    /// Turn ID of the active or last turn.
    pub turn_id: Option<String>,
    /// When the current state was entered.
    pub turn_started_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_metadata_is_constructible() {
        let meta = SessionMetadata {
            key: "test".into(),
            name: None,
            created_at: Utc::now(),
            last_activity: Utc::now(),
            message_count: 5,
            agent_alias: None,
            channel_id: None,
            room_id: None,
            sender_id: None,
            origin_principal: None,
        };
        assert_eq!(meta.key, "test");
        assert_eq!(meta.message_count, 5);
    }

    #[test]
    fn session_query_defaults() {
        let q = SessionQuery::default();
        assert!(q.keyword.is_none());
        assert!(q.limit.is_none());
    }

    struct KeySetBackend(Vec<String>);

    impl SessionBackend for KeySetBackend {
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
        let backend = KeySetBackend(vec![
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
}
