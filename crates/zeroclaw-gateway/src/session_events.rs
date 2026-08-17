//! Session lifecycle SSE frames (metadata only).
//!
//! Builds and broadcasts `session_created` / `session_update` /
//! `session_closed` frames on the shared event bus so the web dashboard can
//! keep its session list live without polling. Frames carry session METADATA
//! only — never message content — which is why `source == "sessions"` frames
//! pass the content filter (see `sse::is_public_sse_event`). They still carry
//! session keys and thread names, so delivery additionally requires an
//! authenticated, unscoped stream (see `sse::withhold_sessions_frame`).
//! Every emit mirrors `BroadcastObserver`'s dual write: the frame is pushed
//! into the history ring buffer and fanned out on the live broadcast channel.

use super::AppState;

/// Which lifecycle transition a frame announces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionEventKind {
    Created,
    Update,
    Closed,
}

impl SessionEventKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Created => "session_created",
            Self::Update => "session_update",
            Self::Closed => "session_closed",
        }
    }
}

/// Optional metadata carried on a lifecycle frame. Everything here is
/// session METADATA — adding any message-content field would break the
/// public-SSE contract pinned by the tests below.
#[derive(Debug, Default, Clone)]
pub(crate) struct SessionEventFields {
    pub agent_alias: Option<String>,
    pub name: Option<String>,
    pub message_count: Option<u64>,
    /// `"idle"` | `"running"` | `"error"` when known.
    pub state: Option<String>,
}

/// Hydrate [`SessionEventFields`] from the backend's metadata + state rows.
/// Missing rows yield empty fields (the frame still carries key/id/timestamp).
pub(crate) fn fields_from_backend(
    backend: &dyn zeroclaw_infra::session_backend::SessionBackend,
    session_key: &str,
) -> SessionEventFields {
    let mut fields = SessionEventFields::default();
    if let Some(meta) = backend.get_session_metadata(session_key) {
        fields.agent_alias = meta.agent_alias;
        fields.name = meta.name;
        fields.message_count = Some(u64::try_from(meta.message_count).unwrap_or(u64::MAX));
    }
    if let Ok(Some(ss)) = backend.get_session_state(session_key) {
        fields.state = Some(ss.state);
    }
    fields
}

/// Build a lifecycle frame. `session_id` is the display form the REST list
/// endpoint returns: `gw_` stripped for gateway sessions, the full composite
/// key for channel-driven sessions.
pub(crate) fn build_session_event(
    kind: SessionEventKind,
    session_key: &str,
    fields: &SessionEventFields,
) -> serde_json::Value {
    let session_id = session_key.strip_prefix("gw_").unwrap_or(session_key);
    let mut frame = serde_json::json!({
        "type": kind.as_str(),
        "source": "sessions",
        "session_key": session_key,
        "session_id": session_id,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    if let Some(ref alias) = fields.agent_alias {
        frame["agent_alias"] = serde_json::Value::String(alias.clone());
    }
    if let Some(ref name) = fields.name {
        frame["name"] = serde_json::Value::String(name.clone());
    }
    if let Some(count) = fields.message_count {
        frame["message_count"] = serde_json::Value::from(count);
    }
    if let Some(ref state) = fields.state {
        frame["state"] = serde_json::Value::String(state.clone());
    }
    frame
}

/// Broadcast a lifecycle frame and record it in the SSE history buffer.
pub(crate) fn emit_session_event(
    state: &AppState,
    kind: SessionEventKind,
    session_key: &str,
    fields: SessionEventFields,
) {
    let frame = build_session_event(kind, session_key, &fields);
    state.event_buffer.push(frame.clone());
    let _ = state.event_tx.send(frame);
}

/// Emit a lifecycle frame with fields freshly hydrated from the session
/// backend. Falls back to bare key/id/timestamp fields when persistence is
/// disabled or the row is already gone.
pub(crate) fn emit_session_event_from_backend(
    state: &AppState,
    kind: SessionEventKind,
    session_key: &str,
) {
    let fields = state
        .session_backend
        .as_ref()
        .map(|backend| fields_from_backend(backend.as_ref(), session_key))
        .unwrap_or_default();
    emit_session_event(state, kind, session_key, fields);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_fields() -> SessionEventFields {
        SessionEventFields {
            agent_alias: Some("default".into()),
            name: Some("release-check".into()),
            message_count: Some(7),
            state: Some("idle".into()),
        }
    }

    #[test]
    fn gateway_key_is_stripped_to_display_id() {
        let frame = build_session_event(
            SessionEventKind::Created,
            "gw_abc-123",
            &SessionEventFields::default(),
        );
        assert_eq!(frame["type"], "session_created");
        assert_eq!(frame["source"], "sessions");
        assert_eq!(frame["session_key"], "gw_abc-123");
        assert_eq!(frame["session_id"], "abc-123");
        assert!(frame["timestamp"].is_string());
    }

    #[test]
    fn channel_composite_key_is_display_id_verbatim() {
        let frame = build_session_event(
            SessionEventKind::Update,
            "discord.clamps_room1_alice",
            &SessionEventFields::default(),
        );
        assert_eq!(frame["session_id"], "discord.clamps_room1_alice");
        assert_eq!(frame["session_key"], "discord.clamps_room1_alice");
    }

    #[test]
    fn frames_carry_metadata_only_never_content() {
        // Pins the public-SSE contract: lifecycle frames are metadata-only.
        // A "content" key (or any unknown key) would leak chat text onto the
        // unauthenticated-capable /api/events stream.
        let allowed = [
            "type",
            "source",
            "session_key",
            "session_id",
            "timestamp",
            "agent_alias",
            "name",
            "message_count",
            "state",
        ];
        for kind in [
            SessionEventKind::Created,
            SessionEventKind::Update,
            SessionEventKind::Closed,
        ] {
            let frame = build_session_event(kind, "gw_abc", &full_fields());
            assert!(
                frame.get("content").is_none(),
                "lifecycle frames must never carry message content: {frame}"
            );
            let obj = frame.as_object().expect("frame is an object");
            for key in obj.keys() {
                assert!(
                    allowed.contains(&key.as_str()),
                    "unexpected key `{key}` on lifecycle frame: {frame}"
                );
            }
        }
    }

    #[test]
    fn optional_fields_are_included_when_set() {
        let frame = build_session_event(SessionEventKind::Update, "gw_abc", &full_fields());
        assert_eq!(frame["agent_alias"], "default");
        assert_eq!(frame["name"], "release-check");
        assert_eq!(frame["message_count"], 7);
        assert_eq!(frame["state"], "idle");
    }

    #[tokio::test]
    async fn emit_dual_writes_to_broadcast_and_buffer() {
        let state = crate::api::test_state(zeroclaw_config::schema::Config::default());
        let mut rx = state.event_tx.subscribe();

        emit_session_event(
            &state,
            SessionEventKind::Closed,
            "gw_gone",
            SessionEventFields::default(),
        );

        let live = rx.try_recv().expect("frame should broadcast");
        assert_eq!(live["type"], "session_closed");
        assert_eq!(live["session_id"], "gone");

        let history = state.event_buffer.snapshot();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0]["type"], "session_closed");
    }
}
