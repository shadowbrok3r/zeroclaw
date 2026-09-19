//! Per-session turn hub: lets a socket that reconnects mid-turn rejoin the
//! turn already running for its session, instead of losing everything the
//! original socket was streaming.
//!
//! The gateway runs one turn at a time per session (the `session_queue`
//! serializes them) and, until now, streamed that turn straight to the one
//! socket that started it. A phone that changed network mid-turn reconnected
//! to a fresh socket that got nothing of the running turn — the reply only
//! reappeared from history after `done`, and the thinking/tool rows streamed
//! after the drop were lost.
//!
//! A [`TurnHub`], keyed by `session_key`, fixes that. The turn's producer
//! ([`crate::ws::process_chat_message`]) still streams to its own socket, but
//! now also mirrors every frame into the hub: appended to an ordered replay
//! buffer and broadcast live. A socket that attaches while the turn is running
//! gets the replay buffer as one `turn_resume` frame, then the rest of the turn
//! live over the broadcast. Pending tool approvals live on the hub too, so an
//! `approval_response` from the resumed socket answers a prompt the running
//! turn is parked on.
//!
//! Contract (matches the zc-codex app, `android/src/zeroclaw.rs` `turn_resume`
//! arm and its `a_turn_cut_by_a_reconnect_is_replaced_by_the_gateways_replay`
//! test): the replay holds the WHOLE running turn from its first frame — the
//! app drops the rows the dropped socket made and renumbers from `frames[]` —
//! so adjacent `chunk` deltas and adjacent `thinking` deltas are merged in the
//! buffer (never across a `tool_call`/`tool_result`, which is where the app
//! numbers rows), while the live broadcast still carries raw deltas.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};

use crate::ws_approval::{PendingApprovals, new_pending_approvals};

/// Live-frame fan-out capacity. A slow observer that falls this far behind is
/// detached (it re-syncs from history / the next turn) rather than stalling the
/// turn — see the `Lagged` handling in the observer loop.
const LIVE_CAPACITY: usize = 1024;

/// The frame types that belong in the replay buffer: exactly what the app
/// re-renders from `turn_resume.frames[]`. Terminal frames (`done`/`aborted`/
/// `error`) are broadcast live to observers but never buffered — a socket that
/// arrives after the turn ended sees `running() == false` and no replay.
pub fn is_replayable(frame_type: &str) -> bool {
    matches!(
        frame_type,
        "chunk" | "thinking" | "tool_call" | "tool_result" | "plan" | "approval_request"
    )
}

/// One session's running-turn state. Shared (`Arc`) between the socket that
/// drives the turn and any socket that attaches to observe it.
pub struct TurnHub {
    /// The running turn's replayable frames, in order, with adjacent chunk /
    /// thinking deltas merged. Cleared at the start and end of every turn.
    replay: Mutex<Vec<Value>>,
    /// Live serialized frames to every attached observer socket.
    live: broadcast::Sender<String>,
    /// True only while a turn is streaming for this session.
    running: AtomicBool,
    /// Tool-approval prompts awaiting an operator decision, keyed by request id.
    /// On the hub (not per socket) so a resumed socket can answer them.
    pub pending_approvals: PendingApprovals,
    /// Steering channel of the running turn, so a resumed socket can inject a
    /// mid-turn message. `None` between turns.
    steering: Mutex<Option<mpsc::Sender<String>>>,
}

impl TurnHub {
    fn new() -> Arc<Self> {
        let (live, _rx) = broadcast::channel(LIVE_CAPACITY);
        Arc::new(Self {
            replay: Mutex::new(Vec::new()),
            live,
            running: AtomicBool::new(false),
            pending_approvals: new_pending_approvals(),
            steering: Mutex::new(None),
        })
    }

    pub fn running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Mark a turn started: reset the replay buffer and install its steering
    /// channel. Taken under the replay lock so it cannot interleave with an
    /// observer's [`attach`](Self::attach).
    pub fn begin(&self, steering: mpsc::Sender<String>) {
        let mut replay = self.replay.lock();
        replay.clear();
        *self.steering.lock() = Some(steering);
        self.running.store(true, Ordering::Release);
    }

    /// Record and fan out one replayable frame. Adjacent `chunk` deltas (and
    /// adjacent `thinking` deltas) are merged in the buffer to keep the eventual
    /// `turn_resume` small; the live broadcast always carries the raw delta so a
    /// mid-turn observer builds the same rows as the original socket.
    ///
    /// The broadcast happens under the replay lock so that an observer calling
    /// [`attach`](Self::attach) either snapshots this frame (and does not also
    /// receive it live) or receives it live (and it is not yet in the snapshot)
    /// — never both, never neither.
    pub fn push(&self, frame: &Value) {
        let ty = frame.get("type").and_then(Value::as_str).unwrap_or("");
        if !is_replayable(ty) {
            return;
        }
        let mut replay = self.replay.lock();
        let merged_into_last = (ty == "chunk" || ty == "thinking")
            && replay
                .last()
                .and_then(|f| f.get("type"))
                .and_then(Value::as_str)
                == Some(ty)
            && {
                let add = frame
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if let Some(last) = replay.last_mut() {
                    let cur = last.get("content").and_then(Value::as_str).unwrap_or("");
                    last["content"] = Value::String(format!("{cur}{add}"));
                }
                true
            };
        if !merged_into_last {
            replay.push(frame.clone());
        }
        let _ = self.live.send(frame.to_string());
    }

    /// Broadcast a terminal frame (`done`/`aborted`/`error`) and end the turn.
    /// Under the replay lock so an observer either attaches (and its subscriber
    /// receives this frame) or sees `running() == false` and skips resume.
    pub fn finish(&self, frame: &Value) {
        let mut replay = self.replay.lock();
        let _ = self.live.send(frame.to_string());
        replay.clear();
        *self.steering.lock() = None;
        self.running.store(false, Ordering::Release);
    }

    /// Attach an observer to a running turn: atomically snapshot the replay
    /// buffer and subscribe to the live broadcast. Returns `None` if no turn is
    /// running (the caller then serves the socket normally). Because both this
    /// and [`push`](Self::push)/[`finish`](Self::finish) take the replay lock,
    /// the subscriber is positioned exactly after the snapshot's last frame.
    ///
    /// An `approval_request` is replayed only while its request is still
    /// pending: `ws_approval` removes a request from `pending_approvals` once it
    /// is answered, denied or timed out, but its frame stays in the buffer, and
    /// the app opens a modal for every replayed request — an answer to a stale
    /// one would go nowhere. A request raised after this snapshot arrives live.
    /// Lock order is replay → pending_approvals; nothing takes them in reverse.
    pub fn attach(&self) -> Option<(broadcast::Receiver<String>, Vec<Value>)> {
        let replay = self.replay.lock();
        if !self.running.load(Ordering::Acquire) {
            return None;
        }
        let rx = self.live.subscribe();
        let pending = self.pending_approvals.lock();
        let frames = replay
            .iter()
            .filter(|frame| {
                frame.get("type").and_then(Value::as_str) != Some("approval_request")
                    || frame
                        .get("request_id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| pending.contains_key(id))
            })
            .cloned()
            .collect();
        Some((rx, frames))
    }

    /// Inject a steering message into the running turn. `false` if no turn is
    /// running or its steering queue is full/closed.
    pub fn steer(&self, content: String) -> bool {
        self.steering
            .lock()
            .as_ref()
            .is_some_and(|tx| tx.try_send(content).is_ok())
    }
}

fn registry() -> &'static Mutex<HashMap<String, Arc<TurnHub>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<TurnHub>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The hub for a session, creating it if absent. Callers hold the returned
/// `Arc` for the socket's lifetime, which is what keeps the entry alive; the
/// sweep below reclaims a session's entry once its last socket is gone.
pub fn hub_for(session_key: &str) -> Arc<TurnHub> {
    let mut map = registry().lock();
    // Opportunistic reclaim: drop entries that no live socket references
    // (only the registry's own Arc remains) and that are not mid-turn. Bounds
    // the map to roughly the number of connected sessions without a background
    // sweeper. Never removes the key being looked up (a caller is about to hold
    // it) nor a session whose turn is still streaming.
    map.retain(|_, hub| Arc::strong_count(hub) > 1 || hub.running());
    map.entry(session_key.to_string())
        .or_insert_with(TurnHub::new)
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn adjacent_chunk_and_thinking_deltas_merge_but_not_across_a_tool_call() {
        let hub = TurnHub::new();
        hub.begin(mpsc::channel(1).0);
        hub.push(&json!({"type":"thinking","content":"Let me "}));
        hub.push(&json!({"type":"thinking","content":"recall (ear"}));
        hub.push(&json!({"type":"tool_call","id":"c1","name":"shell","args":{"command":"comfy-gen where"}}));
        hub.push(
            &json!({"type":"tool_result","id":"c1","name":"shell","output":"prompt: a lighthouse"}),
        );
        hub.push(&json!({"type":"chunk","content":"It "}));
        hub.push(&json!({"type":"chunk","content":"was"}));

        let (_, frames) = hub.attach().expect("running");
        assert_eq!(
            frames.len(),
            4,
            "two thinking deltas and two chunk deltas each merged to one"
        );
        assert_eq!(frames[0]["type"], "thinking");
        assert_eq!(frames[0]["content"], "Let me recall (ear");
        assert_eq!(frames[1]["type"], "tool_call");
        assert_eq!(frames[2]["type"], "tool_result");
        assert_eq!(frames[3]["type"], "chunk");
        assert_eq!(frames[3]["content"], "It was");
    }

    #[test]
    fn terminal_and_non_replayable_frames_are_not_buffered() {
        let hub = TurnHub::new();
        hub.begin(mpsc::channel(1).0);
        hub.push(&json!({"type":"chunk","content":"hi"}));
        // Not a replayable type: ignored by the buffer.
        hub.push(&json!({"type":"history_trimmed","dropped_messages":1}));
        let (_, frames) = hub.attach().expect("running");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["type"], "chunk");
    }

    #[test]
    fn attach_returns_none_after_the_turn_finishes_and_replay_is_cleared() {
        let hub = TurnHub::new();
        hub.begin(mpsc::channel(1).0);
        hub.push(&json!({"type":"chunk","content":"hi"}));
        assert!(hub.running());
        hub.finish(&json!({"type":"done","full_response":"hi"}));
        assert!(!hub.running());
        assert!(
            hub.attach().is_none(),
            "a socket arriving after done gets no replay"
        );
    }

    #[tokio::test]
    async fn an_observer_snapshots_the_past_and_receives_later_frames_live_exactly_once() {
        let hub = TurnHub::new();
        hub.begin(mpsc::channel(1).0);
        hub.push(&json!({"type":"thinking","content":"before"}));
        let (mut rx, frames) = hub.attach().expect("running");
        // The snapshot has the pre-attach frame...
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["content"], "before");
        // ...and the receiver has none of it yet.
        assert!(rx.try_recv().is_err());
        // A frame pushed after attach arrives live, and only live.
        hub.push(&json!({"type":"chunk","content":"after"}));
        let live: Value = serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
        assert_eq!(live["type"], "chunk");
        assert_eq!(live["content"], "after");
        hub.finish(&json!({"type":"done"}));
        let term: Value = serde_json::from_str(&rx.recv().await.unwrap()).unwrap();
        assert_eq!(term["type"], "done");
    }

    #[test]
    fn only_still_pending_approval_requests_are_replayed() {
        let hub = TurnHub::new();
        hub.begin(mpsc::channel(1).0);
        for id in ["answered", "open"] {
            let (tx, _rx) =
                tokio::sync::oneshot::channel::<zeroclaw_api::channel::ChannelApprovalResponse>();
            hub.pending_approvals.lock().insert(id.to_string(), tx);
            hub.push(&json!({"type":"approval_request","request_id":id,"tool":"shell"}));
        }
        hub.push(&json!({"type":"chunk","content":"after"}));
        // `ws_approval` drops a request from the map once it is answered.
        hub.pending_approvals.lock().remove("answered");

        let (_, frames) = hub.attach().expect("running");
        let approvals: Vec<&str> = frames
            .iter()
            .filter(|f| f["type"] == "approval_request")
            .filter_map(|f| f["request_id"].as_str())
            .collect();
        assert_eq!(
            approvals,
            ["open"],
            "an answered request must not re-open a modal"
        );
        assert_eq!(
            frames.last().unwrap()["type"],
            "chunk",
            "other frames are untouched"
        );
    }

    #[test]
    fn steer_routes_to_the_running_turn_and_stops_after_it_finishes() {
        let (tx, mut rx) = mpsc::channel(4);
        let hub = TurnHub::new();
        hub.begin(tx);
        assert!(hub.steer("keep going".into()));
        assert_eq!(rx.try_recv().unwrap(), "keep going");
        hub.finish(&json!({"type":"done"}));
        assert!(!hub.steer("too late".into()), "no steering between turns");
    }

    #[test]
    fn the_registry_reclaims_unreferenced_idle_sessions_but_keeps_referenced_ones() {
        let a = hub_for("gw_reclaim_a");
        // Only the registry + `a` reference it; a running turn must survive a sweep.
        a.begin(mpsc::channel(1).0);
        drop(hub_for("gw_reclaim_b")); // b now referenced only by the registry
        let _c = hub_for("gw_reclaim_c"); // triggers a sweep
        {
            let map = registry().lock();
            assert!(map.contains_key("gw_reclaim_a"), "a is running → kept");
            assert!(
                !map.contains_key("gw_reclaim_b"),
                "b is idle & unreferenced → swept"
            );
        }
        a.finish(&json!({"type":"done"}));
    }
}
