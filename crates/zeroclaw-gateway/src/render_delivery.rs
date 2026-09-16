//! Force the renders of a turn into its reply from the durable Comfy job receipts.
//!
//! Until now the model was the only thing naming the file behind an outbound
//! `[IMAGE:…]` marker, and it named it wrong most of the time: fabricated
//! directories and UUID names, or gallery epochs rebuilt from memory and off by
//! seconds. Every phone-side image failure traced to that one dependency. The job
//! receipts (`comfy-gen where --deliverable`) know exactly which renders this
//! session produced and where their bytes are, so the reply is rewritten from
//! them at turn end, before it is persisted and before the `done` frame goes
//! out: every model-written image marker is dropped and the verified ones are
//! put first, one per line. The client already treats `done.full_response` and
//! the persisted row as authoritative, so nothing on the phone changes.
//!
//! With no render this turn the model is still not trusted blindly: a marker
//! whose path is not an existing regular file is dropped (it could never have
//! been served), one that is stays.

use zeroclaw_providers::ConversationMessage;

use crate::session_jobs;

const IMAGE_OPEN: &str = "[IMAGE:";

/// What the rewrite did, for the log line and the tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Rewrite {
    pub text: String,
    pub added: Vec<String>,
    pub kept: Vec<String>,
    pub dropped: Vec<String>,
}

/// A payload that names a file or resource, as opposed to prose about the marker
/// syntax itself (`[IMAGE:…]`, `[IMAGE:<path>]`), which must stay as written.
fn is_marker_payload(payload: &str) -> bool {
    if payload.is_empty() {
        return false;
    }
    let lower = payload.to_ascii_lowercase();
    payload.starts_with('/')
        || payload.starts_with('~')
        || lower.starts_with("data:")
        || lower.starts_with("comfy-job:")
        || lower.contains("://")
        || [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"]
            .iter()
            .any(|ext| lower.ends_with(ext))
}

/// Case-insensitive `[IMAGE:payload]` spans: `(start, end_exclusive, payload)`.
fn image_markers(text: &str) -> Vec<(usize, usize, &str)> {
    let lower = text.to_ascii_lowercase();
    let open = IMAGE_OPEN.to_ascii_lowercase();
    let mut spans = Vec::new();
    let mut from = 0usize;
    while let Some(rel) = lower[from..].find(&open) {
        let start = from + rel;
        let inner = start + open.len();
        let Some(rel_end) = text[inner..].find(']') else {
            break;
        };
        let end = inner + rel_end;
        let payload = text[inner..end].trim();
        if is_marker_payload(payload) {
            spans.push((start, end + 1, payload));
        }
        from = end + 1;
    }
    spans
}

/// A path a client could actually be served: absolute, no `..`, a regular file now.
fn is_servable_file(path: &str) -> bool {
    let p = std::path::Path::new(path);
    p.is_absolute()
        && !p
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        && std::fs::symlink_metadata(p).is_ok_and(|m| m.is_file())
}

/// Rewrite one reply. `authoritative` are the verified render files of this turn,
/// in order; when it is non-empty the model's own image markers are discarded
/// wholesale, otherwise only the ones that could never be served are.
pub(crate) fn rewrite(text: &str, authoritative: &[String]) -> Rewrite {
    rewrite_with(text, authoritative, true)
}

/// `prepend` false applies the same distrust but adds nothing: for an assistant
/// message that is not the turn's final one, so a backfill shows each render once.
fn rewrite_with(text: &str, authoritative: &[String], prepend: bool) -> Rewrite {
    let distrust_model = !authoritative.is_empty();
    let mut kept = Vec::new();
    let mut dropped = Vec::new();
    let mut body = String::with_capacity(text.len());
    let mut cursor = 0usize;
    for (start, end, payload) in image_markers(text) {
        let keep = !distrust_model && is_servable_file(payload);
        if keep {
            kept.push(payload.to_string());
            continue;
        }
        dropped.push(payload.to_string());
        body.push_str(&text[cursor..start]);
        cursor = end;
    }
    body.push_str(&text[cursor..]);

    // A removed marker usually owned its whole line; do not leave that line behind.
    let body = if dropped.is_empty() {
        body
    } else {
        let mut cleaned = String::with_capacity(body.len());
        let mut blank_run = 0usize;
        for line in body.lines() {
            if line.trim().is_empty() {
                blank_run += 1;
                if blank_run > 1 {
                    continue;
                }
            } else {
                blank_run = 0;
            }
            cleaned.push_str(line);
            cleaned.push('\n');
        }
        cleaned.trim_matches('\n').to_string()
    };

    let mut added = Vec::new();
    if prepend {
        for path in authoritative {
            if !added.contains(path) && !kept.contains(path) {
                added.push(path.clone());
            }
        }
    }
    let text = if added.is_empty() {
        body
    } else {
        let head = added
            .iter()
            .map(|p| format!("{IMAGE_OPEN}{p}]"))
            .collect::<Vec<_>>()
            .join("\n");
        if body.trim().is_empty() {
            head
        } else {
            format!("{head}\n\n{body}")
        }
    };
    Rewrite {
        text,
        added,
        kept,
        dropped,
    }
}

/// Only turns that plausibly produced or claimed an image are worth a receipt lookup.
pub(crate) fn needs_reconcile(response: &str, new_messages: &[ConversationMessage]) -> bool {
    let mentions = |s: &str| {
        let lower = s.to_ascii_lowercase();
        lower.contains("[image:") || lower.contains("render: prompt_id=")
    };
    mentions(response)
        || new_messages.iter().any(|m| match m {
            ConversationMessage::ToolResults(results) => {
                results.iter().any(|r| mentions(&r.content))
            }
            _ => false,
        })
}

/// Rewrite the turn's reply and its persisted assistant message(s) in place.
/// `turn_started_unix` windows the receipt lookup to renders begun this turn.
/// A lookup failure degrades to "drop what could never be served" and is logged;
/// it never fails the turn.
pub(crate) async fn reconcile(
    session_key: &str,
    turn_started_unix: u64,
    response: &mut String,
    new_messages: &mut [ConversationMessage],
) {
    if !session_jobs::enabled() || !needs_reconcile(response, new_messages) {
        return;
    }
    let since = turn_started_unix.saturating_sub(1);
    let authoritative: Vec<String> = match session_jobs::deliverables(session_key, since).await {
        Ok(list) => list
            .into_iter()
            .filter(|d| d.kind == "image")
            .map(|d| d.path)
            .collect(),
        Err(message) => {
            ::zeroclaw_log::record!(
                WARN,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                    .with_outcome(::zeroclaw_log::EventOutcome::Unknown)
                    .with_attrs(::serde_json::json!({
                        "session_key": session_key,
                        "since": since,
                        "error": message,
                    })),
                "render receipts unavailable; keeping only servable model markers"
            );
            Vec::new()
        }
    };

    let result = rewrite(response, &authoritative);
    let unchanged = result.added.is_empty() && result.dropped.is_empty();
    if !unchanged {
        *response = result.text.clone();
    }
    // Every assistant message loses untrusted markers; only the last one gains the
    // verified set, so a backfill from the persisted rows shows each render once.
    let last_assistant = new_messages
        .iter()
        .rposition(|m| matches!(m, ConversationMessage::Chat(c) if c.role == "assistant"));
    for (index, message) in new_messages.iter_mut().enumerate() {
        let ConversationMessage::Chat(chat) = message else {
            continue;
        };
        if chat.role != "assistant" {
            continue;
        }
        let r = rewrite_with(&chat.content, &authoritative, Some(index) == last_assistant);
        if !(r.added.is_empty() && r.dropped.is_empty()) {
            chat.content = r.text;
        }
    }
    if unchanged {
        return;
    }
    ::zeroclaw_log::record!(
        INFO,
        ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note).with_attrs(
            ::serde_json::json!({
                "session_key": session_key,
                "since": since,
                "added": result.added,
                "kept": result.kept,
                "dropped": result.dropped,
            })
        ),
        "render markers reconciled from job receipts"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeroclaw_providers::{ChatMessage, ToolResultMessage};

    fn temp_png(dir: &std::path::Path, name: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, b"\x89PNG").unwrap();
        path.display().to_string()
    }

    #[test]
    fn a_fabricated_marker_is_replaced_by_the_receipt_render() {
        let tmp = tempfile::tempdir().unwrap();
        let real = temp_png(tmp.path(), "cg-job-0.png");
        let reply = "[IMAGE:/home/x/.zeroclaw/agents/comfy/workspace/renders/output/cg_078d349a.png]\n\nThere's your taste.";
        let r = rewrite(reply, std::slice::from_ref(&real));
        assert_eq!(r.text, format!("[IMAGE:{real}]\n\nThere's your taste."));
        assert_eq!(r.added, vec![real]);
        assert_eq!(
            r.dropped,
            vec!["/home/x/.zeroclaw/agents/comfy/workspace/renders/output/cg_078d349a.png"]
        );
        assert!(r.kept.is_empty());
    }

    #[test]
    fn an_existing_but_wrong_marker_is_still_replaced_when_a_render_happened() {
        let tmp = tempfile::tempdir().unwrap();
        let stale = temp_png(tmp.path(), "cg_silvermoon_grip.png");
        let fresh = temp_png(tmp.path(), "cg-fresh-0.png");
        let r = rewrite(
            &format!("[IMAGE:{stale}]\nLook at her."),
            std::slice::from_ref(&fresh),
        );
        assert_eq!(r.text, format!("[IMAGE:{fresh}]\n\nLook at her."));
        assert_eq!(r.dropped, vec![stale]);
    }

    #[test]
    fn two_renders_become_two_lines_in_receipt_order_without_duplicates() {
        let tmp = tempfile::tempdir().unwrap();
        let a = temp_png(tmp.path(), "a.png");
        let b = temp_png(tmp.path(), "b.png");
        let r = rewrite("Three variants.", &[a.clone(), b.clone(), a.clone()]);
        assert_eq!(
            r.text,
            format!("[IMAGE:{a}]\n[IMAGE:{b}]\n\nThree variants.")
        );
        assert_eq!(r.added, vec![a, b]);
    }

    #[test]
    fn without_a_render_only_unservable_markers_are_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let real = temp_png(tmp.path(), "old.png");
        let missing = tmp.path().join("gone.png").display().to_string();
        let reply = format!(
            "[IMAGE:{real}]\n[IMAGE:{missing}]\n[image:relative/x.png]\n\nEarlier render, as asked."
        );
        let r = rewrite(&reply, &[]);
        assert_eq!(
            r.text,
            format!("[IMAGE:{real}]\n\nEarlier render, as asked.")
        );
        assert_eq!(r.kept, vec![real]);
        assert_eq!(r.dropped, vec![missing, "relative/x.png".to_string()]);
        assert!(r.added.is_empty());
    }

    #[test]
    fn a_reply_with_nothing_to_change_is_returned_verbatim() {
        let r = rewrite("Just words.\n\nTwo paragraphs.", &[]);
        assert_eq!(r.text, "Just words.\n\nTwo paragraphs.");
        assert_eq!(
            r,
            Rewrite {
                text: "Just words.\n\nTwo paragraphs.".into(),
                ..Default::default()
            }
        );
        // Video markers are not this module's business.
        let r = rewrite("[VIDEO:/nowhere/clip.mp4]\nDone.", &[]);
        assert_eq!(r.text, "[VIDEO:/nowhere/clip.mp4]\nDone.");
    }

    #[test]
    fn a_marker_inside_a_sentence_is_removed_without_eating_the_sentence() {
        let r = rewrite("Saved it as [IMAGE:/nope/x.png] for you.", &[]);
        assert_eq!(r.text, "Saved it as  for you.");
        assert_eq!(r.dropped, vec!["/nope/x.png"]);
    }

    #[test]
    fn prose_about_the_marker_syntax_is_left_alone() {
        // Seen live: the model explaining that it had no marker to give.
        let tmp = tempfile::tempdir().unwrap();
        let real = temp_png(tmp.path(), "r.png");
        let reply = "No `[IMAGE:…]` marker was emitted this turn, and [IMAGE:<path>] is the shape. [IMAGE:] too.";
        let r = rewrite(reply, std::slice::from_ref(&real));
        assert_eq!(r.text, format!("[IMAGE:{real}]\n\n{reply}"));
        assert!(r.dropped.is_empty());
        // A bare file name or a relative path is still a (bad) reference, not prose.
        let r = rewrite("[IMAGE:cg_x.png] and [image:renders/cg_y.jpeg]", &[]);
        assert_eq!(r.dropped, vec!["cg_x.png", "renders/cg_y.jpeg"]);
    }

    #[test]
    fn reconcile_is_only_triggered_by_image_talk_or_a_render_line() {
        let tool = |content: &str| {
            ConversationMessage::ToolResults(vec![ToolResultMessage {
                tool_call_id: "1".into(),
                content: content.into(),
                tool_name: "shell".into(),
            }])
        };
        assert!(needs_reconcile("[IMAGE:/x.png]", &[]));
        assert!(needs_reconcile(
            "",
            &[tool("render: prompt_id=abc seed=1\n[IMAGE:/ws/cg_x.png]")]
        ));
        assert!(needs_reconcile(
            "",
            &[tool("review: OK\nrender: prompt_id=abc seed=1")]
        ));
        assert!(!needs_reconcile("hello", &[tool("$ ls\nfoo.txt")]));
        assert!(!needs_reconcile(
            "hello",
            &[ConversationMessage::Chat(ChatMessage::assistant(
                "[IMAGE:/x.png]"
            ))]
        ));
    }

    #[test]
    fn non_final_assistant_messages_lose_markers_and_only_the_last_gains_them() {
        let tmp = tempfile::tempdir().unwrap();
        let fresh = temp_png(tmp.path(), "fresh.png");
        let mut messages = [
            ConversationMessage::Chat(ChatMessage::assistant(
                "[IMAGE:/bogus/one.png]\nFirst pass.",
            )),
            ConversationMessage::Chat(ChatMessage::user("again")),
            ConversationMessage::Chat(ChatMessage::assistant(
                "[IMAGE:/bogus/two.png]\nSecond pass.",
            )),
        ];
        let authoritative = vec![fresh.clone()];
        let last = messages
            .iter()
            .rposition(|m| matches!(m, ConversationMessage::Chat(c) if c.role == "assistant"));
        for (index, message) in messages.iter_mut().enumerate() {
            let ConversationMessage::Chat(chat) = message else {
                continue;
            };
            if chat.role != "assistant" {
                continue;
            }
            chat.content = rewrite_with(&chat.content, &authoritative, Some(index) == last).text;
        }
        let text = |i: usize| match &messages[i] {
            ConversationMessage::Chat(c) => c.content.clone(),
            _ => unreachable!(),
        };
        assert_eq!(text(0), "First pass.");
        assert_eq!(text(2), format!("[IMAGE:{fresh}]\n\nSecond pass."));
    }
}
