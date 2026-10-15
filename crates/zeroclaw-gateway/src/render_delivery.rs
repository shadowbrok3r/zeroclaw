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
//!
//! Which renders: when the reply names renders of this turn (by label, file
//! name or path, with or without the `cg_` prefix) only those are delivered.
//! Otherwise the newest [`MAX_FORCED`] are, with a line saying how many more
//! the turn made. A turn that re-rolled one frame thirty times used to attach
//! all thirty-six images (2026-10-15).

use zeroclaw_providers::ConversationMessage;

use crate::session_jobs;

const IMAGE_OPEN: &str = "[IMAGE:";

/// Renders attached when the reply names none of this turn's renders.
pub(crate) const MAX_FORCED: usize = 4;

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
        || is_bare_label(payload)
}

/// A render label the model wrote without a path or extension
/// (`cg_beach_frame1ac`): identifier characters only, and a `_`, `-` or digit,
/// so a lone prose word (`[IMAGE:path]`) is not mistaken for one.
fn is_bare_label(payload: &str) -> bool {
    payload.len() >= 3
        && payload
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        && payload
            .chars()
            .any(|c| c == '_' || c == '-' || c.is_ascii_digit())
}

/// A render reference reduced for matching: file name only, lowercase, no image
/// extension, no `cg_` / `cg-` prefix.
fn norm_ref(s: &str) -> String {
    let base = s
        .rsplit('/')
        .next()
        .unwrap_or(s)
        .trim()
        .to_ascii_lowercase();
    let base = [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"]
        .iter()
        .find_map(|ext| base.strip_suffix(ext))
        .unwrap_or(&base)
        .to_string();
    base.strip_prefix("cg_")
        .or_else(|| base.strip_prefix("cg-"))
        .map(str::to_string)
        .unwrap_or(base)
}

/// One render of the turn: its `--out` label and the file to deliver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Render {
    pub label: String,
    pub path: String,
}

/// The renders to attach, newest first as given, and how many were left out.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Selection {
    pub paths: Vec<String>,
    pub omitted: usize,
}

/// Does the model's marker payload name exactly this render (path, label or file name)?
fn names_exactly(payload: &str, render: &Render) -> bool {
    if payload == render.path {
        return true;
    }
    let want = norm_ref(payload);
    !want.is_empty() && (want == norm_ref(&render.label) || want == norm_ref(&render.path))
}

/// Does the payload name this render by its gallery file name,
/// `cg_<label>_<backend>_<epoch>_00001_`?
fn names_by_gallery_name(payload: &str, render: &Render) -> bool {
    let label = norm_ref(&render.label);
    !label.is_empty() && norm_ref(payload).starts_with(&format!("{label}_"))
}

/// Pick what to attach: the renders the reply names, in the reply's order;
/// otherwise the newest [`MAX_FORCED`].
pub(crate) fn select(renders: &[Render], model_payloads: &[&str]) -> Selection {
    let mut named: Vec<String> = Vec::new();
    for payload in model_payloads {
        let exact: Vec<&Render> = renders
            .iter()
            .filter(|r| names_exactly(payload, r))
            .collect();
        let hits = if exact.is_empty() {
            renders
                .iter()
                .filter(|r| names_by_gallery_name(payload, r))
                .collect()
        } else {
            exact
        };
        for render in hits {
            if !named.contains(&render.path) {
                named.push(render.path.clone());
            }
        }
    }
    if !named.is_empty() {
        return Selection {
            paths: named,
            omitted: 0,
        };
    }
    let mut unique: Vec<String> = Vec::new();
    for render in renders {
        if !unique.contains(&render.path) {
            unique.push(render.path.clone());
        }
    }
    let omitted = unique.len().saturating_sub(MAX_FORCED);
    unique.truncate(MAX_FORCED);
    Selection {
        paths: unique,
        omitted,
    }
}

/// The line that says how many renders were not attached.
fn omitted_line(omitted: usize) -> String {
    let noun = if omitted == 1 { "render" } else { "renders" };
    format!("(+{omitted} more {noun} from this turn not attached)")
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
    let renders: Vec<Render> = match session_jobs::deliverables(session_key, since).await {
        Ok(list) => list
            .into_iter()
            .filter(|d| d.kind == "image")
            .map(|d| Render {
                label: d.label,
                path: d.path,
            })
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

    let payloads: Vec<&str> = image_markers(response)
        .into_iter()
        .map(|(_, _, p)| p)
        .collect();
    let selection = select(&renders, &payloads);
    let authoritative = selection.paths.clone();
    let mut result = rewrite(response, &authoritative);
    if selection.omitted > 0 {
        result.text = format!(
            "{}\n\n{}",
            result.text.trim_end(),
            omitted_line(selection.omitted)
        );
    }
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
        let is_last = Some(index) == last_assistant;
        let mut r = rewrite_with(&chat.content, &authoritative, is_last);
        if is_last && selection.omitted > 0 {
            // `added` is non-empty here (the newest renders were prepended), so the row is rewritten.
            r.text = format!(
                "{}\n\n{}",
                r.text.trim_end(),
                omitted_line(selection.omitted)
            );
        }
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
                "omitted": selection.omitted,
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

    fn render(label: &str, path: &str) -> Render {
        Render {
            label: label.into(),
            path: path.into(),
        }
    }

    #[test]
    fn the_render_the_reply_names_by_label_is_the_only_one_attached() {
        // 2026-10-15: 33 re-rolls and edits; the reply named only the final edit.
        let mut renders: Vec<Render> = (0..33)
            .map(|i| render(&format!("beach_frame1{i}"), &format!("/r/cg-{i}-0.png")))
            .collect();
        renders.insert(
            0,
            render("beach_missionary_pov_frame1ac", "/r/cg-final-0.png"),
        );
        let s = select(&renders, &["cg_beach_missionary_pov_frame1ac"]);
        assert_eq!(
            s,
            Selection {
                paths: vec!["/r/cg-final-0.png".into()],
                omitted: 0
            }
        );
        // By gallery file name or delivered path, too.
        let s = select(
            &renders,
            &["/g/cg_beach_missionary_pov_frame1ac_nvidia_1792_00001_.png"],
        );
        assert_eq!(s.paths, vec!["/r/cg-final-0.png".to_string()]);
        let s = select(&renders, &["/r/cg-final-0.png"]);
        assert_eq!(s.paths, vec!["/r/cg-final-0.png".to_string()]);
    }

    #[test]
    fn an_exact_label_wins_over_a_longer_label_that_shares_its_prefix() {
        let renders = [render("frame1_b", "/r/b.png"), render("frame1", "/r/a.png")];
        assert_eq!(
            select(&renders, &["cg_frame1"]).paths,
            vec!["/r/a.png".to_string()]
        );
    }

    #[test]
    fn with_no_named_render_only_the_newest_four_are_attached_and_the_rest_counted() {
        let renders: Vec<Render> = (0..36)
            .map(|i| render(&format!("l{i}"), &format!("/r/{i}.png")))
            .collect();
        let s = select(&renders, &[]);
        assert_eq!(
            s.paths,
            vec!["/r/0.png", "/r/1.png", "/r/2.png", "/r/3.png"]
        );
        assert_eq!(s.omitted, 32);
        assert_eq!(
            omitted_line(32),
            "(+32 more renders from this turn not attached)"
        );
        // A marker naming nothing of this turn changes nothing about the fallback.
        assert_eq!(select(&renders, &["cg_something_else"]).omitted, 32);
        // Few renders: all of them, nothing omitted.
        let two = [render("a1", "/r/a.png"), render("b1", "/r/b.png")];
        assert_eq!(
            select(&two, &[]),
            Selection {
                paths: vec!["/r/a.png".into(), "/r/b.png".into()],
                omitted: 0
            }
        );
    }

    #[test]
    fn a_bare_label_is_a_marker_but_a_lone_prose_word_is_not() {
        assert!(is_marker_payload("cg_beach_missionary_pov_frame1ac"));
        assert!(!is_marker_payload("path"));
        assert!(!is_marker_payload("<path>"));
        assert!(!is_marker_payload("…"));
        // The label marker the model wrote is replaced by the delivered file.
        let tmp = tempfile::tempdir().unwrap();
        let real = temp_png(tmp.path(), "cg-final-0.png");
        let r = rewrite(
            "[IMAGE:cg_beach_frame1ac]\n\nThat's frame 1.",
            std::slice::from_ref(&real),
        );
        assert_eq!(r.text, format!("[IMAGE:{real}]\n\nThat's frame 1."));
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
