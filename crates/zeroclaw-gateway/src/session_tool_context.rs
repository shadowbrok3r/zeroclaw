//! Tool calls and tool results a `/ws/chat` agent keeps across reconnects.
//!
//! A socket's agent holds every turn's tool calls and results in memory, but a
//! new socket is seeded from the session store, whose transcript is chat text.
//! The store now keeps each turn's tool context beside the transcript
//! (`SessionBackend::append_turn` / `load_conversation`), never in it, so
//! everything that reads the transcript is unchanged. This module bounds what
//! is stored and decides what is seeded back.

use std::borrow::Cow;
use std::collections::HashSet;

use zeroclaw_api::model_provider::{ConversationMessage, ToolCall, ToolResultMessage};
use zeroclaw_runtime::agent::history::truncate_tool_result;

/// Characters stored of each tool result (and of a call's text, arguments and
/// reasoning) when the runtime profile sets no `max_tool_result_chars`.
pub(crate) const DEFAULT_STORED_TOOL_CONTEXT_CHARS: usize = 16_000;

/// Stands in for inline `data:` payloads, which are never stored.
const INLINE_DATA_PLACEHOLDER: &str = "[truncated inline data removed]";

/// Shortest line accepted as the continuation of a line-wrapped payload.
const WRAPPED_PAYLOAD_MIN_LINE: usize = 40;

/// How much tool context an agent's session keeps, resolved from its runtime
/// profile at the moment it is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ToolContextLimits {
    /// Characters kept of each stored text field of tool context.
    pub max_chars: usize,
    /// Newest turns whose tool context is stored and seeded
    /// (`keep_tool_context_turns`). `0` keeps none.
    pub keep_turns: usize,
}

impl ToolContextLimits {
    pub(crate) fn for_agent(config: &zeroclaw_config::schema::Config, agent_alias: &str) -> Self {
        let max_chars = config
            .runtime_profile_for_agent(agent_alias)
            .and_then(|profile| profile.max_tool_result_chars)
            .filter(|&chars| chars > 0)
            .unwrap_or(DEFAULT_STORED_TOOL_CONTEXT_CHARS);
        Self {
            max_chars,
            keep_turns: config.effective_keep_tool_context_turns(agent_alias),
        }
    }
}

/// A finished (or interrupted) turn as it is stored: its chat messages other
/// than system prompts, and each tool call whose results all came back, with
/// those results, bounded. A call without its result, or a result without its
/// call, is left out, so a turn cut short mid-round never seeds a pairing a
/// provider would reject.
pub(crate) fn storable_turn(
    messages: &[ConversationMessage],
    limits: ToolContextLimits,
) -> Vec<ConversationMessage> {
    let answered: HashSet<&str> = messages
        .iter()
        .filter_map(|message| match message {
            ConversationMessage::ToolResults(results) => Some(results),
            _ => None,
        })
        .flatten()
        .map(|result| result.tool_call_id.as_str())
        .collect();
    let complete = |calls: &[ToolCall]| {
        !calls.is_empty() && calls.iter().all(|call| answered.contains(call.id.as_str()))
    };
    let kept_calls: HashSet<&str> = messages
        .iter()
        .filter_map(|message| match message {
            ConversationMessage::AssistantToolCalls { tool_calls, .. }
                if limits.keep_turns > 0 && complete(tool_calls) =>
            {
                Some(tool_calls)
            }
            _ => None,
        })
        .flatten()
        .map(|call| call.id.as_str())
        .collect();

    messages
        .iter()
        .filter_map(|message| match message {
            ConversationMessage::Chat(chat) => {
                (chat.role != "system").then(|| ConversationMessage::Chat(chat.clone()))
            }
            ConversationMessage::AssistantToolCalls {
                text,
                tool_calls,
                reasoning_content,
            } => (!tool_calls.is_empty()
                && tool_calls
                    .iter()
                    .all(|call| kept_calls.contains(call.id.as_str())))
            .then(|| ConversationMessage::AssistantToolCalls {
                text: text.as_deref().map(|text| bounded(text, limits.max_chars)),
                tool_calls: tool_calls
                    .iter()
                    .map(|call| ToolCall {
                        id: call.id.clone(),
                        name: call.name.clone(),
                        arguments: bounded_arguments(&call.arguments, limits.max_chars),
                        extra_content: call.extra_content.clone(),
                    })
                    .collect(),
                reasoning_content: reasoning_content
                    .as_deref()
                    .map(|reasoning| bounded(reasoning, limits.max_chars)),
            }),
            ConversationMessage::ToolResults(results) => {
                let kept: Vec<ToolResultMessage> = results
                    .iter()
                    .filter(|result| kept_calls.contains(result.tool_call_id.as_str()))
                    .map(|result| ToolResultMessage {
                        tool_call_id: result.tool_call_id.clone(),
                        content: bounded(&result.content, limits.max_chars),
                        tool_name: result.tool_name.clone(),
                    })
                    .collect();
                (!kept.is_empty()).then_some(ConversationMessage::ToolResults(kept))
            }
        })
        .collect()
}

/// Drop the tool calls and results of every turn before the newest
/// `keep_turns`. A turn starts at a user chat message, the boundary whole-turn
/// history trimming uses, so chat text is never touched here.
pub(crate) fn retain_recent_tool_context(
    history: Vec<ConversationMessage>,
    keep_turns: usize,
) -> Vec<ConversationMessage> {
    let is_turn_start = |message: &ConversationMessage| matches!(message, ConversationMessage::Chat(chat) if chat.role == "user");
    let first_kept = match keep_turns.checked_sub(1) {
        None => history.len(),
        Some(older) => history
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, message)| is_turn_start(message))
            .nth(older)
            .map_or(0, |(index, _)| index),
    };
    history
        .into_iter()
        .enumerate()
        .filter(|(index, message)| {
            *index >= first_kept || matches!(message, ConversationMessage::Chat(_))
        })
        .map(|(_, message)| message)
        .collect()
}

fn bounded(text: &str, max_chars: usize) -> String {
    truncate_tool_result(&strip_inline_data(text), max_chars)
}

/// Arguments are JSON a provider parses again when the call is replayed, so an
/// over-long one is wrapped, not cut.
fn bounded_arguments(arguments: &str, max_chars: usize) -> String {
    let stripped = strip_inline_data(arguments);
    if stripped.len() <= max_chars {
        return stripped.into_owned();
    }
    serde_json::json!({ "truncated_arguments": truncate_tool_result(&stripped, max_chars) })
        .to_string()
}

/// Replace every inline `data:<type>;base64,<payload>` run, whole
/// `[IMAGE:data:…]` markers included, with [`INLINE_DATA_PLACEHOLDER`]. A
/// payload continues across a line break only onto a line made entirely of
/// base64 characters and at least [`WRAPPED_PAYLOAD_MIN_LINE`] long. Linear in
/// the length of `text`.
fn strip_inline_data(text: &str) -> Cow<'_, str> {
    const SCHEME: &str = "data:";
    const MARKER: &str = "[IMAGE:";
    let is_header_char = |ch: char| {
        ch.is_ascii_alphanumeric() || matches!(ch, '/' | '+' | '.' | '-' | '_' | ';' | '=')
    };
    let is_payload_char =
        |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=');

    let mut out: Option<String> = None;
    let mut copied = 0usize;
    let mut scan = 0usize;
    while let Some(relative) = text[scan..].find(SCHEME) {
        let start = scan + relative;
        let header_start = start + SCHEME.len();
        let header_end = text[header_start..]
            .find(|ch: char| !is_header_char(ch))
            .map_or(text.len(), |len| header_start + len);
        let mut parameters = text[header_start..header_end].split(';');
        let is_data_uri = parameters.next().is_some_and(|media| !media.is_empty())
            && parameters.any(|parameter| parameter == "base64")
            && text[header_end..].starts_with(',');
        if !is_data_uri {
            scan = header_start;
            continue;
        }

        let bytes = text.as_bytes();
        let payload_run = |from: usize| {
            bytes[from..]
                .iter()
                .position(|&byte| !is_payload_char(byte))
                .map_or(bytes.len(), |len| from + len)
        };
        let mut end = payload_run(header_end + 1);
        loop {
            let line_start = if text[end..].starts_with("\r\n") {
                end + 2
            } else if text[end..].starts_with('\n') {
                end + 1
            } else {
                break;
            };
            let line_end = payload_run(line_start);
            let whole_line = line_end == bytes.len() || matches!(bytes[line_end], b'\n' | b'\r');
            if !whole_line || line_end - line_start < WRAPPED_PAYLOAD_MIN_LINE {
                break;
            }
            end = line_end;
        }

        let mut replace_from = start;
        if text[..start].ends_with(MARKER) && text[end..].starts_with(']') {
            replace_from = start - MARKER.len();
            end += 1;
        }
        let buffer = out.get_or_insert_with(|| String::with_capacity(text.len()));
        buffer.push_str(&text[copied..replace_from]);
        buffer.push_str(INLINE_DATA_PLACEHOLDER);
        copied = end;
        scan = end;
    }

    match out {
        Some(mut buffer) => {
            buffer.push_str(&text[copied..]);
            Cow::Owned(buffer)
        }
        None => Cow::Borrowed(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeroclaw_api::model_provider::ChatMessage;

    fn call(id: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            name: "file_read".to_string(),
            arguments: arguments.to_string(),
            extra_content: None,
        }
    }

    fn result(id: &str, content: &str) -> ToolResultMessage {
        ToolResultMessage {
            tool_call_id: id.to_string(),
            content: content.to_string(),
            tool_name: "file_read".to_string(),
        }
    }

    fn turn(n: usize) -> Vec<ConversationMessage> {
        vec![
            ConversationMessage::Chat(ChatMessage::user(format!("ask {n}"))),
            ConversationMessage::AssistantToolCalls {
                text: None,
                tool_calls: vec![call(&format!("call-{n}"), "{}")],
                reasoning_content: None,
            },
            ConversationMessage::ToolResults(vec![result(&format!("call-{n}"), "out")]),
            ConversationMessage::Chat(ChatMessage::assistant(format!("reply {n}"))),
        ]
    }

    fn tool_ids(history: &[ConversationMessage]) -> Vec<String> {
        history
            .iter()
            .flat_map(|message| match message {
                ConversationMessage::AssistantToolCalls { tool_calls, .. } => tool_calls
                    .iter()
                    .map(|call| format!("call:{}", call.id))
                    .collect(),
                ConversationMessage::ToolResults(results) => results
                    .iter()
                    .map(|result| format!("result:{}", result.tool_call_id))
                    .collect(),
                ConversationMessage::Chat(_) => Vec::new(),
            })
            .collect()
    }

    const LIMITS: ToolContextLimits = ToolContextLimits {
        max_chars: 16_000,
        keep_turns: 50,
    };

    #[test]
    fn a_stored_result_is_capped_with_the_truncation_marker() {
        let huge = format!("head {} tail", "x".repeat(52_000));
        let stored = storable_turn(
            &[
                ConversationMessage::Chat(ChatMessage::user("ask")),
                ConversationMessage::AssistantToolCalls {
                    text: Some("looking".into()),
                    tool_calls: vec![call("call-1", r#"{"path":"a"}"#)],
                    reasoning_content: Some("r".repeat(40_000)),
                },
                ConversationMessage::ToolResults(vec![result("call-1", &huge)]),
            ],
            LIMITS,
        );

        let ConversationMessage::ToolResults(results) = &stored[2] else {
            panic!("tool results are stored: {stored:?}");
        };
        let content = &results[0].content;
        assert!(content.len() < 16_200, "capped: {} chars", content.len());
        assert!(content.starts_with("head ") && content.ends_with(" tail"));
        assert!(content.contains("characters truncated"));
        let ConversationMessage::AssistantToolCalls {
            text,
            tool_calls,
            reasoning_content,
        } = &stored[1]
        else {
            panic!("the call is stored: {stored:?}");
        };
        assert_eq!(text.as_deref(), Some("looking"));
        assert_eq!(tool_calls[0].arguments, r#"{"path":"a"}"#);
        assert!(reasoning_content.as_ref().unwrap().len() < 16_200);
    }

    #[test]
    fn over_long_arguments_stay_json() {
        let arguments = serde_json::json!({ "content": "y".repeat(30_000) }).to_string();
        let bounded = bounded_arguments(&arguments, 16_000);
        let parsed: serde_json::Value = serde_json::from_str(&bounded).expect("still JSON");
        assert!(
            parsed["truncated_arguments"]
                .as_str()
                .unwrap()
                .contains("characters truncated")
        );
    }

    #[test]
    fn inline_data_is_never_stored() {
        let payload = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
        let marker = format!("see [IMAGE:data:image/png;base64,{payload}] done");
        assert_eq!(
            strip_inline_data(&marker),
            "see [truncated inline data removed] done"
        );
        let bare = format!("{{\"src\":\"data:image/png;base64,{payload}\"}}");
        assert_eq!(
            strip_inline_data(&bare),
            "{\"src\":\"[truncated inline data removed]\"}"
        );
        let line = "A".repeat(64);
        let wrapped = format!("data:image/png;base64,{line}\n{line}\n{line}\nnext line");
        assert_eq!(
            strip_inline_data(&wrapped),
            "[truncated inline data removed]\nnext line"
        );
        let prose = "a data: field, data:text/plain,hello and [IMAGE:/tmp/a.png]";
        assert!(matches!(strip_inline_data(prose), Cow::Borrowed(_)));
        assert_eq!(strip_inline_data("data:data:data:"), "data:data:data:");
    }

    #[test]
    fn a_call_without_its_result_is_left_out_with_any_stray_result() {
        let stored = storable_turn(
            &[
                ConversationMessage::Chat(ChatMessage::system("prompt")),
                ConversationMessage::Chat(ChatMessage::user("ask")),
                ConversationMessage::AssistantToolCalls {
                    text: None,
                    tool_calls: vec![call("done", "{}")],
                    reasoning_content: None,
                },
                ConversationMessage::ToolResults(vec![result("done", "ok")]),
                ConversationMessage::AssistantToolCalls {
                    text: None,
                    tool_calls: vec![call("half-a", "{}"), call("half-b", "{}")],
                    reasoning_content: None,
                },
                ConversationMessage::ToolResults(vec![
                    result("half-a", "ok"),
                    result("stray", "ok"),
                ]),
                ConversationMessage::Chat(ChatMessage::assistant("partial")),
            ],
            LIMITS,
        );

        assert_eq!(tool_ids(&stored), ["call:done", "result:done"]);
        assert_eq!(
            stored.len(),
            4,
            "system prompt dropped, chat kept: {stored:?}"
        );
    }

    #[test]
    fn no_tool_context_is_stored_when_the_profile_keeps_none() {
        let stored = storable_turn(
            &turn(1),
            ToolContextLimits {
                keep_turns: 0,
                ..LIMITS
            },
        );
        assert!(tool_ids(&stored).is_empty());
        assert_eq!(stored.len(), 2);
    }

    #[test]
    fn only_the_newest_turns_keep_their_tool_context() {
        let history: Vec<ConversationMessage> = (1..=3).flat_map(turn).collect();

        let kept = retain_recent_tool_context(history.clone(), 2);
        assert_eq!(
            tool_ids(&kept),
            [
                "call:call-2",
                "result:call-2",
                "call:call-3",
                "result:call-3"
            ]
        );
        assert_eq!(kept.len(), 10, "turn 1 keeps its chat text");

        assert_eq!(
            tool_ids(&retain_recent_tool_context(history.clone(), 50)).len(),
            6
        );
        let none = retain_recent_tool_context(history, 0);
        assert!(tool_ids(&none).is_empty());
        assert_eq!(none.len(), 6);
    }

    #[test]
    fn limits_follow_the_runtime_profile() {
        let mut config = zeroclaw_config::schema::Config::default();
        config.runtime_profiles.insert(
            "comfy".into(),
            zeroclaw_config::schema::RuntimeProfileConfig {
                keep_tool_context_turns: Some(50),
                ..Default::default()
            },
        );
        config.agents.insert(
            "comfy".into(),
            zeroclaw_config::schema::AliasedAgentConfig {
                runtime_profile: "comfy".into(),
                ..Default::default()
            },
        );
        assert_eq!(
            ToolContextLimits::for_agent(&config, "comfy"),
            ToolContextLimits {
                max_chars: DEFAULT_STORED_TOOL_CONTEXT_CHARS,
                keep_turns: 50,
            }
        );

        config
            .runtime_profiles
            .get_mut("comfy")
            .unwrap()
            .max_tool_result_chars = Some(4_000);
        assert_eq!(
            ToolContextLimits::for_agent(&config, "comfy").max_chars,
            4_000
        );
    }
}
