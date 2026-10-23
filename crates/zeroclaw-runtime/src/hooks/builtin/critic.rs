//! Critic hook: an external reviewer that watches a listed agent's turn and can
//! steer it.
//!
//! Before each model call, the turn's non-system messages go to the critic
//! service (`POST {url}` with `{agent, messages}`). The service decides when a
//! review is due and answers `{"verdict": ..., "note"?: ...}`; a `note` is
//! appended to this one request as a user message, so the model reads it now
//! while the stored history stays as it was. A slow, failed or note-less reply
//! leaves the call untouched — the critic can only add advice, never block.

use async_trait::async_trait;
use serde_json::{Value, json};
use std::time::Duration;

use crate::hooks::traits::{HookHandler, HookResult};
use zeroclaw_api::model_provider::ChatMessage;
use zeroclaw_config::schema::CriticHookConfig;

pub struct CriticHook {
    url: String,
    agent: String,
    client: reqwest::Client,
}

impl CriticHook {
    pub fn new(config: &CriticHookConfig, agent: &str) -> Result<Self, String> {
        let url =
            reqwest::Url::parse(&config.url).map_err(|e| format!("invalid critic URL: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("critic URL must be http(s), got {}", url.scheme()));
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs.max(1)))
            .build()
            .map_err(|e| format!("critic HTTP client: {e}"))?;
        Ok(Self {
            url: config.url.clone(),
            agent: agent.to_string(),
            client,
        })
    }

    /// Whether the config enables the critic for `agent`.
    pub fn applies(config: &CriticHookConfig, agent: &str) -> bool {
        config.enabled && !agent.is_empty() && config.agents.iter().any(|a| a == agent)
    }

    async fn review(&self, messages: &[ChatMessage]) -> Option<String> {
        let body = json!({
            "agent": self.agent,
            "messages": messages
                .iter()
                .filter(|m| m.role != "system")
                .map(|m| json!({"role": m.role, "content": m.content}))
                .collect::<Vec<_>>(),
        });
        let reply: Value = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        reply["note"]
            .as_str()
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(String::from)
    }
}

#[async_trait]
impl HookHandler for CriticHook {
    fn name(&self) -> &str {
        "critic"
    }

    async fn before_llm_call(
        &self,
        messages: &mut Vec<ChatMessage>,
        _model: &mut String,
    ) -> HookResult<()> {
        if let Some(note) = self.review(messages).await {
            ::zeroclaw_log::record!(
                INFO,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Note)
                    .with_attrs(json!({"hook": "critic", "agent": self.agent, "note": note})),
                "critic note added to the model request"
            );
            messages.push(ChatMessage::user(note));
        }
        HookResult::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn config(url: String) -> CriticHookConfig {
        CriticHookConfig {
            enabled: true,
            url,
            agents: vec!["reverse".into()],
            timeout_secs: 2,
        }
    }

    /// One-shot HTTP stub: records the request body, answers `reply`.
    async fn stub(reply: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/review", listener.local_addr().unwrap());
        let task = zeroclaw_spawn::spawn!(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 65536];
            let mut got = Vec::new();
            loop {
                let n = sock.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got).to_string();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let len = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if body.len() >= len {
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{reply}",
                            reply.len()
                        );
                        sock.write_all(resp.as_bytes()).await.unwrap();
                        return body.to_string();
                    }
                }
            }
        });
        (url, task)
    }

    #[test]
    fn applies_only_to_listed_agents_when_enabled() {
        let mut c = config("http://127.0.0.1:1/review".into());
        assert!(CriticHook::applies(&c, "reverse"));
        assert!(!CriticHook::applies(&c, "coder"));
        assert!(!CriticHook::applies(&c, ""));
        c.enabled = false;
        assert!(!CriticHook::applies(&c, "reverse"));
    }

    #[tokio::test]
    async fn a_note_is_appended_and_system_messages_are_not_sent() {
        let (url, server) = stub(r#"{"verdict":"nudge","note":"[critic] stop looping"}"#).await;
        let hook = CriticHook::new(&config(url), "reverse").unwrap();
        let mut msgs = vec![
            ChatMessage::system("secret system prompt"),
            ChatMessage::user("task"),
        ];
        let mut model = "m".to_string();
        assert!(matches!(
            hook.before_llm_call(&mut msgs, &mut model).await,
            HookResult::Continue(())
        ));
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[2].role, "user");
        assert_eq!(msgs[2].content, "[critic] stop looping");
        let sent: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
        assert_eq!(sent["agent"], "reverse");
        assert_eq!(sent["messages"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn no_note_or_no_service_leaves_the_call_alone() {
        let (url, server) = stub(r#"{"verdict":"skip"}"#).await;
        let hook = CriticHook::new(&config(url), "reverse").unwrap();
        let mut msgs = vec![ChatMessage::user("task")];
        let mut model = "m".to_string();
        hook.before_llm_call(&mut msgs, &mut model).await;
        server.await.unwrap();
        assert_eq!(msgs.len(), 1);

        let dead = CriticHook::new(&config("http://127.0.0.1:9/review".into()), "reverse").unwrap();
        dead.before_llm_call(&mut msgs, &mut model).await;
        assert_eq!(msgs.len(), 1);
    }
}
