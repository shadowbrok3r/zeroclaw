use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use zeroclaw_api::tool::{Tool, ToolOutput, ToolResult};
use zeroclaw_config::policy::SecurityPolicy;
use zeroclaw_config::policy::ToolOperation;
use zeroclaw_memory::{Memory, MemoryCategory};

/// Let the agent store memories — its own brain writes
pub struct MemoryStoreTool {
    memory: Arc<dyn Memory>,
    security: Arc<SecurityPolicy>,
}

impl MemoryStoreTool {
    pub fn new(memory: Arc<dyn Memory>, security: Arc<SecurityPolicy>) -> Self {
        Self { memory, security }
    }
}

#[async_trait]
impl Tool for MemoryStoreTool {
    fn name(&self) -> &str {
        "memory_store"
    }

    fn description(&self) -> &str {
        "Store a fact, preference, or note in long-term memory. Use category 'core' for permanent facts, 'daily' for session notes, 'conversation' for chat context, or a custom category name."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Unique key for this memory (e.g. 'user_lang', 'project_stack')"
                },
                "content": {
                    "type": "string",
                    "description": "The information to remember"
                },
                "category": {
                    "type": "string",
                    "description": "Memory category: 'core' (permanent), 'daily' (session), 'conversation' (chat), or a custom category name. Defaults to 'core'."
                },
                "supersedes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "maxItems": 20,
                    "description": crate::i18n::get_required_tool_string("tool-memory-store-supersedes")
                }
            },
            "required": ["key", "content"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> anyhow::Result<ToolResult> {
        let key = args.get("key").and_then(|v| v.as_str()).ok_or_else(|| {
            ::zeroclaw_log::record!(
                WARN,
                ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                    .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                    .with_attrs(::serde_json::json!({"param": "key"})),
                "memory_store: missing key parameter"
            );
            anyhow::Error::msg("Missing 'key' parameter")
        })?;

        let content = args
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ::zeroclaw_log::record!(
                    WARN,
                    ::zeroclaw_log::Event::new(module_path!(), ::zeroclaw_log::Action::Reject)
                        .with_outcome(::zeroclaw_log::EventOutcome::Failure)
                        .with_attrs(::serde_json::json!({"param": "content"})),
                    "memory_store: missing content parameter"
                );
                anyhow::Error::msg("Missing 'content' parameter")
            })?;

        let category = match args.get("category").and_then(|v| v.as_str()) {
            Some("core") | None => MemoryCategory::Core,
            Some("daily") => MemoryCategory::Daily,
            Some("conversation") => MemoryCategory::Conversation,
            Some(other) => MemoryCategory::Custom(other.to_string()),
        };

        if let Err(error) = self
            .security
            .enforce_tool_operation(ToolOperation::Act, "memory_store")
        {
            return Ok(ToolResult {
                success: false,
                output: ToolOutput::default(),
                error: Some(error),
            });
        }

        let supersedes: Vec<String> = match args.get("supersedes") {
            Some(value) => serde_json::from_value(value.clone())?,
            None => Vec::new(),
        };
        anyhow::ensure!(
            supersedes.len() <= 20
                && supersedes
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    == supersedes.len(),
            "{}",
            crate::i18n::get_required_tool_string("tool-memory-store-invalid-predecessors")
        );
        // Keep the old record under its old key. A same-key upsert would erase
        // that evidence before a correction could soft-hide it.
        let mut previous = Vec::new();
        if !supersedes.is_empty() {
            let entries = self
                .memory
                .export(&zeroclaw_memory::ExportFilter::default())
                .await?;
            for id in &supersedes {
                let entry = entries
                    .iter()
                    .find(|e| &e.id == id && e.superseded_by.is_none())
                    .ok_or_else(|| {
                        anyhow::Error::msg(crate::i18n::get_required_tool_string(
                            "tool-memory-store-missing-predecessor"
                        ))
                    })?;
                anyhow::ensure!(
                    entry.key != key,
                    "{}",
                    crate::i18n::get_required_tool_string("tool-memory-store-distinct-key")
                );
                previous.push(entry.clone());
            }
            anyhow::ensure!(
                self.memory.get(key).await?.is_none(),
                "{}",
                crate::i18n::get_required_tool_string("tool-memory-store-distinct-key")
            );
        }

        match self.memory.store(key, content, category, None).await {
            Ok(()) if !supersedes.is_empty() => {
                let result = self.memory.supersede(&supersedes, key).await;
                let mut retired = result.is_ok();
                // Some backends have a no-op trait default. Never report a
                // correction as completed unless the old rows are hidden.
                if retired {
                    for old in &previous {
                        let entry = match old.agent_id.as_deref() {
                            Some(agent) => self.memory.get_for_agent(&old.key, agent).await?,
                            None => self.memory.get(&old.key).await?,
                        };
                        retired &=
                            entry.is_some_and(|e| e.id == old.id && e.superseded_by.is_some());
                    }
                }
                let output = crate::i18n::get_required_tool_string_with_args(
                    if retired {
                        "tool-memory-store-corrected"
                    } else {
                        "tool-memory-store-correction-incomplete"
                    },
                    &[("key", key), ("count", &supersedes.len().to_string())],
                );
                Ok(ToolResult {
                    success: retired,
                    output: output.into(),
                    error: result.err().map(|e| e.to_string()),
                })
            }
            Ok(()) => Ok(ToolResult {
                success: true,
                output: format!("Stored memory: {key}").into(),
                error: None,
            }),
            Err(e) => Ok(ToolResult {
                success: false,
                output: ToolOutput::default(),
                error: Some(format!("Failed to store memory: {e}")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use zeroclaw_config::autonomy::AutonomyLevel;
    use zeroclaw_config::policy::SecurityPolicy;
    use zeroclaw_memory::SqliteMemory;

    fn test_security() -> Arc<SecurityPolicy> {
        Arc::new(SecurityPolicy::default())
    }

    fn test_mem() -> (TempDir, Arc<dyn Memory>) {
        let tmp = TempDir::new().unwrap();
        let mem = SqliteMemory::new("test", tmp.path()).unwrap();
        (tmp, Arc::new(mem))
    }

    #[test]
    fn name_and_schema() {
        let (_tmp, mem) = test_mem();
        let tool = MemoryStoreTool::new(mem, test_security());
        assert_eq!(tool.name(), "memory_store");
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["key"].is_object());
        assert!(schema["properties"]["content"].is_object());
    }

    #[tokio::test]
    async fn store_core() {
        let (_tmp, mem) = test_mem();
        let tool = MemoryStoreTool::new(mem.clone(), test_security());
        let result = tool
            .execute(json!({"key": "lang", "content": "Prefers Rust"}))
            .await
            .unwrap();
        assert!(result.success);
        assert!(result.output.contains("lang"));

        let entry = mem.get("lang").await.unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().content, "Prefers Rust");
    }

    #[tokio::test]
    async fn explicit_correction_keeps_history_and_compatible_facts() {
        use zeroclaw_memory::agent_scoped::AgentScopedMemory;
        let tmp = TempDir::new().unwrap();
        let inner = Arc::new(SqliteMemory::new("test", tmp.path()).unwrap());
        let id = inner.ensure_agent_uuid("artist").await.unwrap();
        let mem: Arc<dyn Memory> = Arc::new(AgentScopedMemory::new(inner, id, []));
        mem.store(
            "backend@old",
            "Rendering uses the old host",
            MemoryCategory::Core,
            None,
        )
        .await
        .unwrap();
        mem.store(
            "compatible",
            "Rendering keeps PNG originals",
            MemoryCategory::Core,
            None,
        )
        .await
        .unwrap();
        let old = mem.get("backend@old").await.unwrap().unwrap();
        // SQLite list() previews only the newest 1000 rows. A real correction
        // must still find and retire older evidence through the scoped backend.
        for i in 0..1001 {
            mem.store(
                &format!("scratch/{i}"),
                "Temporary scratch note",
                MemoryCategory::Daily,
                None,
            )
            .await
            .unwrap();
        }
        assert!(
            mem.list(None, None)
                .await
                .unwrap()
                .iter()
                .all(|e| e.id != old.id)
        );
        let tool = MemoryStoreTool::new(mem.clone(), test_security());
        let same_key = tool
            .execute(json!({"key":"backend@old", "content":"Overwrite", "supersedes":[old.id]}))
            .await;
        assert!(same_key.is_err());
        assert_eq!(
            mem.get("backend@old").await.unwrap().unwrap().content,
            old.content
        );
        let result = tool.execute(json!({"key":"backend@new", "content":"Source: explicit user correction; rendering uses the new host", "supersedes":[old.id]})).await.unwrap();
        assert!(result.success, "{:?}", result.error);
        let winner = mem.get("backend@new").await.unwrap().unwrap();
        let archived = mem.get("backend@old").await.unwrap().unwrap();
        assert_eq!(archived.content, old.content);
        assert_eq!(archived.superseded_by.as_deref(), Some(winner.id.as_str()));
        let recalled = mem.recall("Rendering", 10, None, None, None).await.unwrap();
        assert!(recalled.iter().all(|e| e.id != old.id));
        assert!(recalled.iter().any(|e| e.key == "compatible"));
    }

    #[tokio::test]
    async fn correction_cannot_retire_a_readable_peer_memory() {
        use zeroclaw_memory::agent_scoped::AgentScopedMemory;
        let tmp = TempDir::new().unwrap();
        let inner = Arc::new(SqliteMemory::new("test", tmp.path()).unwrap());
        let own = inner.ensure_agent_uuid("artist").await.unwrap();
        let peer = inner.ensure_agent_uuid("peer").await.unwrap();
        inner
            .store_with_agent(
                "peer-fact",
                "Peer likes blue",
                MemoryCategory::Core,
                None,
                None,
                None,
                Some(&peer),
            )
            .await
            .unwrap();
        let mem: Arc<dyn Memory> =
            Arc::new(AgentScopedMemory::new(inner.clone(), own, [peer.clone()]));
        let old = mem.get("peer-fact").await.unwrap().unwrap();
        let tool = MemoryStoreTool::new(mem, test_security());
        let result = tool.execute(json!({"key":"untrusted-replacement", "content":"Peer likes red", "supersedes":[old.id]})).await.unwrap();
        assert!(!result.success);
        assert!(result.output.contains("could not be verified"));
        assert!(
            inner
                .get_for_agent("peer-fact", &peer)
                .await
                .unwrap()
                .unwrap()
                .superseded_by
                .is_none()
        );
    }

    #[tokio::test]
    async fn store_with_category() {
        let (_tmp, mem) = test_mem();
        let tool = MemoryStoreTool::new(mem.clone(), test_security());
        let result = tool
            .execute(json!({"key": "note", "content": "Fixed bug", "category": "daily"}))
            .await
            .unwrap();
        assert!(result.success);
    }

    #[tokio::test]
    async fn store_with_custom_category() {
        let (_tmp, mem) = test_mem();
        let tool = MemoryStoreTool::new(mem.clone(), test_security());
        let result = tool
            .execute(
                json!({"key": "proj_note", "content": "Uses async runtime", "category": "project"}),
            )
            .await
            .unwrap();
        assert!(result.success);

        let entry = mem.get("proj_note").await.unwrap().unwrap();
        assert_eq!(entry.content, "Uses async runtime");
        assert_eq!(entry.category, MemoryCategory::Custom("project".into()));
    }

    #[tokio::test]
    async fn store_missing_key() {
        let (_tmp, mem) = test_mem();
        let tool = MemoryStoreTool::new(mem, test_security());
        let result = tool.execute(json!({"content": "no key"})).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn store_missing_content() {
        let (_tmp, mem) = test_mem();
        let tool = MemoryStoreTool::new(mem, test_security());
        let result = tool.execute(json!({"key": "no_content"})).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn store_blocked_in_readonly_mode() {
        let (_tmp, mem) = test_mem();
        let readonly = Arc::new(SecurityPolicy {
            autonomy: AutonomyLevel::ReadOnly,
            ..SecurityPolicy::default()
        });
        let tool = MemoryStoreTool::new(mem.clone(), readonly);
        let result = tool
            .execute(json!({"key": "lang", "content": "Prefers Rust"}))
            .await
            .unwrap();
        assert!(!result.success);
        assert!(
            result
                .error
                .as_deref()
                .unwrap_or("")
                .contains("read-only mode")
        );
        assert!(mem.get("lang").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn store_blocked_when_rate_limited() {
        let (_tmp, mem) = test_mem();
        let limited = Arc::new(SecurityPolicy {
            max_actions_per_hour: 0,
            ..SecurityPolicy::default()
        });
        let tool = MemoryStoreTool::new(mem.clone(), limited);
        let result = tool
            .execute(json!({"key": "lang", "content": "Prefers Rust"}))
            .await
            .unwrap();
        assert!(!result.success);
        assert!(
            result
                .error
                .as_deref()
                .unwrap_or("")
                .contains("Rate limit exceeded")
        );
        assert!(mem.get("lang").await.unwrap().is_none());
    }
}
