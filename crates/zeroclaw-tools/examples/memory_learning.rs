//! Operator curation and isolated learning regressions through the real tools.
//! Never runs inference or writes test facts into the live memory database.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};
use zeroclaw_api::tool::Tool;
use zeroclaw_config::policy::SecurityPolicy;
use zeroclaw_memory::{AgentScopedMemory, AuditedMemory, Memory, MemoryCategory, SqliteMemory};
use zeroclaw_tools::memory_store::MemoryStoreTool;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("probe") {
        return probe().await;
    }
    if args.get(1).map(String::as_str) == Some("fixture") {
        let directory = Path::new(args.get(2).context("fixture directory missing")?);
        ensure!(
            directory
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("comfy-learning-eval-")),
            "fixture directory must have the evaluation prefix"
        );
        let inner = Arc::new(SqliteMemory::new("sqlite", &directory.join("data"))?);
        let owner = inner.ensure_agent_uuid("comfy").await?;
        let memory = AgentScopedMemory::new(inner, owner, []);
        let fixtures = if args.get(3).map(String::as_str) == Some("--external") {
            vec![(
                "eval/external",
                "Externalcanary value is ORCHID. Source: separate writer, observed today.",
            )]
        } else {
            vec![
                (
                    "eval/backend/old",
                    "EvalRenderer runs on GPU_A. Source: old setup record.",
                ),
                (
                    "eval/preference",
                    "EvalRenderer preference: BLUE backgrounds. Scope: fixture user's stable preference.",
                ),
                (
                    "eval/lora",
                    "EvalLoraCedar is compatible only with family A, never family B. Source: model compatibility metadata.",
                ),
            ]
        };
        for (key, content) in fixtures {
            memory
                .store(key, content, MemoryCategory::Core, None)
                .await?;
        }
        println!("{}", json!({"seeded":true}));
        return Ok(());
    }
    ensure!(
        args.get(1).map(String::as_str) == Some("curate"),
        "use probe or curate PLAN [--apply]"
    );
    let path = args.get(2).context("curation plan required")?;
    let plan: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let data_dir = Path::new(plan["data_dir"].as_str().context("data_dir missing")?);
    let agent = plan["agent"].as_str().context("agent missing")?;
    let embedding = &plan["embedding"];
    let inner = Arc::new(SqliteMemory::with_embedder(
        "sqlite",
        data_dir,
        Arc::new(zeroclaw_memory::embeddings::OpenAiEmbedding::new(
            embedding["url"].as_str().context("embedding url missing")?,
            "",
            embedding["model"]
                .as_str()
                .context("embedding model missing")?,
            embedding["dimensions"]
                .as_u64()
                .context("embedding dimensions missing")? as usize,
        )),
        0.7,
        0.3,
        10000,
        None,
        Default::default(),
    )?);
    let owner = inner.ensure_agent_uuid(agent).await?;
    let memory: Arc<dyn Memory> = Arc::new(AuditedMemory::new(
        AgentScopedMemory::new(inner, owner, []),
        data_dir,
    )?);
    let writes = plan["writes"].as_array().context("writes missing")?;
    // Validate the entire old corpus before any writes. The plan keeps exact
    // old contents privately; arbitrary IDs alone are insufficient evidence.
    for old in plan["expected"]
        .as_array()
        .context("expected records missing")?
    {
        let entry = memory
            .get(old["key"].as_str().context("expected key missing")?)
            .await?
            .context("expected record missing")?;
        ensure!(
            entry.id == old["id"].as_str().context("expected ID missing")?
                && entry.content
                    == old["content"]
                        .as_str()
                        .context("expected content missing")?,
            "source changed since the curation plan was reviewed"
        );
    }
    if args.get(3).map(String::as_str) != Some("--apply") {
        println!(
            "{}",
            json!({"validated_writes":writes.len(), "applied":false})
        );
        return Ok(());
    }
    let tool = MemoryStoreTool::new(memory.clone(), Arc::new(SecurityPolicy::default()));
    let mut applied = 0;
    for write in writes {
        let key = write["key"].as_str().context("key missing")?;
        if let Some(existing) = memory.get(key).await? {
            ensure!(
                existing.content == write["content"].as_str().context("content missing")?,
                "replacement key changed"
            );
            // Resume a previously interrupted store/retirement without erasing
            // its already persisted replacement or duplicating a memory.
            if let Some(ids) = write.get("supersedes") {
                let ids: Vec<String> = serde_json::from_value(ids.clone())?;
                let remaining = memory
                    .export(&Default::default())
                    .await?
                    .into_iter()
                    .filter(|e| ids.contains(&e.id) && e.superseded_by.is_none())
                    .map(|e| e.id)
                    .collect::<Vec<_>>();
                if !remaining.is_empty() {
                    memory.supersede(&remaining, key).await?;
                }
            }
        } else {
            let result = tool.execute(write.clone()).await?;
            ensure!(result.success, "memory_store failed: {:?}", result.error);
        }
        applied += 1;
    }
    println!("{}", json!({"applied":applied,"agent":agent}));
    Ok(())
}

async fn probe() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let inner = Arc::new(SqliteMemory::new("sqlite", temp.path())?);
    let owner = inner.ensure_agent_uuid("artist").await?;
    let peer = inner.ensure_agent_uuid("peer").await?;
    let memory: Arc<dyn Memory> = Arc::new(AgentScopedMemory::new(
        inner.clone(),
        owner.clone(),
        [peer.clone()],
    ));
    let tool = MemoryStoreTool::new(memory.clone(), Arc::new(SecurityPolicy::default()));
    let mut checks = Vec::new();
    for (key, content) in [
        ("backend/old", "Rendering backend is the retired AMD host."),
        (
            "preference/compatible",
            "Rendering preference: blue backgrounds.",
        ),
        (
            "lora/scoped",
            "Fixture LoRA Cedar is compatible only with family A, never family B.",
        ),
    ] {
        ensure!(
            tool.execute(json!({"key":key,"content":content}))
                .await?
                .success,
            "fixture store failed"
        );
    }
    let old = memory
        .get("backend/old")
        .await?
        .context("old record missing")?;
    ensure!(tool.execute(json!({"key":"backend/new","content":"Rendering backend is the NVIDIA VM. Source: explicit correction; observed today.","supersedes":[old.id]})).await?.success,"correction failed");
    let results = memory.recall("Rendering", 20, None, None, None).await?;
    ensure!(
        results.iter().all(|e| e.id != old.id) && results.iter().any(|e| e.key == "backend/new"),
        "stale backend leaked into recall"
    );
    checks.push("explicit_correction_hides_stale_infrastructure");
    ensure!(
        memory
            .get("backend/old")
            .await?
            .context("history lost")?
            .content
            == old.content,
        "old evidence erased"
    );
    checks.push("correction_history_preserved");
    ensure!(
        results.iter().any(|e| e.key == "preference/compatible"),
        "compatible preference was retired"
    );
    checks.push("compatible_preference_preserved");
    let scope = memory.recall("Cedar", 5, None, None, None).await?;
    ensure!(
        scope.iter().any(|e| e.content.contains("never family B")),
        "LoRA scope lost"
    );
    checks.push("lora_scope_retrieved");
    let other = SqliteMemory::new("sqlite", temp.path())?;
    let _ = memory.recall("Externalcanary", 5, None, None, None).await?;
    other
        .store_with_agent(
            "external",
            "Externalcanary arrived from another database handle.",
            MemoryCategory::Core,
            None,
            None,
            None,
            Some(&owner),
        )
        .await?;
    ensure!(
        memory
            .recall("Externalcanary", 5, None, None, None)
            .await?
            .iter()
            .any(|e| e.key == "external"),
        "external write invisible"
    );
    checks.push("external_write_visible_without_cache");
    inner
        .store_with_agent(
            "peer",
            "Peer-owned preference.",
            MemoryCategory::Core,
            None,
            None,
            None,
            Some(&peer),
        )
        .await?;
    let peer_entry = memory.get("peer").await?.context("read grant failed")?;
    let denied = tool
        .execute(
            json!({"key":"peer-replacement","content":"Wrong owner", "supersedes":[peer_entry.id]}),
        )
        .await?;
    ensure!(
        !denied.success
            && memory
                .get("peer")
                .await?
                .context("peer lost")?
                .superseded_by
                .is_none(),
        "peer read grant permitted mutation"
    );
    checks.push("peer_write_rejected");
    println!(
        "{}",
        json!({"passed":checks.len(),"checks":checks,"scope":"native tools and SQLite; no claim about model judgment"})
    );
    Ok(())
}
