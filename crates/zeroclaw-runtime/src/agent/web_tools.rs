//! Bounded execution of existing web tools for authenticated external clients.
//! Config is resolved on every call; only the rate-limit counters survive calls.
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use zeroclaw_config::{policy::PerSenderTracker, policy::SecurityPolicy, schema::Config};

use crate::approval::ApprovalManager;
use crate::hooks::{HookResult, HookRunner};
use crate::observability::Observer;
use crate::tools::scoped::{ScopedAssembly, ScopedToolRegistry};
use crate::tools::{AllToolsResult, ArcToolRef, Tool};

use super::tool_execution::{ToolDispatchContext, ToolExecutionOutcome, execute_one_tool};
use super::turn::context::TurnMeta;

pub const NAMES: [&str; 2] = ["web_search_tool", "web_fetch"];

/// Uses the agent's current policy and the ordinary scoped assembly seam. It never
/// connects MCP, loads skills, opens hardware, or exposes other built-in tools.
pub async fn registry(
    config: &Config,
    agent: &str,
    tracker: &PerSenderTracker,
) -> Result<ScopedToolRegistry> {
    if !config.agents.get(agent).is_some_and(|a| a.enabled) {
        bail!("Agent is unknown or disabled");
    }
    let risk = config
        .risk_profile_for_agent(agent)
        .context("Agent risk profile is unavailable")?;
    let mut security = SecurityPolicy::for_agent(config, agent)?;
    security.tracker = tracker.clone();
    let security = Arc::new(security);
    let approval = ApprovalManager::for_non_interactive(risk);
    let readonly = risk.level == crate::security::AutonomyLevel::ReadOnly;
    let tools: Vec<Box<dyn Tool>> =
        crate::tools::web_tools_with_security(security.clone(), &config.web_fetch, config)
            .into_iter()
            .filter(|t| !readonly && !approval.needs_approval(t.name()))
            .map(|t| Box::new(ArcToolRef(t)) as Box<dyn Tool>)
            .collect();
    let allowed: Vec<String> = NAMES.iter().map(|s| (*s).to_string()).collect();
    let runtime = crate::platform::create_runtime(&config.runtime)?;
    let assembled = ScopedToolRegistry::assemble(ScopedAssembly {
        config,
        agent_alias: agent,
        security: &security,
        built: AllToolsResult::from_prebuilt_tools(tools),
        skills: &[],
        runtime: runtime.into(),
        caller_allowed: Some(&allowed),
        connect_mcp: false,
        connect_peripherals: false,
        exclude_memory: true,
        acp_delivery: false,
        list_deferred_mcp_specs: false,
        emit_assembly_logs: false,
        mcp_registry: None,
    })
    .await;
    Ok(assembled.registry)
}

pub async fn execute(
    config: &Config,
    agent: &str,
    name: &str,
    arguments: Value,
    tracker: &PerSenderTracker,
    observer: &dyn Observer,
) -> Result<ToolExecutionOutcome> {
    if !NAMES.contains(&name) || !arguments.is_object() {
        bail!("Only web_search_tool and web_fetch with object arguments are supported");
    }
    let tools = registry(config, agent, tracker).await?;
    let hooks = HookRunner::from_config(&config.hooks);
    let (name, arguments) = match hooks.run_before_tool_call(name.into(), arguments).await {
        HookResult::Continue(call) => call,
        HookResult::Cancel(reason) => bail!("Web tool cancelled by hook: {reason}"),
    };
    // Recheck after hooks: a hook cannot widen this endpoint into arbitrary execution.
    if !NAMES.contains(&name.as_str()) || !tools.iter().any(|t| t.name() == name) {
        bail!("Web tool is disabled, excluded, or requires interactive approval for this agent");
    }
    let turn_id = uuid::Uuid::new_v4().to_string();
    let meta = TurnMeta {
        agent_alias: Some(agent),
        parent_agent_alias: None,
        turn_id: &turn_id,
        channel_name: "web-tool-bridge",
    };
    zeroclaw_api::TOOL_LOOP_THREAD_ID
        .scope(
            Some(format!("web-tools:{agent}")),
            execute_one_tool(
                &name,
                arguments,
                Some(&turn_id),
                ToolDispatchContext {
                    tools_registry: &tools,
                    activated_tools: None,
                    excluded_tools: &[],
                    model_switch_callback: None,
                },
                &meta,
                observer,
                None,
                None,
                None,
            ),
        )
        .await
}
