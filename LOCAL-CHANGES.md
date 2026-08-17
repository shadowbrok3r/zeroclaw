# LOCAL-CHANGES.md

Fork-delta manifest for this repository. Upstream is
`zeroclaw-labs/zeroclaw`; upstream tags are merged in periodically via the
`local/upstream-merge` branch. This file exists to keep those merges cheap:
it names every file this fork touches, why, and how to resolve the conflicts
that recur.

Keep this file updated in the same PR as any fork-local change.

## Why this fork diverges

This fork carries the session-threads overhaul: turning ZeroClaw's implicit,
append-only session store into a first-class "threads" surface with lifecycle
events, verb parity across key families, scoped TTLs, ownership stamping, a
web threads UI, and a sessions CLI.

Context that shapes the design:

- The primary client is Claude Code driving the CLI and REST surfaces, so the
  CLI (`zeroclaw sessions`) and REST verb parity matter more than dashboard
  polish.
- Discord is the interim chat channel, so channel-composite session keys
  (`discord.<agent>_<room>_<sender>`) must be first-class citizens of every
  listing, TTL, and deletion surface, not an afterthought behind `gw_` keys.

Architecture documentation for the overhaul:
`docs/book/src/architecture/session-lifecycle.md`.

## Change inventory

### Session lifecycle SSE events

| File | Why |
|---|---|
| `crates/zeroclaw-gateway/src/session_events.rs` (NEW) | Builds and emits `source: "sessions"` frames (`session_created` / `session_update` / `session_closed`) onto the event bus. |
| `crates/zeroclaw-gateway/src/sse.rs` | `is_public_sse_event` additionally admits frames with `source == "sessions"`. |
| `crates/zeroclaw-gateway/src/ws.rs` | Emits created/update/closed at connect, turn-state transitions, and disconnect. |
| `crates/zeroclaw-gateway/src/api.rs` | Emits lifecycle frames from REST rename/delete handlers. |
| `crates/zeroclaw-gateway/src/lib.rs` | Wires the emitter through `AppState` / module declarations. |

### REST verb parity for non-`gw_` keys

| File | Why |
|---|---|
| `crates/zeroclaw-gateway/src/api.rs` | Shared id resolution (verbatim key, then `gw_<id>`, then `rpc_<id>`) replaces the per-handler `gw_` hard-coding and the underscore heuristic in the messages handler. |

### Scoped hourly TTL sweeps

| File | Why |
|---|---|
| `crates/zeroclaw-infra/src/session_backend.rs` | Prefix/family-scoped stale-cleanup API alongside the existing `cleanup_stale`. |
| `crates/zeroclaw-infra/src/session_sqlite.rs` | SQLite implementation of the scoped cleanup. |
| `crates/zeroclaw-gateway/src/lib.rs` | Startup one-shot unscoped sweep becomes an hourly task sweeping `gw_` rows via `gateway.session_ttl_hours`. |
| `crates/zeroclaw-channels/src/orchestrator/mod.rs` | Hourly sweep of channel-composite rows via `channels.session_ttl_hours` (previously a dead knob with zero readers). |
| `crates/zeroclaw-config/src/schema.rs` | Doc-comment updates for both TTL knobs (behavior, scoping, `0` = disabled). |

### Origin principal, resume guard, device scoping

| File | Why |
|---|---|
| `crates/zeroclaw-infra/src/session_backend.rs` | `origin_principal` in session metadata contract. |
| `crates/zeroclaw-infra/src/session_sqlite.rs` | Additive `origin_principal` column migration and read/write support. |
| `crates/zeroclaw-gateway/src/ws.rs` | Stamps the auth-subject principal on first append; refuses cross-agent resume unless `adopt=true` (replaces unconditional alias re-stamp). |
| `crates/zeroclaw-gateway/src/api.rs` | Honors `gateway.scope_sessions_to_device` when listing/resolving sessions. |
| `crates/zeroclaw-config/src/schema.rs` | New opt-in `gateway.scope_sessions_to_device` knob (default `false`). |

### Web threads UI

| File | Why |
|---|---|
| `web/src/components/ThreadsPanel.tsx` (NEW) | Thread list per agent: switch, rename, delete; live-updates from SSE lifecycle events. |
| `web/src/lib/ws.ts` | Session id / resume / `adopt` plumbing on the chat socket. |
| `web/src/contexts/AgentContext.tsx` | Active-thread state per agent. |
| `web/src/pages/AgentChat.tsx` | Hosts the threads panel; thread switching in the chat view. |
| `web/src/pages/Dashboard.tsx` | Consumes `session_created` / `session_update` / `session_closed` frames (upstream already subscribed; the frames now exist). |
| `web/src/lib/api.ts` | Client calls for the session REST verbs. |
| `web/src/types/api.ts` | Session/thread payload types. |
| `web/src/lib/i18n.ts` | Threads UI strings (web text contract). |
| `web/src/lib/slashCommands.ts` | `/new` starts a fresh thread instead of deleting the current session. |

### `zeroclaw sessions` CLI

| File | Why |
|---|---|
| `src/sessions_cli/` (NEW) | `zeroclaw sessions` subcommands over the shared backend: list, show, search, rename, delete. |
| `src/main.rs` | Subcommand registration and dispatch. |
| `crates/zeroclaw-runtime/locales/en/cli.ftl` | Fluent strings for the CLI output (CLI text policy). |

### Documentation

| File | Why |
|---|---|
| `docs/book/src/architecture/session-lifecycle.md` (NEW) | Deep-dive: store, key families, sessionless surfaces, lifecycle, fork surfaces. |
| `docs/book/src/SUMMARY.md` | Registers the new page under Architecture. |
| `docs/book/src/architecture/runtime-state-and-persistence.md` | Chat/channel-sessions row: scoped TTL sweeps and `origin_principal` note. |
| `docs/book/src/gateway/api.md` | "Session endpoints" section: REST verbs, id resolution, SSE lifecycle types. |
| `LOCAL-CHANGES.md` (NEW) | This manifest. |

## Merge runbook

```bash
git fetch origin
git checkout local/upstream-merge
git merge <tag>          # e.g. v0.8.5
# resolve, validate, then merge local/upstream-merge into local/sessions-threads
```

New-file additions never conflict: `crates/zeroclaw-gateway/src/session_events.rs`,
`src/sessions_cli/`, `docs/book/src/architecture/session-lifecycle.md`,
`web/src/components/ThreadsPanel.tsx`, and this file are fork-only paths.
Conflicts concentrate in the shared files below.

| Hotspot | What ours adds | Resolution guidance |
|---|---|---|
| `crates/zeroclaw-config/src/schema.rs` | `gateway.scope_sessions_to_device` near the gateway section; doc-comment edits on both `session_ttl_hours` knobs | Keep both sides; upstream adds fields to the same structs frequently. Re-run schema-derived generators if the merge touches them. |
| `crates/zeroclaw-gateway/src/ws.rs` | Principal stamping, adopt guard, and event emission around `handle_socket` connect/resume and the per-turn state writes | Keep both; ours wraps upstream's resume and state-transition points rather than replacing them. Re-check any upstream change to `set_session_agent_alias` call sites against the adopt guard. |
| `crates/zeroclaw-gateway/src/api.rs` | Shared session-id resolution helper plus event emission in the session handlers | Prefer ours inside `handle_api_session_*`; take upstream elsewhere in the file. If upstream rewrites a session handler, re-apply id resolution and event emission on top. |
| `crates/zeroclaw-gateway/src/lib.rs` | Hourly TTL task replacing the one-shot startup sweep; `session_events` module wiring | Take upstream for unrelated router/daemon churn; keep our sweep task and module wiring. Watch for upstream edits to the old startup `cleanup_stale` block, which we removed. |
| `crates/zeroclaw-channels/src/orchestrator/mod.rs` | Channel-family TTL sweep near the shared-store setup in `start_channels` | Keep both; ours is additive next to store construction. |
| `web/src/lib/i18n.ts` | Threads UI string keys | Union of both key sets; conflicts are line-adjacency noise. |
| `src/main.rs` | `sessions` subcommand arm | Keep both arms; upstream adds subcommands in the same match. |

## Behavior deltas vs upstream

Re-check these after every upstream merge; they are easy to silently lose:

- **Gateway TTL sweep is `gw_`-scoped and hourly.** Upstream runs
  `cleanup_stale` once at startup and deletes stale rows of every family,
  including channel sessions. If a merge reintroduces the startup call,
  channel history gets bulk-deleted under `gateway.session_ttl_hours`.
- **`channels.session_ttl_hours` is live.** Upstream defines it with zero
  readers. Our orchestrator sweep reads it; a merge that drops the sweep makes
  it a dead knob again with no compile error.
- **WS refuses cross-agent resume without `adopt=true`.** Upstream re-stamps
  `agent_alias` unconditionally on every connect. A merge that restores the
  unconditional `set_session_agent_alias` silently reassigns threads between
  agents again.
- **`/new` in the web UI no longer deletes.** Upstream's `/new` deletes the
  current session; ours starts a new thread and keeps the old one. A merge
  taking upstream's `slashCommands.ts` restores destructive `/new`.
