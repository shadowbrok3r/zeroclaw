# LOCAL-CHANGES.md

Fork-delta manifest for this repository. Upstream is
`zeroclaw-labs/zeroclaw`; upstream tags are merged periodically into `main`,
the fork's single line (the deployed binary is built from it). This file exists
to keep those merges cheap:
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

### `crates/zeroclaw-gateway/src/ws.rs` — a turn outlives its client's socket

The turn-forwarding `select!` used to call `cancel_token.cancel()` when the client's socket ended,
which upstream added to stop a disconnected socket hot-looping the branch (#6514). The side effect
was that any client disconnect killed the turn and stored the partial reply with
`[interrupted by user]` appended — so a phone that changed network, or was backgrounded long
enough for the OS to abort its socket, lost the answer it was waiting for and was told it had
interrupted itself.

A `client_gone` flag with `if !client_gone` guards on that arm and on the ping arm stops the branch
being polled, which is what #6514 actually needed, while the turn runs on. It finishes, is
persisted, and the next connection backfills it over `/api/sessions/{id}/messages`.

Conflict note: upstream edits to this `select!` will land on the guarded arms. Keep the guards;
do not restore `cancel_token.cancel()` on the disconnect path.

### Reconnect rejoins the running turn (`turn_resume`, gateway)

The turn outliving its socket (above) still left the phone blind: a socket that
reconnected mid-turn got nothing of the running turn, since it streamed only into
the old socket, and `send_turn_frame` drops every frame after one write over 5 s.
Measured 2026-09-18 (comfy session `e5a406ad`, 22:51-22:56 UTC): the phone
reconnected 32 s in, its thinking froze mid-word for 4.5 minutes, the reply only
came back from history after `done`, and the later thinking/tool rows were lost.

A per-session hub now mirrors every frame of the running turn. A socket that
connects with `session_id` while that session's turn is running gets, right after
`session_start`, `{"type":"turn_resume","frames":[…]}`: the whole turn from its
first frame (adjacent `chunk` deltas merged, adjacent `thinking` merged, never
across a `tool_call`/`tool_result`; an `approval_request` only while still
pending), then the rest of the turn live. Pending approvals and steering are
per session, so the resumed socket can answer a parked approval or steer. The
consumer is zc-codex app 0.8.31+ (`android/src/zeroclaw.rs` `turn_resume` arm,
test `a_turn_cut_by_a_reconnect_is_replaced_by_the_gateways_replay`); older
apps ignore the frame.

| File | Why |
|---|---|
| `crates/zeroclaw-gateway/src/ws_hub.rs` (NEW) | `TurnHub` keyed by `session_key`: ordered replay buffer, live `broadcast`, `pending_approvals`, steering. `begin`/`push`/`finish` from the turn; `attach` snapshots and subscribes under one lock so an observer gets each frame exactly once. Registry reclaims idle, unreferenced sessions on insert. |
| `crates/zeroclaw-gateway/src/ws.rs` | `handle_socket` takes `pending_approvals` from the hub and, if a turn is running, runs `observe_running_turn` before the first-message path. `process_chat_message` takes `&hub`: `begin` at turn start, `push` beside every streamed frame (also after the client is gone), `finish` with `done`/`aborted`/`error`. |
| `crates/zeroclaw-gateway/src/lib.rs` | `pub mod ws_hub;` |

Conflict note: every `send_turn_frame` of a turn frame in `process_chat_message` has a
`hub.push`/`hub.finish` next to it. Upstream edits that add a frame type or a terminal path
must add the matching hub call, or a reconnect will replay an incomplete turn (no compile
error). A new turn-failure code must also be told to zc-codex, which ends the turn on
`PROVIDER_ERROR`/`AUTH_ERROR`/`AGENT_ERROR`.

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
| `crates/zeroclaw-gateway/src/api.rs` | Shared id resolution (verbatim key, then `gw_<id>`, then `rpc_<id>`, then `cc_<id>`) replaces the per-handler `gw_` hard-coding and the underscore heuristic in the messages handler. |

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
| `crates/zeroclaw-gateway/src/ws.rs` | Stamps the auth-subject principal on first append; refreshes `last_activity` on resume; refuses cross-agent resume unless `adopt=true` (replaces unconditional alias re-stamp). |
| `crates/zeroclaw-gateway/src/api.rs` | Honors `gateway.scope_sessions_to_device` on listing and on every id-addressed session verb (non-matching sessions answer 404). |
| `crates/zeroclaw-gateway/src/sse.rs` | Withholds `source == "sessions"` frames from unauthenticated streams and whenever device scoping is enabled. |
| `crates/zeroclaw-config/src/schema.rs` | New opt-in `gateway.scope_sessions_to_device` knob (default `false`). |
| `crates/zeroclaw-tools/src/sessions.rs` | Session-id resolution delegates to the shared `zeroclaw-infra` helper (gains the `rpc_` arm). |

### Web threads UI

| File | Why |
|---|---|
| `web/src/components/ThreadsPanel.tsx` (NEW) | Read-only browser for the session families upstream's `SessionPicker` does not show: channel conversations, `cc_` Claude Code, and `rpc_`/TUI. Transcript viewer; live-updates from SSE lifecycle events. |
| `web/src/lib/ws.ts` | `adopt` plumbing on the chat socket. |
| `web/src/contexts/AgentContext.tsx` | `startNewThread` (ownership-refusal escape hatch over upstream's `startNewSession`). |
| `web/src/pages/AgentChat.tsx` | Hosts the panel; `/new` and the ownership banner route through `startNewThread`. |
| `web/src/pages/Dashboard.tsx` | Consumes `session_created` / `session_update` / `session_closed` frames (upstream already subscribed; the frames now exist). |
| `web/src/lib/api.ts` | Client calls for the session REST verbs. |
| `web/src/types/api.ts` | Session/thread payload types. |
| `web/src/lib/i18n.ts` | Threads UI strings (web text contract). |


### Claude Code session ingestion (`cc_` family)

| File | Why |
|---|---|
| `crates/zeroclaw-config/src/schema.rs` | New `claude_code.hook_secret` knob (default unset; `#[secret]`): enables session ingestion on `/hooks/claude-code`. |
| `crates/zeroclaw-gateway/src/lib.rs` | `AppState.claude_code_hook_secret_hash` (hashed at boot, plaintext never stored); registers the transcript backfill sub-router with its own 8 MiB body limit. |
| `crates/zeroclaw-gateway/src/api.rs` | Reworked `handle_claude_code_hook` (dual payload shapes, `X-ZC-Hook-Secret` auth, ingestion into `cc_<session_id>` rows) plus the new `handle_claude_code_transcript` backfill handler. |
| `crates/zeroclaw-gateway/src/api_config.rs`, `crates/zeroclaw-gateway/src/api_sections.rs` | Test `AppState` literals gain the new field. |
| `web/src/components/ThreadsPanel.tsx` | Read-only "Claude Code" group for `cc_` keys above the TUI group. |
| `web/src/lib/i18n.ts` | `threads.claude_code` string key. |
| `docs/book/src/architecture/session-lifecycle.md` | `cc_` family row plus "Claude Code sessions" subsection (hook contract). |
| `docs/book/src/gateway/api.md` | Claude Code hook endpoints table under session endpoints. |

### `zeroclaw sessions` CLI

| File | Why |
|---|---|
| `src/sessions_cli/` (NEW) | `zeroclaw sessions` subcommands over the shared backend: list, show, search, rename, delete. |
| `src/main.rs` | Subcommand registration and dispatch. |
| `crates/zeroclaw-runtime/locales/en/cli.ftl` | Fluent strings for the CLI output (CLI text policy). |

### Webhook channel

| File | Why |
|---|---|
| `crates/zeroclaw-channels/src/webhook.rs` | Two deltas. The listener binds a configurable address (`channels.webhook.bind_address`) instead of a hard-coded one, and a webhook post is treated as a direct message so it reaches the agent without a mention. `WebhookChannel::new` therefore takes one more parameter than upstream's; the two call sites live in `orchestrator/mod.rs`. |
| `docs/book/src/channels/webhook.md` | Documents the bind address. |

### Render delivery from Comfy job receipts (gateway)

The phone client rendered whatever `[IMAGE:…]` path the model wrote, and the
model wrote it wrong most of the time (fabricated `renders/output/cg_<uuid>.png`
names, gallery epochs rebuilt from memory). The gateway now rewrites the reply at
turn end from the durable job receipts (`comfy-gen where --deliverable`), before
it is persisted and before the `done` frame carries it. Needs `ZEROCLAW_COMFY_GEN`
in the service environment and a comfy-gen with `where --since --deliverable`.

| File | Why |
|---|---|
| `crates/zeroclaw-gateway/src/render_delivery.rs` (NEW) | `rewrite()`: drop every model-written image marker when the receipts show a render this turn (otherwise only unservable ones), prepend the verified files one per line; `reconcile()` applies it to `outcome.response` and the assistant `Chat` rows in `new_messages` (only the last row gains markers, so a backfill shows each render once). |
| `crates/zeroclaw-gateway/src/session_jobs.rs` | `run` split into `run`/`run_args`; `deliverables()` invokes `where --session S --since T --latest 64 --deliverable` and `deliverables_from()` validates the reply (absolute, no `..`, valid job id, index < 64). |
| `crates/zeroclaw-gateway/src/ws.rs` | Captures `turn_started_unix` next to `turn_id`; calls `render_delivery::reconcile` in the `Ok(outcome)` arm before `persist_conversation_messages`. |
| `crates/zeroclaw-gateway/src/lib.rs` | `mod render_delivery;` |

Companion change in `zeroclaw-homelab/comfy-gen`: `jobs::delivered()` completes and
caches the receipt from the render process itself (so the lookup never races the
observer), and `where` grew `--since` / `--deliverable`.

### Cron output reaches the phone (`app.<alias>` delivery, richer `cron_result`)

A scheduled job's output had two destinations: a chat channel, or nowhere. The
phone app (`zc-codex`) is where this deployment actually reads its automations,
so the runs now land there two ways. Durably: `delivery.channel = "app.<agent
alias>"` writes the run into a gateway session, and the app lists sessions
already — no channel account in between, and the session can be replied to like
any other. Live: the `cron_result` frame the gateway broadcasts to every
`/ws/chat` client (`is_global_chat_event`) now names the job and the run row, so
a client can raise a notification and open the run without a follow-up fetch.

The frame carried `job_id`, `success`, `output` and a timestamp; a `job_id` is a
uuid, which is the wrong thing to put in a notification title, and the `output`
was unbounded on a broadcast that reaches phones.

| File | Why |
|---|---|
| `crates/zeroclaw-channels/src/orchestrator/mod.rs` | `deliver_to_app_session()` and `is_app_delivery_channel()`, taken before the registry/config lookup because `app` is not a configured channel: appends the announcement as an assistant row, stamps `agent_alias` (without it `/api/sessions` drops the row as an orphan and the app never lists it), and names the session from its id the first time only. |
| `crates/zeroclaw-runtime/src/cron/scheduler.rs` | `CronResultEvent` builds the one frame both the scheduled loop and a manual trigger send; `MAX_CRON_EVENT_OUTPUT_BYTES` (8 KiB) bounds it with `truncated`/`output_bytes` rather than a marker; `ScheduledRunReport`/`PersistedRun` carry the name, agent, run id, status and duration out of the run; `app_delivery_body()` heads an app delivery with the job's name and its own timezone's hour. |
| `crates/zeroclaw-runtime/src/cron/store.rs` | `insert_run_and_prune()` and both persist paths return the inserted `cron_runs.id`, which is what `run_id` names. |
| `crates/zeroclaw-runtime/src/cron/mod.rs` | `app` joins the delivery channels the cron tool schemas accept; a bare `app` is refused at add time, since the session it would write to is listed per agent. |
| `crates/zeroclaw-gateway/src/api.rs` | `POST /api/cron/{id}/run` answers with `run_id`, so a manual run and its broadcast collapse into one row on the client instead of being matched by timestamp. |
| `crates/zeroclaw-infra/src/session_sqlite.rs` | `PRAGMA busy_timeout = 5000`. WAL keeps a reader out of a writer's way but not a second writer, and SQLite's default timeout is zero — the loser gets `database is locked` immediately. A cron delivery is a second writer against a store the gateway is already using. |

The frame is deliberately still `session_id`-less: that is what keeps
`is_global_chat_event` forwarding it to every socket. The app half is
`zc-codex` `da220d7` (0.8.41), which reads every new field and degrades to the
old shape when the gateway is older.

### Provider truncation surfacing

Unrelated to the session overhaul; carried here because it is a one-file
provider fix this deployment depends on.

| File | Why |
|---|---|
| `crates/zeroclaw-providers/src/compatible.rs` | Adds `finish_reason` to the non-streaming `Choice` (upstream omits the field entirely) and surfaces a `length` stop on both the streaming and non-streaming paths as a `WARN` plus a visible notice appended to the reply. |

### Stream guard releases bracketed prose (runtime)

Unrelated to the session overhaul. The streaming text guard holds everything from
a `[` or `{` onward until it parses as JSON, so a text-form tool call never
leaks into the stream. Prose never parses, so a reply that opened with an avatar
tag (`[ACT emotion="playful"]`) reached `/ws/chat` as one chunk just before
`done`. Measured 2026-09-18: the `default` agent streamed one chunk per reply,
`research` 60-70.

| File | Why |
|---|---|
| `crates/zeroclaw-runtime/src/agent/turn/protocol_detect.rs` | `bracket_candidate_is_prose()`: a held `[`/`{` candidate is prose when its first JSON value is a syntax error, or a complete value that is not a known-tool envelope. An incomplete value (serde EOF) and a growing `[tool_call]` opener are not. |
| `crates/zeroclaw-runtime/src/agent/turn/stream_guard.rs` | `StreamTextGuard::push` releases such a candidate at once. `prose_bracket_tests` cover avatar tags, prose brackets, a suppressed known-tool envelope and an incomplete one. |

### A blocked shell command asks the operator instead of dead-ending

Upstream gives a shell command three ways to be refused, and two of them ignore
`approved` entirely: a command outside `allowed_commands`, and a high-risk command
under `block_high_risk_commands`. The model gets a refusal it cannot act on, and
the operator is never asked — even on a phone that is attached and waiting. The
third, the medium/high `!approved` arms, never fired here at all, because `shell`
is in every profile's `auto_approve` and the runtime therefore stamps
`approved = true` on every shell call (`set_runtime_approved_arg`).

Now a command the policy would refuse is escalated into a real approval prompt and
put to the operator over the same route `turn_resume` uses. Opt-in and derived, not
a new knob: it follows `risk_profile.approval_route` being set, so a profile with
somewhere to ask asks, and every profile without one — plus every shipped preset —
refuses exactly as before. That is deliberate: `presets::balanced` contracts "a
hard block, not an approval prompt", and this must not quietly redefine it.

The floor is unchanged and is NOT promptable: a runtime with no shell access, the
shell tool's own dialect-aware forbidden-path scan (`shell.rs`, which never sees
`approved`), full autonomy (which never prompts, so `approved` there has no human
behind it), and PowerShell — whose `--%` stop-parsing token and provider-prefix
quoting (`E'nv:'PATH`) can hide what a command does from the person approving it.
An approval is only meaningful if the operator can see what they are approving.

The escalation also refuses, rather than asks, when a command hides its target
behind `$`, a backtick (or `%` under cmd). Found the hard way on 2026-09-19: the
path scan matches path-SHAPED arguments, so `F="/tmp/x" && printf ok > "$F"` looks
like an assignment token and an indirection, not a path. It reported nothing at
escalation time and again at execution time, and an approved run wrote outside
the workspace.

That rule lives in `operator_can_override`, and it must live there and nowhere
else. Putting it in `shell_command_needs_operator_approval_for_shell` instead is
the obvious-looking move and it is wrong: that function only decides whether to
ASK. Returning `false` from it means "no approval needed", not "refuse" — the
refusals are lifted by `operator_can_override`, which would still know nothing
about the command. A rule added to the asking side alone therefore removes the
prompt and leaves the command running, with nobody asked. That was measured on
the live daemon, not theorised: the probe above ran to completion with no
approval_request on the socket, and an in-workspace `D="…" && rm -rf "$D"`
deleted the directory the same silent way — strictly worse than the laundering
the rule was written to stop.

Two consequences worth keeping. Not asking is only safe when it means refusing,
so both halves must consult one predicate; and the regression test asserts the
refusal, not merely the absence of a modal, because a test that checks only for
"no prompt" passes happily while the command executes.

**The underlying scan is still blind to indirection** — hardening it to resolve
assignments and expansions would change execution for every agent, not just the
routed ones, and is deliberately left as separate work.

Variable indirection is its one proven blind spot, and probing the live daemon on
2026-09-19 mapped the edge: a relative escape (`> ../../../../../../tmp/x`), split
quoting (`> "/tm""p/x"`) and a glob (`> /tm?/x`) are all caught by the scan and
refused without a modal, so it resolves traversal and treats a quote-split or
globbed token as path-shaped. Every hidden-target shape tried — `"$F"`, `` ` ` ``
and `$(…)` — now fails closed with a refusal rather than silence, and the ordinary
escalations still ask: a literal `rm -rf <workspace>/…` prompts and runs when
approved, and a long literal command prompts, survives a socket drop and
reconnect, and records a decline as the operator's.

Because those refusals stand regardless, nothing that hits them is escalated —
asking spends a person's attention on a question whose answer cannot be honoured.
That includes the path floor, which is easy to under-estimate: `workspace_only`
makes it an ALLOWLIST, so with `allowed_roots = []` every path outside the agent's
workspace is refused even though it is nowhere in `forbidden_paths`. Measured
2026-09-19 against the live gateway: an APPROVED `rm -rf /tmp/zc-test-dir` still
came back "Path blocked by security policy". `shell_command_needs_operator_approval`
therefore consults `forbidden_workspace_path_argument_for_shell` — the same scan
`shell.rs` runs — and declines to ask. The same `rm -rf` inside the workspace does
prompt, and runs when approved: the risk tier is liftable, the confinement is not.

| File | Why |
|---|---|
| `crates/zeroclaw-config/src/policy.rs` | `operator_approval_route` on `SecurityPolicy` (set only by `from_profiles`, from `risk_profile.approval_route.is_some()`); `operator_can_override()` gates the allowlist and high-risk-block refusals on it + supervised + non-PowerShell; `shell_command_needs_operator_approval[_for_shell]()` mirrors exactly the refusals an approval can satisfy. |
| `crates/zeroclaw-runtime/src/approval/mod.rs` | `ApprovalManager` carries the agent's `SecurityPolicy` (`with_shell_policy`) and answers `shell_command_needs_operator()`. `derive_for_risk_profile` deliberately does NOT inherit it — a delegate runs another profile whose `allowed_commands` are not ours. |
| `crates/zeroclaw-runtime/src/agent/agent.rs` | Attaches the policy to the manager it builds. |
| `crates/zeroclaw-runtime/src/agent/turn/approval_gate.rs` | Escalates `Approved` → `Prompt` for such a command, and downgrades an `AlwaysApprove` on an escalated call to a one-shot `Yes`, so one tap on a `mkdir` cannot session-allowlist `shell` and pre-approve a later `rm -rf`. |

An approval frame also carries the arguments untruncated, because the same
reasoning applies one layer out: `summarize_args` redacts by key AND cuts every
value at 80 characters, so the phone was being asked to approve a shell command
it could only partly see. `TurnEvent::ApprovalRequest` gained an `arguments`
field and the three `/ws/chat` frame builders emit it. It is not the raw object
— `approval::redact_args` applies the same `looks_like_secret_key` redaction as
the summary and drops only the truncation, because this reaches a phone modal
and its notification text, which can render on a lock screen. The ACP/RPC
mapping in `rpc/dispatch.rs` deliberately ignores the field (`..`): widening that
surface is a separate decision.

| File | Why |
|---|---|
| `crates/zeroclaw-api/src/agent.rs` | `TurnEvent::ApprovalRequest.arguments: Option<Value>`, documented as redacted-but-untruncated. |
| `crates/zeroclaw-runtime/src/approval/mod.rs` | `redact_args()` beside `summarize_args` so one redaction list serves both and they cannot drift. |
| `crates/zeroclaw-gateway/src/ws_approval.rs` | Populates it from `ChannelApprovalRequest.raw_arguments`, which the WS path previously dropped on the floor. |
| `crates/zeroclaw-gateway/src/ws.rs` | All three `approval_request` frame builders emit `arguments`. |

Consumed by the zc-codex app from 0.8.35, which renders `arguments.command` in
full for `shell` approvals and falls back to `arguments_summary` when absent.

Conflict note: the escalation in `approval_gate.rs` and the two `operator_can_override`
calls in `policy.rs` are one mechanism — keep them together. Dropping only the
escalation while keeping the policy relaxation is the dangerous half: `shell` is
auto-approved, so every risky command would arrive with `approved = true` and run
without anyone being asked. Tests pinning this: `supervised_operator_approval_lifts_the_*`,
`full_autonomy_approval_does_not_lift_the_allowlist_or_the_block`, and
`needs_operator_approval_matches_what_validate_would_refuse`.

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
git switch -c upstream-merge/<tag> main   # short-lived; delete after merging
git merge <tag>          # e.g. v0.8.6
# resolve, validate (cargo test, then a release build), then:
git switch main && git merge --ff-only upstream-merge/<tag>
```

New-file additions never conflict: `crates/zeroclaw-gateway/src/session_events.rs`,
`crates/zeroclaw-gateway/src/ws_hub.rs`,
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
| `crates/zeroclaw-gateway/src/lib.rs` AppState | — | v0.8.5 **removed** `webhook_secret_hash` in favour of `configured_gateway_webhook_secret_hash(state)` resolved from live config. Every AppState literal (including ~20 test literals in `api.rs`) must drop it and keep `claude_code_hook_secret_hash`. |
| `crates/zeroclaw-channels/src/webhook.rs` | Bind address + direct-message treatment | Keep both; re-apply on top of upstream's rewrite. **After every merge, grep for `WebhookChannel::new` and `WebhookConfig {` across the tree.** The fork's `bind_address` is parameter 3 of 11 and a field of `WebhookConfig`, so any call site or literal upstream adds shifts every later argument by one — a new positional site fails to compile, but one that upstream later grows an 11th argument for would compile with `secret` landing in `auth_header`. v0.8.5 added two such sites: `orchestrator/mod.rs` (test helper) and `zeroclaw-runtime/src/daemon/mod.rs` (`webhook_only_config_is_supervised`). |
| `web/src/lib/ws.ts` | `adopt` flag | v0.8.5 moved session-id storage out of `ws.ts` into `web/src/lib/chatSessions.ts` (`getActiveSessionId` / `setActiveSessionId`) and made `sessionId` a required `WebSocketClientOptions` field. Keep only `adopt` here; repoint any `setSessionId` importer at `chatSessions`. |
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
- ~~**`/new` in the web UI no longer deletes.**~~ **Resolved upstream in
  v0.8.5.** Upstream implemented the same non-destructive `/new` (issue #7543),
  so this delta is retired and `slashCommands.ts` is back on upstream's
  `agent.cmd_help_new` key. `/new` still routes through the fork's
  `startNewThread` rather than upstream's `startNewSession`, because only the
  threads wrapper carries the ownership escape hatch. It deliberately emits no
  success notice: hydration for the freshly minted id lands immediately after
  the handler and would overwrite one (upstream's `localMessageMutationVersionRef`
  fence only discards mutations that land *after* the fetch starts).
- **TTL sweeps have safety rules.** Scoped sweeps skip `state = 'running'`
  rows, WS resume refreshes `last_activity`, and a `running` state write
  recreates a swept metadata row so no turn is silently dropped. A merge that
  reverts the SQLite `set_session_state`/sweep predicates reopens the
  delete-mid-turn race.
- **Session SSE frames are gated.** `source == "sessions"` lifecycle frames
  are withheld from `/api/events` when the stream is unauthenticated or when
  `gateway.scope_sessions_to_device` is on; channel keys embed room/sender
  identifiers, so this gate is a privacy boundary, not an optimization.
- **The session-id resolution policy lives in `zeroclaw-infra`.** Gateway
  REST, the sessions CLI, and the agent-facing session tools all resolve
  `verbatim -> gw_ -> rpc_ -> cc_` through one helper next to the `SessionBackend`
  trait. Do not reintroduce per-surface copies.
- **`/hooks/claude-code` gains authenticated ingestion.** Upstream's handler
  logs and returns ok; ours additionally ingests `cc_<session_id>` session
  rows when `claude_code.hook_secret` is configured and the caller presents
  it via `X-ZC-Hook-Secret` (plus a transcript backfill sibling endpoint).
  With no secret configured the unauthenticated log-only behavior is
  unchanged. A merge that restores upstream's handler silently drops
  ingestion with no compile error on the config knob.
- **Hook endpoints are auth-rate-limited and wipe-safe.** Secret guessing on
  both hook endpoints goes through the gateway auth limiter (mirroring
  `/webhook`), a transcript upload that parses to zero turns never clears the
  live rows (`replaced: false`), and SQLite transcript replacement is one
  transaction (`SessionBackend::replace_messages`). The `claude_code_runner`
  tool's own hook posting is legacy/best-effort: it cannot attach the secret
  header or `?agent=`, so runner-spawned sessions do not ingest (documented
  at the `hook_url` site in `claude_code_runner.rs`).
- **`SessionPicker` owns the agent's own conversations; `ThreadsPanel` owns the
  rest.** v0.8.5 shipped upstream's own multi-conversation UI (#9353, #9355),
  which duplicated the fork's `gw_` thread list — both were mounted in
  `AgentChat` at once. The duplicate list was removed from `ThreadsPanel`
  (-269 lines net), leaving it a read-only browser for the channel, `cc_` and
  `rpc_`/TUI families. This is not cosmetic: only `SessionPicker` tracks
  `reservedSessionIds`, so only it can refuse to put two sockets on one gateway
  session. Do not re-add switch/rename/delete here — route them through
  `SessionPicker`.

- **`startNewThread` returns `boolean`.** Upstream gates every conversation
  transition on `sessionPersistence === true`; without the return value that
  gate is a silent no-op, so `/new` reports `agent.sessions_unavailable`
  instead of appearing to work. `switchThread` was removed with the duplicate
  list.

- **A `max_tokens` stop is no longer silent.** Upstream parses `finish_reason`
  but only ever compares it to `"tool_calls"`, and the non-streaming `Choice`
  has no `finish_reason` field at all — so a reply cut off at the output-token
  ceiling ships as though it were complete, and a reply whose whole budget went
  to thinking ships as nothing. Ours logs a `WARN` and appends
  `TRUNCATION_NOTICE` on both paths. The notice is emitted as an ordinary
  `StreamEvent::TextDelta`, deliberately NOT a new enum variant (that enum has
  ~168 references across 15 files), so every channel renders it unchanged. It
  carries no square brackets and no filesystem path because the Discord
  dispatcher promotes `[IMAGE:...]`-shaped text and bare paths into media
  markers; a promoted warning would replace the reply with a delivery failure.
  A merge that restores upstream's `Choice` struct drops the non-streaming half
  with no compile error.

- **`cargo test` no longer wipes this node's Tailscale serve config.**
  `TailscaleTunnel::stop()` (`crates/zeroclaw-runtime/src/tunnel/tailscale.rs`)
  shells out to `tailscale <serve|funnel> reset`, which clears the *entire*
  serve config for the node rather than just the tunnel's own port, and
  `tunnel::tailscale::tests::stop_without_started_process_is_ok` calls `stop()`
  unconditionally against the real binary. On 2026-09-05 15:12:41, a
  `cargo test --locked --workspace` during the v0.8.5 merge therefore deleted
  the `https://ubuntu-ai-amd.taile483f.ts.net → 127.0.0.1:11437` mapping that
  `claude-remote` depends on; it went unnoticed until 2026-09-11 and was
  reproduced on that date by running the single test. The fix is local and
  outside the source tree: `.cargo/config.toml` sets
  `[target.x86_64-unknown-linux-gnu] runner` to
  `~/.local/libexec/cargo-tailscale-guard/runner`, which prepends a passthrough
  `tailscale` stub to `PATH` for every test/bench/run binary — all subcommands
  reach `/usr/bin/tailscale` except `serve reset` and `funnel reset`, which
  no-op with a message on stderr. A merge that overwrites `.cargo/config.toml`
  drops the guard with no compile error and no test failure; the symptom is
  `tailscale serve status` reporting `No serve config` after a test run.

- **Bracketed prose streams.** Upstream's `StreamTextGuard` holds any reply
  from its first `[` or `{` to the end of the turn unless the held text parses
  as JSON, so avatar tags (`[ACT ...]`, `[DELAY 1]`) turned a streamed reply into
  one chunk. A merge that takes upstream's `stream_guard.rs` drops the release
  and its tests together, with no compile error; the symptom is a `default`
  agent reply arriving on `/ws/chat` as a single `chunk`.
