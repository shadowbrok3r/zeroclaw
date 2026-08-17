# Gateway HTTP API

The gateway exposes a REST surface alongside the local CLI. Anything that can
be set with `zeroclaw config get/set/list/init/migrate` is also reachable via
HTTP, so the dashboard, third-party tooling, and the CLI all drive the same
underlying `Config` mutation core.

This page is a high-level overview. Field-level definitions, request and response shapes, and "Try it out" forms for the currently documented OpenAPI subset live at `/api/docs` on a running gateway. Those schemas come from runtime types, but the route inventory is assembled separately and does not yet cover every route registered by the gateway. The router in `crates/zeroclaw-gateway/src/lib.rs` remains the authority for the full live surface.

> Tracked under issue #6175.

## Authentication

The configuration value reads and mutations described on this page are gated
by the existing pairing and bearer authentication. Shape discovery through `/api/docs`,
`/api/openapi.json`, and config `OPTIONS` is public. A first-run pairing code is
printed when the daemon starts; subsequent authenticated calls send the derived
bearer token in the `Authorization` header. The Scalar explorer at `/api/docs`
exposes an "Authentication" panel where you paste the token before issuing
authenticated calls.

Local-bound by default. Over-the-network access requires TLS termination at
the gateway or in front of it; the per-property and PATCH endpoints are not
safe to expose unauthenticated regardless of TLS posture.

## Discovering the surface

Two endpoints answer the question "what can I do here?":

- `OPTIONS /api/config` returns the JSON Schema for the whole-config type.
  Static per build; clients should cache against the `ETag` header. Its current
  `Allow` header still lists legacy `PUT`, which the router does not register.
- `OPTIONS /api/config/prop?path=<dotted>` returns the schema fragment for a
  specific path with `Allow: GET, PUT, DELETE, OPTIONS`. Returns 404 if the
  path doesn't exist in the schema.

`OPTIONS` returns capabilities. `GET /api/config/prop` and `GET /api/config/list` return the user's current values. Forms in the dashboard issue `OPTIONS` once at load time to learn types and constraints, then `GET` to populate fields, then `PUT`/`PATCH` to write. A compatibility `GET /api/config` also returns a whole-config snapshot with secrets masked so older bundled dashboard pages do not fail against newer gateways. New clients should prefer the per-property surface because it carries field metadata and explicit secret handling.

CORS preflight requests (those carrying `Access-Control-Request-Method`) get
the standard preflight response and short-circuit before the schema body is
returned.

## Per-property CRUD

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/config` | Compatibility whole-config snapshot with secrets masked; new clients should prefer the per-property surface. |
| `PATCH` | `/api/config` | Apply a JSON Patch (RFC 6902) document atomically. |
| `OPTIONS` | `/api/config` | Whole-config JSON Schema (capabilities, not values). |
| `GET` | `/api/config/prop?path=...` | Read one field. Secrets return `{path, populated}` only. |
| `PUT` | `/api/config/prop` | Write one field. Body: `{path, value, comment?}`. Secrets respond with `{path, populated: true}` only. |
| `DELETE` | `/api/config/prop?path=...` | Reset one field to its default. Secrets respond with `{path, populated: false}`. |
| `OPTIONS` | `/api/config/prop?path=...` | Per-field schema fragment. |
| `GET` | `/api/config/list?prefix=...` | Enumerate every reachable path with type and category. Secret entries carry `{path, populated, is_secret: true}` and no value. |
| `POST` | `/api/config/init?section=...` | Instantiate `None` nested sections with defaults. Dynamic-map aliases are not created here; use `POST /api/config/map-key`. |
| `POST` | `/api/config/migrate` | Apply on-disk schema migration in place. Mirrors `zeroclaw config migrate`. |

## Atomic batch writes: JSON Patch

`PATCH /api/config` accepts a JSON Patch document (RFC 6902). The supported
config operations are `add`, `replace`, `remove`, and `test`. ZeroClaw also
accepts a `comment` extension for config annotations. Config operations run
against an in-memory copy; once every operation has applied,
`Config::validate()` runs once on the result. If validation passes, the new
state is persisted and swapped in. If any operation or final validation fails,
on-disk and in-memory state are unchanged. Comment annotations are applied
after the save on a non-fatal, best-effort basis.

`move` and `copy` return `400 op_not_supported` because safe reference-graph
rewriting is not part of this surface. `test` against a `#[secret]` path is
rejected with `secret_test_forbidden`: a differential outcome would be the
only signal a client could read, and that would leak the value.

Path syntax: JSON Pointer (`/agents/researcher/model_provider`) or the
dotted form (`agents.researcher.model_provider`). Both are accepted; the
server normalises.

The CLI counterpart is `zeroclaw config patch <file-or-stdin>`, which applies
the same op set against the local Config and returns the same structured
response shape (`--json` for scripts).

## Secrets: write-only over HTTP

Per-property reads never expose secret fields (those marked `#[secret]` or
`#[derived_from_secret]` in the schema). Their responses carry
`{populated: bool}` only, with no value, length, masked stand-in, or hash. The
compatibility `GET /api/config` instead serializes the whole config after
applying `MaskSecrets`, so secret fields can appear there only as masked
placeholders. Neither config read surface returns the underlying secret value.

`PUT` and `PATCH` write the new secret value and respond with
`{populated: true}`; `DELETE` clears it and responds with
`{populated: false}`. There is no HTTP path to retrieve a secret by any means.

## Stable error codes

Errors return JSON with a stable `code` field plus a human-readable `message`.
Frontends and scripts match against the code; UI matches against the path.

| Code | Status | Meaning |
|---|---|---|
| `path_not_found` | 404 | The requested property does not exist in the schema. |
| `validation_failed` | 400 | The whole-config validator rejected the proposed state. |
| `dangling_reference` | 400 | A configured alias reference (e.g. `agents.<x>.model_provider`) names a missing target (e.g. `providers.models.<type>.<alias>`). |
| `value_type_mismatch` | 400 | The submitted JSON value cannot coerce into the target type. |
| `op_not_supported` | 400 | JSON Patch op is `move` / `copy` / unknown. |
| `secret_test_forbidden` | 400 | JSON Patch `test` op targeted a secret path. |
| `config_changed_externally` | 409 | The on-disk config drifted from the in-memory copy. (See drift detection.) |
| `reload_failed` | 500 | The save succeeded but daemon reload could not pick up the new state; on-disk reverted. |
| `internal_error` | 500 | Unclassified server-side failure. |

## Live exploration

Once a gateway is running, browse to `http://<gateway-host>:<port>/api/docs` for the Scalar API explorer. The raw specification is available at `/api/openapi.json` for other compatible viewers.

The explorer's authentication panel binds to the `bearerAuth` scheme declared
in the spec, paste your pairing-derived bearer token there before issuing
live calls. The CLI shortcut for the URL is `zeroclaw config docs`.

If the Scalar bundle can't load from the CDN (offline / air-gapped install),
the page degrades gracefully and points you at the raw spec at
`/api/openapi.json` so you can use any compatible viewer
(Insomnia, Postman, Swagger UI, etc.).

## Session endpoints

The gateway exposes the unified session store (see
[Session lifecycle](../architecture/session-lifecycle.md)) over REST. All
endpoints require the pairing-derived bearer token.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/sessions` | List sessions with metadata (name, agent, channel, timestamps, message count). Rows attributable to no agent or channel are skipped. |
| `GET` | `/api/sessions/{id}/messages` | Read the persisted transcript. |
| `POST` | `/api/sessions/{id}/messages` | Send a message into the session and run a turn. |
| `PUT` | `/api/sessions/{id}` | Rename the session. |
| `DELETE` | `/api/sessions/{id}` | Delete the session's message rows and metadata. |
| `GET` | `/api/sessions/{id}/state` | Per-turn state: `idle`, `running`, or `error`. |
| `POST` | `/api/sessions/{id}/abort` | Abort the session's running turn. |
| `GET` | `/api/sessions/running` | List sessions currently mid-turn. |

`{id}` resolution is uniform across all verbs: the id is tried verbatim as a
store key first, then as `gw_<id>`, then as `rpc_<id>`. Channel-composite and
RPC sessions therefore get the same verb set as gateway WebSocket sessions.

Session mutations also emit lifecycle frames on `/api/events` with
`source: "sessions"` and `type` of `session_created`, `session_update`, or
`session_closed`; the field contract is documented in
[Session lifecycle](../architecture/session-lifecycle.md#session-lifecycle-sse-events).

### Claude Code hook endpoints

Two endpoints ingest remote Claude Code sessions into the `cc_` key family.
They do not use bearer auth: they are gated by the shared secret configured
as `claude_code.hook_secret`, presented via the `X-ZC-Hook-Secret` header.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/hooks/claude-code?agent=<alias>` | Ingest one hook event (native Claude Code hook JSON or the legacy `ClaudeCodeHookEvent` shape) into `cc_<session_id>`. With no `hook_secret` configured the endpoint is log-only and unauthenticated (unchanged historical behavior). With a secret configured, a correct header ingests; a missing or wrong header answers 401. |
| `POST` | `/hooks/claude-code/transcript?session=<sid>&agent=<alias>` | Replace the session's live rows with a parsed Claude Code transcript JSONL tail (8 MiB body cap, 413 above it). Same header auth; answers 404 when no `hook_secret` is configured, 401 on a missing or wrong header. Metadata (name, agent alias) survives the replacement. |

Both endpoints validate `session`/`session_id` against `[A-Za-z0-9_-]{1,64}`
and require `?agent=` to name a configured agent; violations answer 400. See
[Claude Code sessions](../architecture/session-lifecycle.md#claude-code-sessions)
for the event mapping and contract rules.

## Event stream contract

`GET /api/events` is a raw Server-Sent Events stream of observable runtime
events. It is not a deduplicated one-row-per-turn lifecycle timeline.

Gateway handlers, webhook handling, cron/heartbeat work, and agent-loop
observers can all publish lifecycle-shaped events into the same broadcast path.
Clients should treat the stream as an append-only observation log. If a
dashboard wants a compact turn timeline, it should group or deduplicate by the
identifiers present on the event payload rather than assuming each
`agent_start`, `llm_request`, or `agent_end` frame appears only once.

`GET /api/events/history` replays the retained recent events from the same
buffer, oldest first. It is a reconnect window for subscribers, not a separate
canonical lifecycle store.
