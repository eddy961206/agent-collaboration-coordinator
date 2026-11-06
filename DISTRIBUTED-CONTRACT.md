# Native and distributed implementation boundary

Coordinator and worker hosts are separate processes. Each worker host owns its CLI/RT processes and durable job ledger. Coordinator death must not kill worker processes. Workers have no Gitea publishing credentials; only the coordinator publishes.

## Engines (native_cli.py)

`NativeEngine(kind, executable, work_root, provider_config, checkpoint_callback=None)`
kind: claude or codex. Provider config must explicitly select mode fixture/live; fixture uses fake HTTP API and dummy key, no existing home/auth. No real model calls in tests.

Existing adapter-shaped methods: `start(run) -> handle`, `deliver(handle,snapshot) -> receipt`, `stop(handle) -> receipt`, `observe(handle) -> observation`, `close()`.
run fields: id/task_id/owner/adapter/revision/generation/text; optional `resume_session` (owned prior engine handle). Engine must validate prior ownership/task and stopped state before reuse. Capture native session/thread/turn IDs; preserve home/session history across explicit resume. Individual child CLI approval requests must fail closed, not hang silently. Native CLI actual successful output alone is not coordinator task completion.
Observation includes stopped, status, session_id, turn_id when applicable, accepted_revision (transport only), result if completed. Runtime's semantic acknowledgement remains explicit.
No shell=True, no executable from HTTP caller, no user credential files copied into tests. Each task has isolated engine home/workspace.

## Worker host (worker_host.py)

`WorkerHost(state_dir, node_id, token, engines, allowed_owners)` and `make_worker_server(host, address)`.
Engines are config-owned adapter objects, not caller-provided executable paths. Bind loopback only; across PCs use authenticated SSH tunnels (already verified host keys), or TLS configured separately. Authenticated requests must identify expected node ID.

API: GET /v1/info, POST /v1/start, POST /v1/deliver, POST /v1/stop, POST /v1/observe. All require `Authorization: Bearer <node-token>` including info. Body contains durable handle; start also has run. Mutation request body includes `operation_id`.
Persistent jobs key is run_id; record launching before engine.start. Same ID+payload must never start twice. Ambiguous host restart/launch remains unknown rather than retry. Per-run command receipts deduplicate deliver/stop. An in-progress operation after restart is unknown. Stop of already stopped job must never affect resumed work. `resume_from` on start refers to an owned stopped prior job for same task/engine and passes its engine handle to engine.start as run.resume_session.

## RemoteAdapter (remote.py)

`RemoteAdapter(base_url, node_id, token, engine, allowed_owners)`.
`prepare(run)` performs a read-only identity handshake and returns a stable handle BEFORE any mutating RPC. Handle: adapter=remote, node_id, ledger_id, engine, run_id, binding hash of immutable run launch and a signed proof. No in-memory ownership requirement on coordinator side. Persisted handles must work after new RemoteAdapter creation. Validate node/ledger identity and handle proof on every response.
`start_prepared(run, saved_handle)` uses a deterministic operation ID and the already persisted identity. Never re-prepare when retrying an existing launch. `start(run)` is a convenience wrapper for independent clients. On uncertainty exception.handle preserves prepared identity. `.deliver/.stop/.observe` use authenticated worker RPC and stable operation IDs; `.recover(handle)` observes without restarting a job; not-found must be distinct from unknown. `.close()` does not stop remote jobs.
Allowed endpoint transport: http loopback only (intended SSH tunnel), https with verified CA or explicit CA file. No redirects, inherited proxies, production RT endpoints or arbitrary URL per request.

## Coordinator recovery (root-owned)

Store durable launch payload and prepared handle before remote dispatch. Mark recoverable remote runs recovering without incrementing generation; block publication. Query worker's matching node/run/binding. If still running, restore same execution identity, no duplicate launch. If stopped, confirm stop; if completed, preserve final result for finish. If not found, replay exact persisted launch via the worker's idempotent start. If uncertain/unreachable, stay recovering, hold resources, do not reassign.
Recover interrupted start/change/stop command receipts from factual worker/task state, preserving source-review blocks separately. Never treat absence of external comment as proof of failed publication; reconcile existing operation receipts.
Actual coordinator process termination+new process test is required, not merely recreating an object with old adapters. Physical second-PC worker test is separate from multi-container test.

Completed worker output is persisted as result_ready/reviewed=false. It is not automatically published or accepted as correct. The recorded native output revision and coordinator acknowledgement remain separate. All publication operations left executing at coordinator restart become unknown for explicit source reconciliation.
