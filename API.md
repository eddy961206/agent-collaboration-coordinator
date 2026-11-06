# Boundary and API

## Modules

| Module | Responsibility |
|---|---|
| `store.py` | SQLite transactions: task, revision, generation, run, resource and operation ledger |
| `journal.py` | Input-command receipts and uncertain status-comment creation |
| `instance.py` | One coordinator per state directory, enforced by an OS lock |
| `runtime.py` | Per-task orchestration, source validation and publication |
| `server.py` | Bearer-authenticated lab API and HMAC-signed Gitea webhooks |
| `transport.py` | Allowlisted lab Gitea endpoints, no redirects or inherited proxies |
| `adapters.py` | Fixed direct fake worker and real lab RT rooms |
| `fake_worker.py` | Deterministic parent/child protocol, no model code |
| `native_cli.py` | Owned Claude Code and Codex app-server processes, native sessions and explicit resume |
| `worker_host.py`, `remote.py` | Separate authenticated PC workers, durable node ledger and transport receipts |
| `recovery.py` | Reconnect matching workers, replay durable intentions, backfill source and collect unreviewed output |

All JSON has explicit task and run identities. A human actor is taken from its bearer token or from the canonical Gitea comment author ID and login, never from an untrusted `actor` body field.

## HTTP

All `/api` endpoints require bearer authentication. Worker tokens are scoped to one run. Viewer accounts may read but cannot mutate. A task owner and decision owner may operate that task; conflict/source review decisions are restricted to its decision owner.

| Method/path | Input or behavior |
|---|---|
| `GET /health` | Lab mode, no model execution |
| `GET /api/tasks` | Registered tasks |
| `POST /api/tasks` | `repo`, `issue`, `owner`, `decision_owner`, `adapter`, `text`. Registers one allowlisted real lab issue |
| `GET /api/tasks/{encoded task_id}` | Current task, runs, audit events, status publication error |
| `POST /api/tasks/{encoded task_id}/commands` | `action`, `data`; requires `Idempotency-Key` |
| `POST /api/tasks/{encoded task_id}/status` | Refresh/reconcile the single service-owned status comment |
| `GET /api/runs/{run_id}` | Current run record |
| `POST /api/runs/{run_id}/ack` | Worker token + `revision`; explicitly acknowledge current request |
| `POST /api/runs/{run_id}/publish` | Worker token + `operation_id`, `body`; real lab Gitea comment |
| `POST /api/runs/{run_id}/finish` | Worker token + `result`, `evidence`; children must already be stopped |
| `POST /api/operations/{id}/reconcile` | Authorized task actor; confirm exactly one matching bot comment for an unknown publication |
| `POST /webhooks/gitea` | `X-Gitea-Signature` HMAC-SHA256 and `X-Gitea-Event: issue_comment` |

Encode the full task ID (for example `labadmin/synthetic#1`) as one path component. POST result loss does not authorize a new operation ID. The operation ledger blocks all further external actions for a task while an outcome remains unknown.

Commands:

```json
{"action":"start","data":{"revision":1}}
{"action":"change","data":{"base_revision":1,"text":"Complete updated request"}}
{"action":"stop","data":{"run_id":"specific-current-run"}}
{"action":"resume","data":{"revision":2,"generation":2}}
{"action":"deliver","data":{"run_id":"specific-current-run"}}
{"action":"observe","data":{"run_id":"specific-current-run"}}
{"action":"recover","data":{}}
{"action":"review_sources","data":{"base_revision":2,"through_event_id":17,"reason":"Reviewed all source events through this ID"}}
```

Use a distinct command key for a distinct command. The same key and same task/payload returns its durable result. Reusing it across tasks or with different data is rejected. Accepted request changes remain accepted if delivery fails or the worker is stopping; delivery is reported as pending/after_resume.

## Configuration example

Only synthetic credentials belong in lab config. Generate a unique state directory and keys for each lab. Keep deployment configs and credential files outside Git. Gitea supports allowlisted HTTPS with default certificate verification or an explicit `ca_file`; `token_file` may be used instead of a password.

```json
{
  "mode": "isolated-lab",
  "state_dir": "/out/state",
  "listen": "127.0.0.1",
  "port": 18881,
  "worker_secret": "unique-fixture-only-value",
  "webhook_secret": "unique-fixture-only-value",
  "actors": {
    "alice": {"gitea_id": 2, "role": "operator", "token": "unique-fixture-alice"},
    "bob": {"gitea_id": 3, "role": "operator", "token": "unique-fixture-bob"}
  },
  "gitea": {
    "url": "http://gitea:3000",
    "allowed_hosts": ["gitea"],
    "user": "labadmin",
    "password": "generated-fixture-password",
    "repos": ["labadmin/synthetic"]
  },
  "roundtable": {
    "url": "http://127.0.0.1:18787",
    "allowed_hosts": ["127.0.0.1"],
    "participants": ["claude", "claude"]
  }
}
```

RT must already be configured to use the intended owned CLI/provider. Tests require fake providers. No API merges a PR, deploys software, kills an arbitrary process, or adopts an existing app session.

## Distributed configuration

The coordinator supports `mode: distributed` and a `remote_workers` object. These entries become task adapter names. Worker endpoints use verified SSH loopback forwarding or HTTPS with a trusted CA. Coordinator shutdown does not stop remote workers.

```json
{
  "remote_workers": {
    "alice-claude": {
      "url": "http://127.0.0.1:18891",
      "node_id": "pc-alice",
      "token_file": ".runtime/alice-node.token",
      "engine": "claude",
      "allowed_owners": ["alice"],
      "timeout": 5
    }
  }
}
```

Start a worker with `python -m coordinator.worker_host --config <file>`. Its config has `state_dir`, `node_id`, `token`, `allowed_owners`, loopback `listen`, `port` and `engines`. Fixture engine configuration is `{ "kind": "fixture" }`. RT uses `kind: rt`, `url`, `participants`, `allowed_hosts`. Native engines use `kind: claude` or `codex`, an absolute `executable` and `provider_config`.

```json
{
  "state_dir": ".runtime/worker-alice",
  "node_id": "pc-alice",
  "token": "REPLACE_WITH_A_PRIVATE_NODE_SECRET",
  "allowed_owners": ["alice"],
  "listen": "127.0.0.1",
  "port": 18891,
  "engines": {
    "fixture": {"kind": "fixture"},
    "claude": {
      "kind": "claude",
      "executable": "/absolute/path/to/claude",
      "provider_config": {
        "mode": "fixture",
        "network_isolated": true,
        "base_url": "http://127.0.0.1:19091",
        "timeout": 20
      }
    }
  }
}
```

Live native mode requires `allow_model_calls: true`, an explicit `model`, `base_url` and `api_key_env` naming the worker machine's credential environment variable. It uses a new managed task home and owned session. It does not copy a personal OAuth home. Requests requiring an ungranted tool approval are rejected. Fixture modes disable real tools and use dummy credentials.

The worker RPC requires `Authorization: Bearer <node token>` and `X-Worker-Node: <node ID>`. `GET /v1/info` returns node/ledger identity. `POST /v1/start`, `/v1/deliver`, `/v1/stop`, `/v1/observe` use a signed durable handle. Mutations have operation IDs. All responses are bound to node, ledger and run. A worker database replacement is distinguishable from a process restart.

Task/run `result_ready` means output was collected and awaits review. It is not `done`. A coordinator restart reconnects active workers; a worker-host restart with uncertain process ownership stays unknown. Unknown publications are reconciled separately from lifecycle recovery.

## Existing-service migration seam

The current Claude service has one worker across issues and source IDs without edit revisions. Its module opens/migrates its own database. This prototype therefore consumes independent event JSON and never imports it. The previous Codex receiver also initializes state when imported.

A future per-task handoff must stop new dispatch from the old service, finish or reconcile its active work, preserve its source receipts, register the task here, then enable the new dispatcher for that task. Never run both dispatchers for the same task. Existing issue URLs remain the source reference. This repository has no command that changes the production dispatcher.
