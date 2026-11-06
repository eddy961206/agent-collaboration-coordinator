# Implementation contract

Isolated implementation, stdlib Python 3.12. No imports or writes to existing services.
No real model, CLI or existing user session execution. Default deployment target is the lab only.

## Store API

`Store(path)` opens own SQLite DB; every mutating method uses transactional checks.
`CoordinationError(code)` for expected rejections. All methods return JSON-compatible dicts.

- create_task(task_id, repo, issue, owner, adapter, text, decision_owner=None)
- task(task_id), tasks(), run(run_id), runs(task_id=None), events(task_id=None)
- change(task_id, actor, base_revision, text, event_id) -> accepted/conflict with latest task
- record_event(task_id, source_id, source_version, body_hash, actor, body, disposition) -> duplicate or recorded
- start(task_id, actor, expected_revision=None, expected_generation=None) -> run dict with id/task_id/owner/adapter/revision/generation/status/text, new generation, one active run per task; ready or paused only. Runtime supplies expected values.
- set_handle(run_id, handle)
- acknowledge(run_id, revision) -> only current run/generation; stores current accepted revision
- stop(task_id, actor, expected_run_id=None) -> invalidates generation, run stopping, task stopping. Runtime supplies the targeted run id.
- confirm_stopped(run_id, descendants_stopped: bool) -> paused only when all stopped
- get_snapshot(run_id) -> task/run/revision/text; run stale state remains explicit
- heartbeat(run_id); expire(run_id) -> unknown; explicit confirm_stopped required before resume
- reserve_operation(run_id, operation_id, payload, resource=None, environment=None)
- begin_operation(operation_id) -> checks again before dispatch; only `dispatch=True` authorizes an external effect. Duplicate success/executing/unknown never redispatched.
- finish_operation(operation_id, success: bool | None, receipt=None)
- reconcile_operation(operation_id, success: bool, receipt)
- operation(operation_id)
- finish(run_id, result, evidence) -> reject stale revision/owner; marks done (no production deployment claim)
- acquire_resource(name, run_id), release_resource(name, run_id), bump_environment(name), environment(name)
- set_source_pending(task_id, pending: bool)

Task revision monotonically increases on accepted change; only owner/decision_owner may mutate.
`change` does not advance run revision; worker must acknowledge after reading.
`start` returns a new run id even on resume. It does not silently start from unknown/stopping.
Conflicts retained; cannot publish while conflict/source pending unresolved.
Operations store immutable payload hash and run stamp. Unknown external result must reconcile.
No external network, process or model calls inside Store.

## Adapter API

`start(run: dict) -> handle: dict`
`deliver(handle: dict, snapshot: dict) -> receipt: dict`
`stop(handle: dict) -> receipt: dict`
`observe(handle: dict) -> {stopped: bool, children: list, ...}`
`close()` cleans up only owned fake workers. All calls bounded by timeout.
`DirectAdapter(workspace_root)` starts only our stdlib fake worker using sys.executable,
in a per-run directory, sanitized environment. It supports start/change/stop without model calls.
`RoundtableAdapter(base_url, participants)` uses fresh rooms on a separate lab RT.
No existing room reuse or production URLs. RT delivery is transport receipt, not semantic acknowledgement.
Adapter errors never turn into success. Runtime explicitly records worker acknowledgement separately.

## Root runtime

Authenticated local HTTP API, signed Gitea webhooks, typed `/coord` comments,
explicit repository/actor allowlist, source event dedup+edit handling,
poll/run actions only through coordinator, Gitea status/result publisher with operation ledger,
and lab harness. Every task has one dispatcher; no production dispatcher registration.
