# Scope

This repository is an isolated Gitea collaboration coordinator prototype.
Keep production services, credentials, SQLite databases, repositories and running sessions untouched.
Never import the existing Claude/Codex issue-agent modules; their imports can mutate state.

# Implementation and verification

- Stdlib Python. Keep durable state in Store/Journal, transport in adapters, authority in Runtime.
- One Runtime owns a state directory. Different tasks execute concurrently; task-scoped mutations serialize.
- A successful transport response is not an AI semantic acknowledgement.
- Tests must not call a real model service. For the user's explicitly requested native integration, real Claude/Codex CLI processes may run only with isolated homes, dummy credentials, local fake provider endpoints and external network blocked. Existing app sessions remain untouched.
- The real integration lab uses an internal Docker network and synthetic accounts only.
- Runtime artifacts stay in ignored `.runtime/`. Do not commit credentials or raw production issue snapshots.
- For state/authority changes run `python -m lab.check`. For transport/runtime changes run the real isolated lab as described in README.
- Report code verification, adapter transport, actual model behavior and production deployment separately.
- Native worker hosts and coordinator restart recovery are in scope. Connecting existing live tasks or changing production dispatch remains out of scope; no implicit live cutover.
