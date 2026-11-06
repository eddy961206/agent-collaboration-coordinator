"""Durable coordination rules; no network, processes, or model calls.

Each mutation opens its own connection and checks state under BEGIN IMMEDIATE.
External effects are deliberately not executed here: an operation can have an
unknown result, and only explicit reconciliation can settle that result.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import uuid


class CoordinationError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


class Store:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._reader() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, repo TEXT NOT NULL, issue INTEGER NOT NULL,
                    owner TEXT NOT NULL, decision_owner TEXT NOT NULL, adapter TEXT NOT NULL,
                    text TEXT NOT NULL, revision INTEGER NOT NULL, generation INTEGER NOT NULL,
                    status TEXT NOT NULL, active_run_id TEXT,
                    source_pending INTEGER NOT NULL DEFAULT 0,
                    conflict_pending INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_task_per_issue
                    ON tasks(repo COLLATE NOCASE, issue);
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
                    owner TEXT NOT NULL, adapter TEXT NOT NULL, revision INTEGER NOT NULL,
                    generation INTEGER NOT NULL, status TEXT NOT NULL, text TEXT NOT NULL,
                    handle TEXT, result TEXT, evidence TEXT,
                    heartbeat_at REAL NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_task ON runs(task_id)
                    WHERE status IN ('running', 'stopping', 'unknown');
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES tasks(id),
                    type TEXT NOT NULL, payload TEXT NOT NULL, created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS changes (
                    task_id TEXT NOT NULL REFERENCES tasks(id), event_id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, status TEXT NOT NULL, revision INTEGER NOT NULL,
                    PRIMARY KEY(task_id, event_id)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    task_id TEXT NOT NULL REFERENCES tasks(id), source_id TEXT NOT NULL,
                    source_version TEXT NOT NULL, body_hash TEXT NOT NULL, body TEXT NOT NULL,
                    actor TEXT NOT NULL, event_id INTEGER NOT NULL REFERENCES events(id),
                    PRIMARY KEY(task_id, source_id, source_version)
                );
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
                    task_id TEXT NOT NULL REFERENCES tasks(id), payload TEXT NOT NULL,
                    payload_hash TEXT NOT NULL, revision INTEGER NOT NULL, generation INTEGER NOT NULL,
                    resource TEXT, environment INTEGER, status TEXT NOT NULL,
                    receipt TEXT, reason TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS resources (
                    name TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id)
                );
                CREATE TABLE IF NOT EXISTS environments (
                    name TEXT PRIMARY KEY, generation INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS launches (
                    run_id TEXT PRIMARY KEY REFERENCES runs(id), command_key TEXT UNIQUE,
                    payload TEXT NOT NULL
                );
            """)
            columns = {row['name'] for row in connection.execute('PRAGMA table_info(tasks)')}
            if 'recovery_pending' not in columns:
                connection.execute('ALTER TABLE tasks ADD COLUMN recovery_pending INTEGER NOT NULL DEFAULT 0')
            connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_task_v2 ON runs(task_id) WHERE status IN ('running','stopping','unknown','recovering')")
            connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_task_v3 ON runs(task_id) WHERE status IN ('running','stopping','unknown','recovering','result_ready')")

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def _transaction(self):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _reader(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _row(connection, table, identity):
        row = connection.execute(f"SELECT * FROM {table} WHERE id=?", (identity,)).fetchone()
        if row is None:
            raise CoordinationError(f"{table[:-1]}_not_found")
        return dict(row)

    @staticmethod
    def _task_json(row):
        value = dict(row)
        value["task_id"] = value["id"]
        value["source_pending"] = bool(value["source_pending"])
        value["conflict_pending"] = bool(value["conflict_pending"])
        value['recovery_pending'] = bool(value.get('recovery_pending', False))
        return value

    @staticmethod
    def _run_json(row):
        value = dict(row)
        for key in ("handle", "result", "evidence"):
            value[key] = json.loads(value[key]) if value[key] is not None else None
        return value

    @staticmethod
    def _operation_json(row, dispatch=False):
        value = dict(row)
        value["payload"] = json.loads(value["payload"])
        value["receipt"] = json.loads(value["receipt"]) if value["receipt"] is not None else None
        value["dispatch"] = dispatch
        return value

    @staticmethod
    def _event_json(row):
        value = dict(row)
        value["payload"] = json.loads(value["payload"])
        return value

    @staticmethod
    def _event(connection, task_id, kind, payload):
        return connection.execute(
            "INSERT INTO events(task_id,type,payload,created_at) VALUES(?,?,?,?)",
            (task_id, kind, _json(payload), time.time()),
        ).lastrowid

    @staticmethod
    def _authorize(task, actor):
        if actor not in (task["owner"], task["decision_owner"]):
            raise CoordinationError("forbidden_actor")

    def _current_run(self, connection, run_id, require_revision=True, publish=False):
        run = self._row(connection, "runs", run_id)
        task = self._row(connection, "tasks", run["task_id"])
        if task["active_run_id"] != run_id or task["generation"] != run["generation"]:
            raise CoordinationError("stale_generation")
        if run['status'] not in ('running', 'result_ready') or task['status'] not in ('working', 'result_ready'):
            raise CoordinationError("run_not_running")
        if require_revision and run["revision"] != task["revision"]:
            raise CoordinationError("stale_revision")
        if publish and task['recovery_pending']:
            raise CoordinationError('recovery_pending')
        if publish and task["source_pending"]:
            raise CoordinationError("source_pending")
        if publish and task["conflict_pending"]:
            raise CoordinationError("conflict_pending")
        return task, run

    def create_task(self, task_id, repo, issue, owner, adapter, text, decision_owner=None):
        if not all(isinstance(value, str) and value for value in (task_id, repo, owner, adapter)):
            raise CoordinationError("invalid_task")
        if type(issue) is not int or issue <= 0 or not isinstance(text, str):
            raise CoordinationError("invalid_task")
        now = time.time()
        with self._transaction() as connection:
            if connection.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone():
                raise CoordinationError("task_exists")
            if connection.execute("SELECT 1 FROM tasks WHERE repo=? COLLATE NOCASE AND issue=?", (repo, issue)).fetchone():
                raise CoordinationError("issue_already_registered")
            connection.execute("""INSERT INTO tasks
                (id,repo,issue,owner,decision_owner,adapter,text,revision,generation,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,1,0,'ready',?,?)""",
                (task_id, repo, issue, owner, decision_owner or owner, adapter, text, now, now))
            self._event(connection, task_id, "task_created", {"owner": owner, "revision": 1})
            return self._task_json(self._row(connection, "tasks", task_id))

    def task(self, task_id):
        with self._reader() as connection:
            return self._task_json(self._row(connection, "tasks", task_id))

    def tasks(self):
        with self._reader() as connection:
            return [self._task_json(row) for row in connection.execute("SELECT * FROM tasks ORDER BY created_at,id")]

    def run(self, run_id):
        with self._reader() as connection:
            return self._run_json(self._row(connection, "runs", run_id))

    def runs(self, task_id=None):
        with self._reader() as connection:
            rows = connection.execute("SELECT * FROM runs WHERE task_id=? ORDER BY created_at,id", (task_id,)) if task_id else connection.execute("SELECT * FROM runs ORDER BY created_at,id")
            return [self._run_json(row) for row in rows]

    def events(self, task_id=None):
        with self._reader() as connection:
            rows = connection.execute("SELECT * FROM events WHERE task_id=? ORDER BY id", (task_id,)) if task_id else connection.execute("SELECT * FROM events ORDER BY id")
            return [self._event_json(row) for row in rows]

    def change(self, task_id, actor, base_revision, text, event_id):
        if not isinstance(text, str) or not text or not isinstance(event_id, str) or not event_id:
            raise CoordinationError("invalid_change")
        fingerprint = _hash({"actor": actor, "base_revision": base_revision, "text": text})
        with self._transaction() as connection:
            task = self._row(connection, "tasks", task_id)
            self._authorize(task, actor)
            old = connection.execute("SELECT * FROM changes WHERE task_id=? AND event_id=?", (task_id, event_id)).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise CoordinationError("event_id_collision")
                return {"status": "duplicate", "original_status": old["status"], "event_id": event_id, "task": self._task_json(task)}
            if base_revision != task["revision"] or (task["conflict_pending"] and actor != task["decision_owner"]):
                status = "conflict"
                connection.execute("UPDATE tasks SET conflict_pending=1,updated_at=? WHERE id=?", (time.time(), task_id))
            else:
                status = "accepted"
                connection.execute("""UPDATE tasks SET text=?,revision=revision+1,conflict_pending=0,
                    status=CASE WHEN status='done' THEN 'ready' ELSE status END,updated_at=? WHERE id=?""", (text, time.time(), task_id))
            updated = self._row(connection, "tasks", task_id)
            connection.execute("INSERT INTO changes VALUES(?,?,?,?,?)", (task_id, event_id, fingerprint, status, updated["revision"]))
            self._event(connection, task_id, "change_" + status, {"event_id": event_id, "actor": actor, "base_revision": base_revision, "revision": updated["revision"], "text": text})
            return {"status": status, "event_id": event_id, "task": self._task_json(updated)}

    def record_event(self, task_id, source_id, source_version, body_hash, actor, body, disposition):
        source_version = str(source_version)
        collision = False
        with self._transaction() as connection:
            self._row(connection, "tasks", task_id)
            old = connection.execute("SELECT * FROM sources WHERE task_id=? AND source_id=? AND source_version=?", (task_id, source_id, source_version)).fetchone()
            if old:
                if old["body_hash"] != body_hash or old["body"] != body or old["actor"] != actor:
                    connection.execute("UPDATE tasks SET source_pending=1,updated_at=? WHERE id=?", (time.time(), task_id))
                    self._event(connection, task_id, "source_version_conflict", {"source_id": source_id, "source_version": source_version, "body_hash": body_hash})
                    collision = True
                else:
                    row = connection.execute("SELECT * FROM events WHERE id=?", (old["event_id"],)).fetchone()
                    return {"status": "duplicate", "event": self._event_json(row)}
            else:
                payload = {"source_id": source_id, "source_version": source_version, "body_hash": body_hash, "actor": actor, "body": body, "disposition": disposition}
                event_id = self._event(connection, task_id, "source_recorded", payload)
                connection.execute("INSERT INTO sources VALUES(?,?,?,?,?,?,?)", (task_id, source_id, source_version, body_hash, body, actor, event_id))
                row = connection.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
                return {"status": "recorded", "event": self._event_json(row)}
        if collision:
            raise CoordinationError("source_version_conflict")

    def start(self, task_id, actor, expected_revision=None, expected_generation=None,
              command_key=None, launch_extra=None):
        with self._transaction() as connection:
            task = self._row(connection, "tasks", task_id)
            self._authorize(task, actor)
            if expected_revision is not None and expected_revision != task["revision"]:
                raise CoordinationError("stale_revision")
            if expected_generation is not None and expected_generation != task["generation"]:
                raise CoordinationError("stale_generation")
            if task["status"] not in ("ready", "paused") or task["active_run_id"]:
                raise CoordinationError("task_not_startable")
            if connection.execute("SELECT 1 FROM operations WHERE task_id=? AND status IN ('executing','unknown')", (task_id,)).fetchone():
                raise CoordinationError("unresolved_operations")
            if task["conflict_pending"] or task["source_pending"] or task['recovery_pending']:
                raise CoordinationError("task_pending")
            run_id = uuid.uuid4().hex
            now = time.time()
            generation = task["generation"] + 1
            connection.execute("""INSERT INTO runs
                (id,task_id,owner,adapter,revision,generation,status,text,heartbeat_at,created_at,updated_at)
                VALUES(?,?,?,?,?,?,'running',?,?,?,?)""",
                (run_id, task_id, task["owner"], task["adapter"], task["revision"], generation, task["text"], now, now, now))
            connection.execute("UPDATE tasks SET status='working',generation=?,active_run_id=?,updated_at=? WHERE id=?", (generation, run_id, now, task_id))
            self._event(connection, task_id, "run_started", {"run_id": run_id, "actor": actor, "generation": generation, "revision": task["revision"]})
            result = self._run_json(self._row(connection, "runs", run_id))
            if command_key is not None:
                spec = {**result, **(launch_extra or {})}
                connection.execute('INSERT INTO launches VALUES(?,?,?)', (run_id, command_key, _json(spec)))
            return result

    def launch(self, run_id):
        with self._reader() as connection:
            row = connection.execute('SELECT payload FROM launches WHERE run_id=?', (run_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def launch_for_command(self, key):
        with self._reader() as connection:
            row = connection.execute('SELECT run_id FROM launches WHERE command_key=?', (key,)).fetchone()
            return self._run_json(self._row(connection, 'runs', row[0])) if row else None

    def change_for_command(self, task_id, key):
        with self._reader() as connection:
            row = connection.execute('SELECT status,revision FROM changes WHERE task_id=? AND event_id=?', (task_id, key)).fetchone()
            return dict(row) if row else None

    def set_recovery_pending(self, task_id, pending):
        with self._transaction() as connection:
            self._row(connection, 'tasks', task_id)
            connection.execute('UPDATE tasks SET recovery_pending=? WHERE id=?', (bool(pending), task_id))

    def begin_recovery(self, run_id):
        with self._transaction() as connection:
            run = self._row(connection, 'runs', run_id)
            task = self._row(connection, 'tasks', run['task_id'])
            if task['active_run_id'] != run_id:
                raise CoordinationError('run_not_active')
            if run['status'] == 'running':
                connection.execute("UPDATE runs SET status='recovering' WHERE id=?", (run_id,))
                connection.execute("UPDATE tasks SET status='recovering' WHERE id=?", (task['id'],))
            elif run['status'] not in ('recovering', 'stopping', 'unknown'):
                raise CoordinationError('run_not_active')
            connection.execute("UPDATE operations SET status='unknown',reason='coordinator_lost' WHERE run_id=? AND status='executing'", (run_id,))
            self._event(connection, task['id'], 'recovery_started', {'run_id': run_id})
            return self._run_json(self._row(connection, 'runs', run_id))

    def restore_run(self, run_id, observation):
        with self._transaction() as connection:
            run = self._row(connection, 'runs', run_id)
            task = self._row(connection, 'tasks', run['task_id'])
            if task['active_run_id'] != run_id or task['generation'] != run['generation']:
                raise CoordinationError('stale_generation')
            if run['status'] != 'recovering':
                raise CoordinationError('not_recovering')
            if observation.get('status') not in ('running', 'completed', 'done', 'idle', 'stopped', 'finished'):
                raise CoordinationError('worker_state_unconfirmed')
            now = time.time()
            connection.execute("UPDATE runs SET status='running',heartbeat_at=?,updated_at=? WHERE id=?", (now, now, run_id))
            connection.execute("UPDATE tasks SET status='working',updated_at=? WHERE id=?", (now, task['id']))
            safe = {k: observation.get(k) for k in ('status','session_id','turn_id','node_id','accepted_revision') if k in observation}
            self._event(connection, task['id'], 'worker_reconnected', {'run_id': run_id, 'observation': safe})
            return self._run_json(self._row(connection, 'runs', run_id))

    def recover_operations(self):
        with self._transaction() as connection:
            connection.execute("UPDATE operations SET status='unknown',reason='coordinator_restarted' WHERE status='executing'")

    def stage_result(self, run_id, result, evidence):
        """Durable output awaiting review; this is not a verified task completion."""
        with self._transaction() as connection:
            task, run = self._current_run(connection, run_id, require_revision=False)
            if run['status'] == 'result_ready':
                if run['result'] != _json(result):
                    raise CoordinationError('result_changed_after_completion')
                return self._run_json(run)
            connection.execute("UPDATE runs SET status='result_ready',result=?,evidence=?,updated_at=? WHERE id=?",
                               (_json(result), _json(evidence), time.time(), run_id))
            connection.execute("UPDATE tasks SET status='result_ready',updated_at=? WHERE id=?", (time.time(), task['id']))
            self._event(connection, task['id'], 'result_received', {'run_id': run_id, 'reviewed': False})
            return self._run_json(self._row(connection, 'runs', run_id))

    def set_handle(self, run_id, handle):
        with self._transaction() as connection:
            run = self._row(connection, 'runs', run_id)
            task = self._row(connection, 'tasks', run['task_id'])
            if task['active_run_id'] != run_id or task['generation'] != run['generation'] or run['status'] not in ('running', 'recovering'):
                raise CoordinationError('stale_generation')
            encoded = _json(handle)
            if run["handle"] is not None and run["handle"] != encoded:
                raise CoordinationError("handle_already_set")
            connection.execute("UPDATE runs SET handle=?,updated_at=? WHERE id=?", (encoded, time.time(), run_id))
            return self._run_json(self._row(connection, "runs", run_id))

    def acknowledge(self, run_id, revision):
        with self._transaction() as connection:
            task, run = self._current_run(connection, run_id, require_revision=False)
            if revision != task["revision"]:
                raise CoordinationError("stale_revision")
            if run["revision"] != revision:
                connection.execute("UPDATE runs SET revision=?,text=?,updated_at=? WHERE id=?", (revision, task["text"], time.time(), run_id))
                self._event(connection, task["id"], "revision_acknowledged", {"run_id": run_id, "revision": revision})
            return self._run_json(self._row(connection, "runs", run_id))

    def stop(self, task_id, actor, expected_run_id=None):
        with self._transaction() as connection:
            task = self._row(connection, "tasks", task_id)
            self._authorize(task, actor)
            if expected_run_id is not None and expected_run_id != task["active_run_id"]:
                raise CoordinationError("stale_stop_target")
            if task["status"] == "stopping":
                return self._task_json(task)
            now = time.time()
            status = "stopping" if task["active_run_id"] else "paused"
            connection.execute("UPDATE tasks SET status=?,generation=generation+1,updated_at=? WHERE id=?", (status, now, task_id))
            if task["active_run_id"]:
                connection.execute("UPDATE runs SET status='stopping',updated_at=? WHERE id=?", (now, task["active_run_id"]))
            self._event(connection, task_id, "stop_requested", {"actor": actor, "run_id": task["active_run_id"]})
            return self._task_json(self._row(connection, "tasks", task_id))

    def confirm_stopped(self, run_id, descendants_stopped):
        if type(descendants_stopped) is not bool:
            raise CoordinationError("invalid_stop_observation")
        with self._transaction() as connection:
            run = self._row(connection, "runs", run_id)
            task = self._row(connection, "tasks", run["task_id"])
            if run["status"] == "stopped":
                return {"confirmed": True, "run": self._run_json(run), "task": self._task_json(task)}
            if task["active_run_id"] != run_id or run["status"] not in ("stopping", "unknown"):
                raise CoordinationError("stop_not_requested")
            if descendants_stopped:
                now = time.time()
                connection.execute("UPDATE runs SET status='stopped',updated_at=? WHERE id=?", (now, run_id))
                connection.execute("UPDATE tasks SET status='paused',active_run_id=NULL,updated_at=? WHERE id=?", (now, task["id"]))
                connection.execute("UPDATE operations SET status='rejected',reason='run_stopped',updated_at=? WHERE run_id=? AND status='reserved'", (now, run_id))
                # A request sent before stop may still have an external effect.
                connection.execute("UPDATE operations SET status='unknown',reason='run_stopped_during_operation',updated_at=? WHERE run_id=? AND status='executing'", (now, run_id))
                if not connection.execute("SELECT 1 FROM operations WHERE run_id=? AND status='unknown'", (run_id,)).fetchone():
                    connection.execute("DELETE FROM resources WHERE run_id=?", (run_id,))
                self._event(connection, task["id"], "run_stopped", {"run_id": run_id})
            return {"confirmed": descendants_stopped, "run": self._run_json(self._row(connection, "runs", run_id)), "task": self._task_json(self._row(connection, "tasks", task["id"]))}

    def get_snapshot(self, run_id):
        with self._reader() as connection:
            run = self._row(connection, "runs", run_id)
            task = self._row(connection, "tasks", run["task_id"])
            return {"task": self._task_json(task), "run": self._run_json(run), "revision": task["revision"], "text": task["text"], "stale": run["revision"] != task["revision"] or run["generation"] != task["generation"] or task["active_run_id"] != run_id}

    def heartbeat(self, run_id):
        with self._transaction() as connection:
            self._current_run(connection, run_id, require_revision=False)
            now = time.time()
            connection.execute("UPDATE runs SET heartbeat_at=?,updated_at=? WHERE id=?", (now, now, run_id))
            return self._run_json(self._row(connection, "runs", run_id))

    def expire(self, run_id):
        with self._transaction() as connection:
            run = self._row(connection, "runs", run_id)
            task = self._row(connection, "tasks", run["task_id"])
            if run["status"] == "unknown":
                return self._run_json(run)
            if task["active_run_id"] != run_id or run["status"] not in ("running", "stopping"):
                raise CoordinationError("run_not_active")
            now = time.time()
            connection.execute("UPDATE tasks SET status='unknown',generation=generation+1,updated_at=? WHERE id=?", (now, task["id"]))
            connection.execute("UPDATE runs SET status='unknown',updated_at=? WHERE id=?", (now, run_id))
            connection.execute("UPDATE operations SET status='unknown',reason='run_expired',updated_at=? WHERE run_id=? AND status='executing'", (now, run_id))
            self._event(connection, task["id"], "run_unknown", {"run_id": run_id})
            return self._run_json(self._row(connection, "runs", run_id))

    @staticmethod
    def _environment_generation(connection, name):
        row = connection.execute("SELECT generation FROM environments WHERE name=?", (name,)).fetchone()
        return row[0] if row else 1

    def _check_resource(self, connection, run_id, resource, environment):
        if resource is None:
            if environment is not None:
                raise CoordinationError("environment_requires_resource")
            return
        row = connection.execute("SELECT run_id FROM resources WHERE name=?", (resource,)).fetchone()
        if row is None or row[0] != run_id:
            raise CoordinationError("resource_not_owned")
        if environment != self._environment_generation(connection, resource):
            raise CoordinationError("stale_environment")

    def reserve_operation(self, run_id, operation_id, payload, resource=None, environment=None):
        if not isinstance(operation_id, str) or not operation_id:
            raise CoordinationError("invalid_operation_id")
        payload_hash = _hash(payload)
        with self._transaction() as connection:
            old = connection.execute("SELECT * FROM operations WHERE id=?", (operation_id,)).fetchone()
            if old:
                if old["run_id"] != run_id or old["payload_hash"] != payload_hash or old["resource"] != resource or (environment is not None and old["environment"] != environment):
                    raise CoordinationError("operation_id_collision")
                return self._operation_json(old)
            task, run = self._current_run(connection, run_id, publish=True)
            if connection.execute("SELECT 1 FROM operations WHERE task_id=? AND status='unknown'", (task["id"],)).fetchone():
                raise CoordinationError("unresolved_operations")
            if resource is not None and environment is None:
                environment = self._environment_generation(connection, resource)
            self._check_resource(connection, run_id, resource, environment)
            now = time.time()
            connection.execute("""INSERT INTO operations
                (id,run_id,task_id,payload,payload_hash,revision,generation,resource,environment,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,'reserved',?,?)""",
                (operation_id, run_id, task["id"], _json(payload), payload_hash, run["revision"], run["generation"], resource, environment, now, now))
            self._event(connection, task["id"], "operation_reserved", {"operation_id": operation_id, "run_id": run_id, "revision": run["revision"]})
            return self._operation_json(self._row(connection, "operations", operation_id))

    def begin_operation(self, operation_id):
        rejection = None
        with self._transaction() as connection:
            operation = self._row(connection, "operations", operation_id)
            if operation["status"] != "reserved":
                return self._operation_json(operation)
            try:
                task, run = self._current_run(connection, operation["run_id"], publish=True)
                if connection.execute("SELECT 1 FROM operations WHERE task_id=? AND status='unknown'", (task["id"],)).fetchone():
                    raise CoordinationError("unresolved_operations")
                if operation["revision"] != run["revision"]:
                    raise CoordinationError("stale_revision")
                if operation["generation"] != run["generation"]:
                    raise CoordinationError("stale_generation")
                self._check_resource(connection, run["id"], operation["resource"], operation["environment"])
            except CoordinationError as error:
                rejection = error
                connection.execute("UPDATE operations SET status='rejected',reason=?,updated_at=? WHERE id=?", (error.code, time.time(), operation_id))
                self._event(connection, operation["task_id"], "operation_rejected", {"operation_id": operation_id, "reason": error.code})
            else:
                connection.execute("UPDATE operations SET status='executing',updated_at=? WHERE id=?", (time.time(), operation_id))
                self._event(connection, operation["task_id"], "operation_dispatched", {"operation_id": operation_id})
                return self._operation_json(self._row(connection, "operations", operation_id), dispatch=True)
        if rejection:
            raise rejection

    def finish_operation(self, operation_id, success, receipt=None):
        if success is not None and type(success) is not bool:
            raise CoordinationError("invalid_operation_result")
        status = "unknown" if success is None else "succeeded" if success else "failed"
        with self._transaction() as connection:
            operation = self._row(connection, "operations", operation_id)
            if operation["status"] == status:
                if operation["receipt"] != _json(receipt):
                    raise CoordinationError("receipt_collision")
                return self._operation_json(operation)
            if operation["status"] != "executing":
                raise CoordinationError("operation_not_executing")
            connection.execute("UPDATE operations SET status=?,receipt=?,updated_at=? WHERE id=?", (status, _json(receipt), time.time(), operation_id))
            self._event(connection, operation["task_id"], "operation_" + status, {"operation_id": operation_id, "receipt": receipt})
            return self._operation_json(self._row(connection, "operations", operation_id))

    def reconcile_operation(self, operation_id, success, receipt):
        if type(success) is not bool or receipt is None:
            raise CoordinationError("reconciliation_evidence_required")
        with self._transaction() as connection:
            operation = self._row(connection, "operations", operation_id)
            status = "succeeded" if success else "failed"
            if operation["status"] == status and operation["receipt"] == _json(receipt):
                return self._operation_json(operation)
            if operation["status"] != "unknown":
                raise CoordinationError("operation_not_unknown")
            connection.execute("UPDATE operations SET status=?,receipt=?,updated_at=? WHERE id=?", (status, _json(receipt), time.time(), operation_id))
            self._event(connection, operation["task_id"], "operation_reconciled", {"operation_id": operation_id, "success": success, "receipt": receipt})
            run = self._row(connection, "runs", operation["run_id"])
            if run["status"] == "stopped" and not connection.execute("SELECT 1 FROM operations WHERE run_id=? AND status IN ('executing','unknown')", (run["id"],)).fetchone():
                connection.execute("DELETE FROM resources WHERE run_id=?", (run["id"],))
            return self._operation_json(self._row(connection, "operations", operation_id))

    def operation(self, operation_id):
        with self._reader() as connection:
            return self._operation_json(self._row(connection, "operations", operation_id))

    def finish(self, run_id, result, evidence):
        with self._transaction() as connection:
            task, run = self._current_run(connection, run_id, publish=True)
            if connection.execute("SELECT 1 FROM operations WHERE run_id=? AND status IN ('reserved','executing','unknown')", (run_id,)).fetchone():
                raise CoordinationError("unresolved_operations")
            now = time.time()
            connection.execute("UPDATE runs SET status='done',result=?,evidence=?,updated_at=? WHERE id=?", (_json(result), _json(evidence), now, run_id))
            connection.execute("UPDATE tasks SET status='done',active_run_id=NULL,updated_at=? WHERE id=?", (now, task["id"]))
            connection.execute("DELETE FROM resources WHERE run_id=?", (run_id,))
            self._event(connection, task["id"], "run_finished", {"run_id": run_id, "revision": run["revision"], "result": result, "evidence": evidence})
            return self._run_json(self._row(connection, "runs", run_id))

    def acquire_resource(self, name, run_id):
        if not isinstance(name, str) or not name:
            raise CoordinationError("invalid_resource")
        with self._transaction() as connection:
            self._current_run(connection, run_id, require_revision=False)
            old = connection.execute("SELECT run_id FROM resources WHERE name=?", (name,)).fetchone()
            if old and old[0] != run_id:
                raise CoordinationError("resource_busy")
            connection.execute("INSERT OR IGNORE INTO resources VALUES(?,?)", (name, run_id))
            return {"name": name, "run_id": run_id, "environment": self._environment_generation(connection, name)}

    def release_resource(self, name, run_id):
        with self._transaction() as connection:
            run = self._row(connection, "runs", run_id)
            if run["status"] not in ("running", "done", "stopped"):
                raise CoordinationError("stop_not_confirmed")
            old = connection.execute("SELECT run_id FROM resources WHERE name=?", (name,)).fetchone()
            if old and old[0] != run_id:
                raise CoordinationError("resource_not_owned")
            if connection.execute("SELECT 1 FROM operations WHERE run_id=? AND resource=? AND status IN ('executing','unknown')", (run_id, name)).fetchone():
                raise CoordinationError("unresolved_operations")
            connection.execute("DELETE FROM resources WHERE name=? AND run_id=?", (name, run_id))
            return {"name": name, "run_id": run_id, "released": True}

    def bump_environment(self, name):
        with self._transaction() as connection:
            value = self._environment_generation(connection, name) + 1
            connection.execute("INSERT INTO environments VALUES(?,?) ON CONFLICT(name) DO UPDATE SET generation=excluded.generation", (name, value))
            return {"name": name, "generation": value}

    def environment(self, name):
        with self._reader() as connection:
            return {"name": name, "generation": self._environment_generation(connection, name)}

    def set_source_pending(self, task_id, pending):
        if type(pending) is not bool:
            raise CoordinationError("invalid_source_pending")
        with self._transaction() as connection:
            self._row(connection, "tasks", task_id)
            connection.execute("UPDATE tasks SET source_pending=?,updated_at=? WHERE id=?", (int(pending), time.time(), task_id))
            self._event(connection, task_id, "source_pending_changed", {"pending": pending})
            return self._task_json(self._row(connection, "tasks", task_id))
