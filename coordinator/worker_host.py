"""Persistent per-machine worker authority, separate from coordinator lifetime."""
import argparse
from contextlib import contextmanager
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import signal
import sqlite3
import threading
import time
import uuid

from .adapters import AdapterError, DirectAdapter, RoundtableAdapter, snapshot_payload
from .instance import InstanceLock
from .remote import canonical_json, handle_for, launch_spec, verify_handle, _loopback


class WorkerHost:
    def __init__(self, state_dir, node_id, token, engines, allowed_owners):
        self.state_dir = Path(state_dir).resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if not node_id or not token or not engines or not allowed_owners:
            raise ValueError('worker_configuration_missing')
        self.node_id, self.token = node_id, token
        self.engines = engines
        self.allowed_owners = frozenset(allowed_owners)
        self.instance_id = uuid.uuid4().hex
        self.path = self.state_dir / 'worker.sqlite'
        self.instance_lock = InstanceLock(self.state_dir / '.worker.lock')
        self._locks, self._lock_guard = {}, threading.Lock()
        self.closed = False
        try:
            with self.db(True) as db:
                db.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
                identity = {'node_id': node_id, 'token_hash': hashlib.sha256(token.encode()).hexdigest()}
                for key, value in identity.items():
                    row = db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
                    if row and row['value'] != value:
                        raise ValueError('worker_persistent_identity_mismatch')
                    db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', (key, value))
                db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', ('ledger_id', uuid.uuid4().hex))
                self.ledger_id = db.execute("SELECT value FROM meta WHERE key='ledger_id'").fetchone()['value']
                db.execute('''CREATE TABLE IF NOT EXISTS jobs (
                    run_id TEXT PRIMARY KEY, engine TEXT NOT NULL, handle TEXT NOT NULL,
                    launch TEXT NOT NULL, instance_id TEXT NOT NULL, status TEXT NOT NULL,
                    engine_handle TEXT, observation TEXT, updated REAL NOT NULL)''')
                db.execute('''CREATE TABLE IF NOT EXISTS commands (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, action TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, state TEXT NOT NULL, result TEXT, updated REAL NOT NULL)''')
                db.execute('CREATE TABLE IF NOT EXISTS delivery_heads (run_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, text_hash TEXT NOT NULL)')
                db.execute("UPDATE commands SET state='unknown' WHERE state='processing'")
                db.execute("UPDATE jobs SET status='unknown' WHERE status NOT IN ('stopped','completed')")
        except BaseException:
            self.instance_lock.close()
            raise

    @contextmanager
    def db(self, write=False):
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    def lock(self, run_id):
        with self._lock_guard:
            return self._locks.setdefault(run_id, threading.RLock())

    def info(self):
        return {'node_id': self.node_id, 'instance_id': self.instance_id, 'ledger_id': self.ledger_id,
                'engines': sorted(self.engines), 'protocol': 1}

    def _job(self, handle):
        verify_handle(handle, self.node_id, self.token, ledger_id=self.ledger_id)
        if handle['engine'] not in self.engines:
            raise AdapterError('worker_engine_not_allowed')
        with self.db() as db:
            row = db.execute('SELECT * FROM jobs WHERE run_id=?', (handle['run_id'],)).fetchone()
        if not row:
            raise AdapterError('remote_run_not_found')
        row = dict(row)
        if row['handle'] != canonical_json(handle):
            raise AdapterError('worker_run_binding_conflict')
        for name in ('handle', 'launch', 'engine_handle', 'observation'):
            row[name] = json.loads(row[name]) if row[name] is not None else None
        return row

    def _begin(self, operation_id, handle, action, payload, retry_stop=False):
        if not isinstance(operation_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,256}', operation_id):
            raise AdapterError('invalid_worker_operation_id')
        fingerprint = hashlib.sha256(canonical_json(payload).encode()).hexdigest()
        with self.db(True) as db:
            row = db.execute('SELECT * FROM commands WHERE id=?', (operation_id,)).fetchone()
            if row:
                if (row['run_id'] != handle['run_id'] or row['action'] != action
                        or row['fingerprint'] != fingerprint):
                    raise AdapterError('worker_operation_collision')
                if row['state'] == 'done':
                    return json.loads(row['result'])
                if row['state'] == 'rejected':
                    raise AdapterError(json.loads(row['result'])['error'])
                if row['state'] == 'unknown' and action == 'stop' and retry_stop:
                    db.execute("UPDATE commands SET state='processing',result=NULL,updated=? WHERE id=?", (time.time(), operation_id))
                    return None
                raise AdapterError('worker_command_' + row['state'])
            db.execute('INSERT INTO commands VALUES(?,?,?,?,?,?,?)',
                       (operation_id, handle['run_id'], action, fingerprint, 'processing', None, time.time()))
        return None

    def _receipt(self, operation_id, state, result):
        with self.db(True) as db:
            db.execute('UPDATE commands SET state=?,result=?,updated=? WHERE id=?',
                       (state, canonical_json(result), time.time(), operation_id))

    def _envelope(self, handle, observation=None, receipt=None):
        result = {'node_id': self.node_id, 'instance_id': self.instance_id, 'ledger_id': self.ledger_id, 'handle': handle}
        if observation is not None:
            result['observation'] = observation
        if receipt is not None:
            result['receipt'] = receipt
        return result

    def _persist_observation(self, job, observation):
        observation = dict(observation)
        if 'accepted_revision' not in observation and isinstance(observation.get('revision'), int):
            observation['accepted_revision'] = observation['revision']
        status = observation.get('status')
        if observation.get('stopped') is True:
            terminal = 'completed' if status in ('completed', 'done', 'succeeded') else 'stopped'
            if status not in ('failed', 'error', 'cancelled'):
                observation['status'] = terminal
        else:
            terminal = 'running'
            observation.setdefault('status', 'running')
        with self.db(True) as db:
            db.execute('UPDATE jobs SET observation=?,status=?,updated=? WHERE run_id=?',
                       (canonical_json(observation), terminal, time.time(), job['run_id']))
        return observation

    def _observe(self, handle):
        job = self._job(handle)
        if job['status'] in ('stopped', 'completed'):
            return job['observation']
        if job['instance_id'] != self.instance_id or job['status'] in ('unknown', 'launching'):
            return {'stopped': False, 'status': 'unknown' if job['status'] != 'launching' else 'starting',
                    'reason': 'worker_ownership_unconfirmed', 'last_observation': job['observation']}
        try:
            observation = self.engines[job['engine']].observe(job['engine_handle'])
        except Exception as exc:
            return {'stopped': False, 'status': 'unknown', 'reason': getattr(exc, 'code', type(exc).__name__)}
        return self._persist_observation(job, observation)

    def observe(self, handle):
        with self.lock(handle.get('run_id')):
            return self._envelope(handle, self._observe(handle))

    def start(self, handle, run, operation_id):
        spec = launch_spec(run)
        verify_handle(handle, self.node_id, self.token, ledger_id=self.ledger_id)
        if spec['owner'] not in self.allowed_owners:
            raise AdapterError('worker_owner_not_allowed')
        if handle['engine'] not in self.engines:
            raise AdapterError('worker_engine_not_allowed')
        if handle != handle_for(spec, self.node_id, self.token, handle['engine'], self.ledger_id):
            raise AdapterError('worker_launch_binding_mismatch')
        with self.lock(handle['run_id']):
            cached = self._begin(operation_id, handle, 'start', {'handle': handle, 'run': spec})
            if cached is not None:
                # The durable start receipt proves identity, not current liveness.
                observation = self._observe(handle)
                if observation.get('status') in ('unknown', 'starting'):
                    raise AdapterError('worker_launch_unknown')
                return self._envelope(handle, observation, {'replayed': True})
            try:
                with self.db() as db:
                    existing = db.execute('SELECT handle FROM jobs WHERE run_id=?', (handle['run_id'],)).fetchone()
                if existing:
                    job = self._job(handle)
                    if job['status'] in ('unknown', 'launching'):
                        raise AdapterError('worker_launch_unknown')
                    result = self._envelope(handle, self._observe(handle), {'replayed': True})
                    self._receipt(operation_id, 'done', result)
                    return result
                engine_run = dict(spec)
                if spec.get('resume_from'):
                    with self.db() as db:
                        prior = db.execute('SELECT * FROM jobs WHERE run_id=?', (spec['resume_from'],)).fetchone()
                    if not prior:
                        raise AdapterError('resume_source_not_found')
                    prior_launch = json.loads(prior['launch'])
                    if (prior['engine'] != handle['engine'] or prior_launch['task_id'] != spec['task_id']
                            or prior_launch['owner'] != spec['owner']):
                        raise AdapterError('resume_source_mismatch')
                    if prior['status'] not in ('stopped', 'completed'):
                        raise AdapterError('resume_source_not_stopped')
                    prior_observation = json.loads(prior['observation'] or '{}')
                    if not prior_observation.get('stopped'):
                        raise AdapterError('resume_source_stop_unconfirmed')
                    engine_run['resume_session'] = json.loads(prior['engine_handle'])
                with self.db(True) as db:
                    active = db.execute("SELECT launch FROM jobs WHERE status NOT IN ('stopped','completed')").fetchall()
                    if any(json.loads(row['launch'])['task_id'] == spec['task_id'] for row in active):
                        raise AdapterError('worker_task_already_active')
                    db.execute('INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)',
                               (spec['id'], handle['engine'], canonical_json(handle), canonical_json(spec),
                                self.instance_id, 'launching', None, None, time.time()))
                    db.execute('INSERT INTO delivery_heads VALUES(?,?,?)',
                               (spec['id'], spec['revision'], hashlib.sha256(spec['text'].encode()).hexdigest()))
                try:
                    engine_handle = self.engines[handle['engine']].start(engine_run)
                except Exception as exc:
                    with self.db(True) as db:
                        db.execute("UPDATE jobs SET status='unknown',engine_handle=?,updated=? WHERE run_id=?",
                                   (canonical_json(exc.handle) if getattr(exc, 'handle', None) else None,
                                    time.time(), spec['id']))
                    self._receipt(operation_id, 'unknown', {'error': 'worker_launch_unconfirmed'})
                    raise AdapterError('worker_launch_unconfirmed') from exc
                with self.db(True) as db:
                    db.execute("UPDATE jobs SET status='running',engine_handle=?,updated=? WHERE run_id=?",
                               (canonical_json(engine_handle), time.time(), spec['id']))
                result = self._envelope(handle, self._observe(handle), {'started': True})
                self._receipt(operation_id, 'done', result)
                return result
            except AdapterError as exc:
                with self.db() as db:
                    state = db.execute('SELECT state FROM commands WHERE id=?', (operation_id,)).fetchone()
                if state['state'] == 'processing':
                    self._receipt(operation_id, 'rejected', {'error': exc.code})
                raise
            except Exception:
                self._receipt(operation_id, 'unknown', {'error': 'worker_launch_unconfirmed'})
                raise

    def mutate(self, action, handle, operation_id, snapshot=None):
        if action not in ('deliver', 'stop'):
            raise AdapterError('unknown_worker_action')
        with self.lock(handle.get('run_id')):
            job = self._job(handle)
            if action == 'deliver':
                snapshot = snapshot_payload(snapshot)
                if (snapshot.get('task_id') is not None and snapshot['task_id'] != job['launch']['task_id']
                        or snapshot.get('generation') is not None and snapshot['generation'] != job['launch']['generation']):
                    raise AdapterError('worker_snapshot_identity_mismatch')
            payload = {'handle': handle, 'snapshot': snapshot} if action == 'deliver' else {'handle': handle}
            cached = self._begin(operation_id, handle, action, payload,
                                 retry_stop=action == 'stop' and job['instance_id'] == self.instance_id)
            if cached is not None:
                if action == 'stop':
                    return self._envelope(handle, self._observe(handle), {'replayed': True})
                return {**cached, 'instance_id': self.instance_id}
            dispatched = False
            try:
                if job['status'] in ('stopped', 'completed'):
                    if action == 'deliver':
                        raise AdapterError('worker_run_terminal')
                    result = self._envelope(handle, job['observation'], {'already_stopped': True})
                else:
                    if job['instance_id'] != self.instance_id or not job['engine_handle']:
                        raise AdapterError('worker_ownership_unconfirmed')
                    if action == 'deliver' and job['status'] != 'running':
                        raise AdapterError('worker_run_not_running')
                    if action == 'deliver':
                        with self.db() as db:
                            head = db.execute('SELECT * FROM delivery_heads WHERE run_id=?', (handle['run_id'],)).fetchone()
                            uncertain = db.execute("SELECT 1 FROM commands WHERE run_id=? AND action='deliver' AND state='unknown'", (handle['run_id'],)).fetchone()
                        if uncertain:
                            raise AdapterError('worker_delivery_unconfirmed')
                        text_hash = hashlib.sha256(snapshot['text'].encode()).hexdigest()
                        if head and snapshot['revision'] < head['revision']:
                            raise AdapterError('stale_worker_revision')
                        if head and snapshot['revision'] == head['revision']:
                            if text_hash != head['text_hash']:
                                raise AdapterError('worker_revision_conflict')
                            result = self._envelope(handle, self._observe(handle),
                                                    {'delivered': True, 'revision': snapshot['revision'],
                                                     'replayed': True, 'acknowledgement': 'transport_only'})
                            self._receipt(operation_id, 'done', result)
                            return result
                    engine = self.engines[job['engine']]
                    dispatched = True
                    if action == 'deliver':
                        receipt = engine.deliver(job['engine_handle'], snapshot)
                        with self.db(True) as db:
                            db.execute('INSERT INTO delivery_heads VALUES(?,?,?) ON CONFLICT(run_id) DO UPDATE SET revision=excluded.revision,text_hash=excluded.text_hash',
                                       (handle['run_id'], snapshot['revision'], text_hash))
                    else:
                        receipt = engine.stop(job['engine_handle'])
                    observation = engine.observe(job['engine_handle'])
                    observation = self._persist_observation(job, observation)
                    result = self._envelope(handle, observation, receipt)
                self._receipt(operation_id, 'done', result)
                return result
            except AdapterError as exc:
                self._receipt(operation_id, 'unknown' if dispatched else 'rejected', {'error': exc.code})
                raise
            except Exception:
                self._receipt(operation_id, 'unknown', {'error': 'worker_operation_unconfirmed'})
                raise

    def close(self, stop_engines=True):
        if self.closed:
            return
        self.closed = True
        try:
            if stop_engines:
                for engine in self.engines.values():
                    engine.close()
        finally:
            self.instance_lock.close()


def make_worker_server(host, address):
    if not _loopback(address[0]):
        raise ValueError('worker_bind_loopback_only')

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, body):
            raw = canonical_json(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            self.dispatch('GET')

        def do_POST(self):
            self.dispatch('POST')

        def dispatch(self, method):
            try:
                if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + host.token):
                    raise AdapterError('worker_unauthenticated')
                if self.headers.get('X-Worker-Node') != host.node_id:
                    raise AdapterError('worker_node_mismatch')
                if method == 'GET' and self.path == '/v1/info':
                    return self.reply(200, host.info())
                length = int(self.headers.get('Content-Length', '0'))
                if method != 'POST' or not 0 <= length <= 1_000_000:
                    raise AdapterError('invalid_worker_request')
                body = json.loads(self.rfile.read(length))
                if self.path == '/v1/start':
                    result = host.start(body['handle'], body['run'], body['operation_id'])
                elif self.path == '/v1/observe':
                    result = host.observe(body['handle'])
                elif self.path in ('/v1/stop', '/v1/deliver'):
                    result = host.mutate(self.path.rsplit('/', 1)[-1], body['handle'], body['operation_id'], body.get('snapshot'))
                else:
                    raise AdapterError('unknown_worker_endpoint')
                self.reply(200, result)
            except AdapterError as exc:
                status = 401 if exc.code == 'worker_unauthenticated' else 403 if exc.code in ('worker_node_mismatch', 'worker_owner_not_allowed', 'remote_handle_proof_invalid') else 404 if exc.code == 'remote_run_not_found' else 409
                self.reply(status, {'node_id': host.node_id, 'instance_id': host.instance_id, 'ledger_id': host.ledger_id, 'error': exc.code})
            except (ValueError, TypeError, KeyError):
                self.reply(400, {'node_id': host.node_id, 'instance_id': host.instance_id, 'ledger_id': host.ledger_id, 'error': 'invalid_worker_request'})
            except Exception:
                self.reply(503, {'node_id': host.node_id, 'instance_id': host.instance_id, 'ledger_id': host.ledger_id, 'error': 'worker_operation_unconfirmed'})

    return ThreadingHTTPServer(address, Handler)


def configured_engines(config, state_dir):
    engines = {}
    for name, entry in config.items():
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', name):
            raise ValueError('invalid_engine_name')
        kind = entry['kind']
        root = Path(state_dir) / 'engines' / name
        if kind == 'fixture':
            engines[name] = DirectAdapter(root)
        elif kind == 'rt':
            engines[name] = RoundtableAdapter(entry['url'], entry['participants'], allowed_hosts=entry['allowed_hosts'])
        elif kind in ('claude', 'codex'):
            from .native_cli import NativeEngine
            engines[name] = NativeEngine(kind, entry['executable'], root, entry['provider_config'])
        else:
            raise ValueError('unknown_engine_kind')
    return engines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding='utf-8'))
    engines = configured_engines(config['engines'], config['state_dir'])
    host = WorkerHost(config['state_dir'], config['node_id'], config['token'], engines, config['allowed_owners'])
    server = make_worker_server(host, (config.get('listen', '127.0.0.1'), int(config['port'])))
    def shutdown(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        host.close()


if __name__ == '__main__':
    main()
