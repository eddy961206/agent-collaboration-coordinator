"""Authenticated lab server. No production defaults, automatic model launch or polling."""
import argparse
import hashlib
import hmac
import json
import re
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .adapters import DirectAdapter, RoundtableAdapter
from .runtime import Runtime
from .store import CoordinationError
from .transport import GiteaClient


def make_server(runtime, address, tokens, webhook_secret):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, code, value):
            raw = json.dumps(value, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(raw)

        def authenticate(self):
            auth = self.headers.get('Authorization', '')
            if not auth.startswith('Bearer '):
                raise CoordinationError('unauthenticated')
            token = auth[7:]
            actor = next((name for name, expected in tokens.items() if hmac.compare_digest(token, expected)), None)
            return actor, token

        def read_body(self):
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 <= length <= 1_000_000:
                raise ValueError('body_too_large')
            return self.rfile.read(length)

        def do_GET(self):
            self.route('GET')

        def do_POST(self):
            self.route('POST')

        def route(self, method):
            try:
                path = urlsplit(self.path).path
                if path == '/health' and method == 'GET':
                    return self.reply(200, {'status': 'ok', 'mode': 'coordinator', 'model_execution': 'worker-configured', 'recovery_errors': runtime.recovery.errors})
                if path == '/webhooks/gitea' and method == 'POST':
                    raw = self.read_body()
                    expected = hmac.new(webhook_secret.encode(), raw, hashlib.sha256).hexdigest()
                    if not hmac.compare_digest(self.headers.get('X-Gitea-Signature', ''), expected):
                        raise CoordinationError('invalid_webhook_signature')
                    if self.headers.get('X-Gitea-Event') != 'issue_comment':
                        return self.reply(200, {'ignored': 'event_type'})
                    return self.reply(200, runtime.webhook(json.loads(raw)))
                actor, token = self.authenticate()
                parts = [unquote(p) for p in path.strip('/').split('/')]
                data = json.loads(self.read_body() or b'{}') if method == 'POST' else {}
                if parts[:2] == ['api', 'runs'] and len(parts) >= 3:
                    rid = parts[2]
                    run = runtime.store.run(rid)
                    is_worker = hmac.compare_digest(token, runtime.worker_token(rid))
                    if method == 'GET' and len(parts) == 3 and actor:
                        return self.reply(200, run)
                    if not is_worker:
                        raise CoordinationError('worker_token_required')
                    if len(parts) != 4 or method != 'POST':
                        raise CoordinationError('unknown_endpoint')
                    if parts[3] == 'ack':
                        return self.reply(200, runtime.acknowledge(rid, int(data['revision'])))
                    if parts[3] == 'publish':
                        op = data['operation_id']
                        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', op):
                            raise ValueError('invalid_operation_id')
                        return self.reply(200, runtime.publish(rid, op, data['body']))
                    if parts[3] == 'finish':
                        observation = runtime.observe(rid)['observation']
                        if not observation.get('stopped'):
                            raise CoordinationError('children_still_running')
                        result = runtime.store.finish(rid, data['result'], data['evidence'])
                        runtime.queue_status(result['task_id'])
                        return self.reply(200, result)
                    raise CoordinationError('unknown_endpoint')
                if not actor:
                    raise CoordinationError('unauthenticated')
                if parts == ['api', 'tasks']:
                    if method == 'GET':
                        return self.reply(200, runtime.store.tasks())
                    runtime.actor(actor)
                    owner = data.get('owner', actor)
                    decision = data.get('decision_owner', actor)
                    runtime.actor(owner)
                    runtime.actor(decision)
                    if owner != actor and runtime.actors[actor]['role'] != 'admin':
                        raise CoordinationError('forbidden')
                    if data['adapter'] not in runtime.adapters:
                        raise ValueError('unknown_adapter')
                    selected = runtime.adapters[data['adapter']]
                    if getattr(selected, 'durable_remote', False) and owner not in selected.allowed_owners:
                        raise CoordinationError('worker_owner_not_allowed')
                    runtime.gitea.issue(data['repo'], int(data['issue']))
                    tid = f"{data['repo']}#{int(data['issue'])}"
                    result = runtime.store.create_task(tid, data['repo'], int(data['issue']), owner, data['adapter'], data['text'], decision)
                    runtime.queue_status(tid)
                    return self.reply(201, result)
                if parts[:2] == ['api', 'tasks'] and len(parts) >= 3:
                    tid = parts[2]
                    if method == 'GET' and len(parts) == 3:
                        return self.reply(200, {'task': runtime.store.task(tid), 'runs': runtime.store.runs(tid), 'events': runtime.store.events(tid), 'status_publish_error': runtime.status_errors.get(tid), 'recovery_error': runtime.recovery.errors.get(tid)})
                    runtime.task_permission(tid, actor)
                    if len(parts) == 4 and parts[3] == 'commands' and method == 'POST':
                        key = self.headers.get('Idempotency-Key', '')
                        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', key):
                            raise ValueError('idempotency_key_required')
                        return self.reply(200, runtime.command('api:' + key, tid, actor, data['action'], data.get('data', {})))
                    if len(parts) == 4 and parts[3] == 'status' and method == 'POST':
                        return self.reply(200, runtime.sync_status(tid))
                if parts[:2] == ['api', 'operations'] and len(parts) == 4 and parts[3] == 'reconcile' and method == 'POST':
                    operation = runtime.store.operation(parts[2])
                    run = runtime.store.run(operation['run_id'])
                    runtime.task_permission(run['task_id'], actor)
                    return self.reply(200, runtime.reconcile_publish(parts[2]))
                raise CoordinationError('unknown_endpoint')
            except CoordinationError as exc:
                code = 403 if exc.code in ('forbidden', 'unknown_source_author', 'repository_not_allowed', 'worker_owner_not_allowed') else 401 if exc.code in ('unauthenticated', 'invalid_webhook_signature', 'worker_token_required') else 409
                self.reply(code, {'error': exc.code})
            except (ValueError, TypeError, KeyError) as exc:
                self.reply(400, {'error': str(exc)[:160]})
            except Exception as exc:
                # Endpoint/credential-bearing exception strings do not enter responses.
                self.reply(503, {'error': type(exc).__name__, 'detail': 'operation_unconfirmed_inspect_local_state'})

    return ThreadingHTTPServer(address, Handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding='utf-8'))
    if config.get('mode') not in ('isolated-lab', 'distributed'):
        raise SystemExit('Explicit isolated-lab or distributed configuration required.')
    actors = config['actors']
    adapters = {'direct': DirectAdapter(Path(config['state_dir']) / 'workspaces')}
    if config.get('roundtable'):
        rt = config['roundtable']
        adapters['rt'] = RoundtableAdapter(rt['url'], rt['participants'], allowed_hosts=rt['allowed_hosts'])
    for name, worker in config.get('remote_workers', {}).items():
        from .remote import RemoteAdapter
        token = Path(worker['token_file']).read_text(encoding='utf-8').strip() if 'token_file' in worker else worker['token']
        adapters[name] = RemoteAdapter(worker['url'], worker['node_id'], token, worker['engine'], worker['allowed_owners'],
                                       timeout=worker.get('timeout', 5), ca_file=worker.get('ca_file'))
    gt = config['gitea']
    gitea_token = Path(gt['token_file']).read_text(encoding='utf-8').strip() if gt.get('token_file') else None
    client = GiteaClient(gt['url'], gt['allowed_hosts'], gt['user'], gt.get('password', ''), gt['repos'],
                         token=gitea_token, ca_file=gt.get('ca_file'))
    runtime = Runtime(config['state_dir'], adapters, client, actors, config['worker_secret'], publish_status=True,
                      recover_background=bool(config.get('remote_workers')))
    server = make_server(runtime, (config.get('listen', '127.0.0.1'), config.get('port', 18881)),
                         {name: value['token'] for name, value in actors.items()}, config['webhook_secret'])
    def shutdown(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown)
    try:
        server.serve_forever()
    finally:
        runtime.close()
        server.server_close()


if __name__ == '__main__':
    main()
