"""Real native CLI/RT + separate worker/coordinator processes, no model service.

Run only in a fresh Docker --network none container. This is a process/network
protocol test on one host; it is not the separate physical-PC test.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import ProxyHandler, Request, build_opener
import uuid

from coordinator.remote import RemoteAdapter
from coordinator.adapters import AdapterError
from lab.fake_model_api import FixtureServer


class FakeGitea(ThreadingHTTPServer):
    daemon_threads = True
    def __init__(self, address):
        self.comments = {}
        self.serial = 0
        self.lock = threading.Lock()
        super().__init__(address, GiteaHandler)


class GiteaHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, code, value):
        raw = json.dumps(value).encode()
        self.send_response(code); self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)

    def do_GET(self):
        parts = urlsplit(self.path).path.split('/')
        if parts[:6] != ['', 'api', 'v1', 'repos', 'fake', 'team'] or parts[6] != 'issues':
            return self.reply(404, {})
        with self.server.lock:
            if len(parts) == 8:
                return self.reply(200, {'number': int(parts[7]), 'html_url': f'http://127.0.0.1:19092/fake/team/issues/{parts[7]}'})
            if parts[7] == 'comments':
                row = self.server.comments.get(int(parts[8]))
                return self.reply(200 if row else 404, row or {})
            if parts[8] == 'comments':
                return self.reply(200, [x for x in self.server.comments.values() if x['issue'] == int(parts[7])])
        self.reply(404, {})

    def do_POST(self):
        self.mutate(False)

    def do_PATCH(self):
        self.mutate(True)

    def mutate(self, patch):
        parts = urlsplit(self.path).path.split('/')
        body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
        with self.server.lock:
            if patch:
                row = self.server.comments[int(parts[-1])]
                row['body'] = body['body']
            else:
                number = int(parts[-2]); self.server.serial += 1
                row = {'id': self.server.serial, 'issue': number, 'body': body['body'],
                       'user': {'login': 'coordinator-bot', 'id': 9},
                       'issue_url': f'http://127.0.0.1:19092/fake/team/issues/{number}',
                       'html_url': f'http://127.0.0.1:19092/fake/team/issues/{number}#issuecomment-{self.server.serial}'}
                self.server.comments[row['id']] = row
            self.reply(200 if patch else 201, row)


def wait(predicate, seconds=45):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (URLError, ConnectionError, TimeoutError):
            pass
        except AdapterError as exc:
            if exc.code != 'worker_unreachable':
                raise
        time.sleep(.1)
    raise AssertionError('bounded observation timed out: ' + str(last)[:150])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--claude', required=True)
    parser.add_argument('--codex', required=True)
    parser.add_argument('--rt', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True)
    opener = build_opener(ProxyHandler({}))
    processes, logs, checks = [], [], []
    provider = FixtureServer(('127.0.0.1', 19091), out / 'provider')
    provider.hold = True
    gitea = FakeGitea(('127.0.0.1', 19092))
    servers = [provider, gitea]
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in servers]
    for thread in threads:
        thread.start()

    def check(name, condition, evidence=None):
        checks.append({'name': name, 'passed': bool(condition), 'evidence': evidence})
        (out / 'native-distributed.json').write_text(json.dumps({'checks': checks, 'model_calls': 0}, indent=2), encoding='utf-8')
        print(json.dumps({'name': name, 'passed': bool(condition)}), flush=True)
        if not condition:
            raise AssertionError(name + ': ' + str(evidence)[:300])

    def request(method, path, body=None, actor='alice', token=None, key=None):
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + (token or 'fixture-' + actor)}
        if key:
            headers['Idempotency-Key'] = key
        data = json.dumps(body).encode() if body is not None else None
        req = Request('http://127.0.0.1:18881' + path, data, headers, method=method)
        try:
            with opener.open(req, timeout=40) as response:
                raw = response.read(); return response.status, json.loads(raw) if raw else {}
        except HTTPError as exc:
            return exc.code, json.loads(exc.read() or b'{}')

    def command(task, action, data, actor=None, key=None):
        return request('POST', '/api/tasks/' + quote(task['id'], safe='') + '/commands',
                       {'action': action, 'data': data}, actor=actor or task['owner'], key=key or uuid.uuid4().hex)

    def current(task):
        code, data = request('GET', '/api/tasks/' + quote(task['id'], safe=''))
        assert code == 200, data
        return data

    def spawn(label, command_line, env=None):
        log = (out / (label + '.log')).open('w', encoding='utf-8')
        logs.append(log)
        process = subprocess.Popen(command_line, cwd='/app', stdout=log, stderr=subprocess.STDOUT,
                                   env=env, start_new_session=True)
        processes.append(process)
        return process

    def config_file(name, value):
        path = out / name
        path.write_text(json.dumps(value, indent=2), encoding='utf-8')
        return str(path)

    try:
        blocked = False
        try:
            with socket.create_connection(('1.1.1.1', 443), timeout=1):
                pass
        except OSError:
            blocked = True
        check('external_network_blocked', blocked)
        wrappers = out / 'wrappers'; wrappers.mkdir()
        wrapper = '''#!{python}
import json,os,sys
kind={kind!r}
exe={exe!r}
home={home!r}
os.makedirs(home,exist_ok=True)
os.makedirs(home+'/codex',exist_ok=True)
os.makedirs(home+'/claude',exist_ok=True)
env={{'PATH':os.defpath,'HOME':home,'USERPROFILE':home,'TMPDIR':'/tmp','NO_PROXY':'*',
     'CLAUDE_CONFIG_DIR':home+'/claude','CODEX_HOME':home+'/codex',
     'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC':'1','DISABLE_TELEMETRY':'1','DISABLE_AUTOUPDATER':'1'}}
args=sys.argv[1:]
if kind=='claude':
    env.update({{'ANTHROPIC_BASE_URL':'http://127.0.0.1:19091','ANTHROPIC_API_KEY':'fixture-not-real'}})
    args=['--bare',*args,'--setting-sources','','--mcp-config','{{"mcpServers":{{}}}}']
else:
    env['COORDINATOR_PROVIDER_KEY']='fixture-not-real'
    cfg={{'model_provider':'coordinator','model_providers.coordinator.name':'Fixture','model_providers.coordinator.base_url':'http://127.0.0.1:19091/v1','model_providers.coordinator.wire_api':'responses','model_providers.coordinator.env_key':'COORDINATOR_PROVIDER_KEY','model_providers.coordinator.requires_openai_auth':False,'model_providers.coordinator.supports_websockets':False,'model_providers.coordinator.request_max_retries':0,'model_providers.coordinator.stream_max_retries':0,'features.plugins':False,'features.apps':False,'analytics.enabled':False}}
    for key,value in cfg.items():args.extend(['-c',key+'='+json.dumps(value)])
os.execve(exe,[exe,*args],env)
'''
        rt_ports = {'node-a': 18787, 'node-b': 18788}
        for node, rt_port in rt_ports.items():
            wrapper_dir = wrappers / node
            wrapper_dir.mkdir()
            for kind in ('claude', 'codex'):
                path = wrapper_dir / kind
                path.write_text(wrapper.format(python=sys.executable, kind=kind, exe=getattr(args, kind), home=str(out / (node + '-rt-home'))), encoding='utf-8')
                path.chmod(0o700)
            spawn('roundtable-' + node, [args.rt, '-addr', f'127.0.0.1:{rt_port}', '-data', str(out / (node + '-rt')),
                       '-claude', str(wrapper_dir / 'claude'), '-codex', str(wrapper_dir / 'codex'),
                       '-agy', '/nonexistent', '-opencode', '/nonexistent', '-update-check', 'off'])
            wait(lambda port=rt_port: opener.open(f'http://127.0.0.1:{port}/api/config', timeout=2).status == 200)
        owners = ['alice', 'bob', 'carol']
        native = lambda kind: {'kind': kind, 'executable': getattr(args, kind), 'provider_config': {
            'mode': 'fixture', 'network_isolated': True, 'base_url': 'http://127.0.0.1:19091' + ('/v1' if kind == 'codex' else ''), 'timeout': 15}}
        remote_configs = {}
        clients = {}
        for node, port, kind in [('node-a', 19882, 'claude'), ('node-b', 19883, 'codex')]:
            config = {'state_dir': str(out / node), 'node_id': node, 'token': 'fixture-' + node,
                      'port': port, 'allowed_owners': owners, 'engines': {kind: native(kind),
                      'rt': {'kind': 'rt', 'url': f'http://127.0.0.1:{rt_ports[node]}', 'participants': ['claude', 'codex'], 'allowed_hosts': ['127.0.0.1']}}}
            worker = spawn(node, [sys.executable, '-B', '-m', 'coordinator.worker_host', '--config', config_file(node + '.json', config)])
            for engine in (kind, 'rt'):
                name = node + '-' + engine
                remote_configs[name] = {'url': f'http://127.0.0.1:{port}', 'node_id': node, 'token': 'fixture-' + node,
                                        'engine': engine, 'allowed_owners': owners, 'timeout': 30}
                clients[name] = RemoteAdapter(remote_configs[name]['url'], node, 'fixture-' + node, engine, owners, timeout=30)
                wait(lambda client=clients[name]: client.info())
        config = {'mode': 'distributed', 'state_dir': str(out / 'coordinator'), 'port': 18881,
                  'worker_secret': 'fixture-workers', 'webhook_secret': 'fixture-webhook',
                  'actors': {name: {'gitea_id': index + 1, 'role': 'admin' if name == 'admin' else 'operator', 'token': 'fixture-' + name}
                             for index, name in enumerate([*owners, 'admin'])},
                  'gitea': {'url': 'http://127.0.0.1:19092', 'allowed_hosts': ['127.0.0.1'], 'user': 'coordinator-bot', 'password': 'fixture', 'repos': ['fake/team']},
                  'remote_workers': remote_configs}
        coordinator_path = config_file('coordinator.json', config)
        coordinator = spawn('coordinator-first', [sys.executable, '-B', '-m', 'coordinator.server', '--config', coordinator_path])
        wait(lambda: request('GET', '/health')[0] == 200)
        tasks = []
        for number, (owner, adapter) in enumerate([('alice', 'node-a-claude'), ('bob', 'node-b-codex'), ('carol', 'node-a-rt'), ('alice', 'node-b-rt')], 1):
            code, data = request('POST', '/api/tasks', {'repo': 'fake/team', 'issue': number, 'owner': owner,
                               'decision_owner': 'bob', 'adapter': adapter, 'text': f'INITIAL_{adapter}_R1'}, actor='admin')
            assert code == 201, (code, data)
            tasks.append({'id': data['id'], 'owner': owner, 'adapter': adapter})
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(command, task, 'start', {'revision': 1}) for task in tasks]
            for task, future in zip(tasks, futures):
                code, data = future.result(); assert code == 200, (code, data)
                task['run'] = data['run']; task['token'] = data['worker_token']
        check('four_real_native_or_rt_jobs_started', all(current(t)['task']['status'] == 'working' for t in tasks))
        def observation(task):
            return clients[task['adapter']].observe(task['run']['handle'])
        before = {t['id']: observation(t) for t in tasks}
        for task in tasks:
            if task['adapter'].endswith('-rt'):
                def seats_running(task=task):
                    state = observation(task)
                    if any(p['status'] == 'error' for p in state['children']):
                        raise AssertionError('real RT seat failed; inspect roundtable log')
                    return len(state['children']) == 2 and all(p['status'] == 'running' for p in state['children'])
                wait(seats_running)
        wait(lambda: len(provider.requests) >= 6)
        for task in tasks:
            code, data = command(task, 'change', {'base_revision': 1, 'text': 'CHANGED_R2_' + task['adapter']}, actor='bob')
            assert code == 200 and data['status'] == 'accepted', (code, data)
        check('mid_request_changes_accepted', all(current(t)['task']['revision'] == 2 for t in tasks))
        old_pid = coordinator.pid
        coordinator.kill(); coordinator.wait(timeout=5)
        after_death = {t['id']: observation(t) for t in tasks}
        check('workers_continue_after_coordinator_sigkill', all(not value['stopped'] for value in after_death.values()),
              {'old_coordinator_pid': old_pid})
        coordinator = spawn('coordinator-restarted', [sys.executable, '-B', '-m', 'coordinator.server', '--config', coordinator_path])
        wait(lambda: request('GET', '/health')[0] == 200)
        wait(lambda: all(current(t)['task']['status'] == 'working' for t in tasks))
        recovered = {t['id']: observation(t) for t in tasks}
        for task in tasks:
            state = current(task)
            check('same_run_recovered_' + task['adapter'], len(state['runs']) == 1 and state['runs'][0]['id'] == task['run']['id'])
            old, new = before[task['id']], recovered[task['id']]
            if task['adapter'].endswith('-rt'):
                check('same_rt_room_recovered_' + task['adapter'], old['room_id'] == new['room_id'])
            else:
                check('same_cli_process_and_session_recovered_' + task['adapter'], old['pid'] == new['pid'] and old['session_id'] == new['session_id'])
        check('new_coordinator_process_is_distinct', coordinator.pid != old_pid)
        for task in tasks:
            old_run, old_token = task['run'], task['token']
            code, data = command(task, 'stop', {'run_id': old_run['id']})
            assert code == 200, (code, data)
            wait(lambda: current(task)['task']['status'] == 'paused')
            check('stopped_' + task['adapter'], observation(task)['stopped'])
            code, data = command(task, 'change', {'base_revision': 2, 'text': 'RESUMED_R3_' + task['adapter']}, actor='bob')
            assert code == 200, (code, data)
            paused = current(task)['task']
            code, data = command(task, 'resume', {'revision': 3, 'generation': paused['generation']})
            assert code == 200, (code, data)
            task['run'], task['token'] = data['run'], data['worker_token']
            check('resumed_new_run_with_latest_request_' + task['adapter'], task['run']['id'] != old_run['id'] and task['run']['revision'] == 3)
            code, denial = command(task, 'stop', {'run_id': old_run['id']})
            check('late_stop_rejected_' + task['adapter'], code == 409, denial)
            code, denial = request('POST', '/api/runs/' + old_run['id'] + '/publish', {'operation_id': 'late-' + old_run['id'], 'body': 'old'}, token=old_token)
            check('old_worker_cannot_publish_' + task['adapter'], code == 409, denial)
        # Complete the resumed jobs while the coordinator is truly absent.
        second_pid = coordinator.pid
        coordinator.kill(); coordinator.wait(timeout=5)
        provider.hold = False; provider.release.set()
        for task in tasks:
            done = wait(lambda: observation(task) if observation(task)['stopped'] else None)
            if task['adapter'].endswith('-rt'):
                messages = [m for m in done['messages'] if m.get('kind') == 'agent']
                completed = len(messages) >= 2 and all(m['status'] == 'done' for m in messages)
            else:
                completed = done['status'] == 'completed'
            check('native_or_rt_finished_' + task['adapter'], completed, {'status': done['status']})
        check('completed_while_coordinator_was_absent', coordinator.poll() is not None)
        coordinator = spawn('coordinator-after-completion', [sys.executable, '-B', '-m', 'coordinator.server', '--config', coordinator_path])
        wait(lambda: request('GET', '/health')[0] == 200)
        check('second_restart_is_new_process', coordinator.pid != second_pid)
        for task in tasks:
            wait(lambda: current(task)['task']['status'] == 'result_ready')
            state = current(task)
            run = next(row for row in state['runs'] if row['id'] == task['run']['id'])
            check('native_output_recovered_for_review_' + task['adapter'], run['result'] is not None and run['evidence']['reviewed'] is False)
            code, data = request('POST', '/api/runs/' + task['run']['id'] + '/ack', {'revision': 3}, token=task['token'])
            assert code == 200, (code, data)
            code, data = request('POST', '/api/runs/' + task['run']['id'] + '/publish', {'operation_id': 'result-' + task['run']['id'], 'body': 'Synthetic result only'}, token=task['token'])
            check('recovered_task_can_publish_current_revision_' + task['adapter'], code == 200)
        check('latest_resumed_requests_reached_fake_provider', all(any('RESUMED_R3_' + t['adapter'] in json.dumps(row) for row in provider.requests) for t in tasks))
        artifacts = {kind: {'sha256': hashlib.sha256(Path(getattr(args, kind)).read_bytes()).hexdigest(),
                            'version': subprocess.check_output([getattr(args, kind), '--version'], text=True).strip()}
                     for kind in ('claude', 'codex')}
        rt_hash = hashlib.sha256(Path(args.rt).read_bytes()).hexdigest()
        artifacts['roundtable'] = {'sha256': rt_hash, 'source_commit': '383e02d6dc2feed92865baeb0443c9bc37deea63' if rt_hash == '70fe534716722662e8f6765c0e7c8ab7287cadeb36ce6fe6a2e606a3c8da1f7c' else None}
        result = {'scope': 'Real Claude/Codex/RT with fake provider; distinct coordinator and two worker processes on one isolated host',
                  'checks': checks, 'passed': sum(x['passed'] for x in checks), 'total': len(checks), 'model_calls': 0,
                  'fake_requests': len(provider.requests), 'artifacts': artifacts, 'verified_at_utc': datetime.now(timezone.utc).isoformat(),
                  'network': 'Docker --network none, only loopback provider',
                  'limits': ['No real model reasoning', 'Physical PC test is separate']}
        (out / 'native-distributed.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
        (out / 'native-distributed-summary.json').write_text(json.dumps({**result, 'checks': [{'name': c['name'], 'passed': c['passed']} for c in checks]}, indent=2), encoding='utf-8')
        print(json.dumps({'passed': result['passed'], 'total': result['total'], 'model_calls': 0}), flush=True)
    finally:
        provider.release.set()
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=5)
        for log in logs:
            log.close()
        for server in servers:
            server.shutdown(); server.server_close()
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    main()
