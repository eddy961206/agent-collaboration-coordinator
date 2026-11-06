"""Real isolated Gitea + RT; three human accounts and deterministic workers."""
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

from coordinator.adapters import DirectAdapter, RoundtableAdapter
from coordinator.runtime import Runtime
from coordinator.server import make_server
from coordinator.transport import GiteaClient, LabHTTP

OUT = Path('/out')
SESSION = json.loads(Path('/app/.runtime/lab-session.json').read_text())
PASSWORD = SESSION['password']
REPO = 'labadmin/synthetic'
BASE = 'http://coordinator:18881'
OPENER = build_opener(ProxyHandler({}))
CHECKS = []


def call(method, path, body=None, *, actor='alice', token=None, key=None, base=BASE, basic=False):
    auth = ('Basic ' + base64.b64encode(f'{actor}:{PASSWORD}'.encode()).decode()) if basic else 'Bearer ' + (token or 'fixture-' + actor)
    headers = {'Authorization': auth, 'Content-Type': 'application/json'}
    if key:
        headers['Idempotency-Key'] = key
    request = Request(base + path, json.dumps(body).encode() if body is not None else None, headers, method=method)
    try:
        with OPENER.open(request, timeout=20) as response:
            data = response.read()
            return response.status, json.loads(data) if data else None
    except HTTPError as exc:
        return exc.code, json.loads(exc.read() or b'{}')


def gt(method, path, body=None, actor='labadmin'):
    code, result = call(method, '/api/v1' + path, body, actor=actor, base='http://gitea:3000', basic=True)
    if code >= 300:
        raise AssertionError(f'Gitea {method} {path}: {code} {result}')
    return result


def check(name, condition, evidence):
    CHECKS.append({'name': name, 'passed': bool(condition), 'evidence': evidence})
    (OUT / 'integration-results.json').write_text(json.dumps({'checks': CHECKS, 'model_calls': 0}, indent=2), encoding='utf-8')
    print(json.dumps(CHECKS[-1]), flush=True)
    if not condition:
        raise AssertionError(name)


def wait(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.1)
    raise AssertionError('bounded observation timed out')


def command(tid, action, data, actor, key=None):
    return call('POST', '/api/tasks/' + quote(tid, safe='') + '/commands',
                {'action': action, 'data': data}, actor=actor, key=key or uuid.uuid4().hex)


def task(tid):
    code, result = call('GET', '/api/tasks/' + quote(tid, safe=''))
    assert code == 200, (code, result)
    return result['task']


def post_comment(number, body, actor):
    return gt('POST', f'/repos/{REPO}/issues/{number}/comments', {'body': body}, actor)


def signed_delivery(payload, secret):
    raw = json.dumps(payload).encode()
    request = Request(BASE + '/webhooks/gitea', raw,
                      {'X-Gitea-Signature': hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest(),
                       'X-Gitea-Event': 'issue_comment', 'Content-Type': 'application/json'}, method='POST')
    with OPENER.open(request, timeout=15) as response:
        return json.loads(response.read())


def main():
    blocked = False
    try:
        with socket.create_connection(('1.1.1.1', 443), timeout=1):
            pass
    except OSError:
        blocked = True
    check('external_network_blocked', blocked, 'Docker --internal; outbound TCP attempt rejected')
    check('no_model_credentials', not any(k in os.environ for k in ['OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'GEMINI_API_KEY']), 'No user environment or credential mounts')
    actors = {}
    for name in ['alice', 'bob', 'carol', 'viewer']:
        row = gt('POST', '/admin/users', {'username': name, 'email': name + '@example.invalid',
                                         'password': PASSWORD, 'must_change_password': False})
        actors[name] = {'gitea_id': row['id'], 'role': 'viewer' if name == 'viewer' else 'operator'}
    admin = gt('GET', '/user')
    actors['labadmin'] = {'gitea_id': admin['id'], 'role': 'admin'}
    gt('POST', '/user/repos', {'name': 'synthetic', 'private': True, 'auto_init': True})
    for name in actors:
        if name != 'labadmin':
            gt('PUT', f'/repos/{REPO}/collaborators/{name}', {'permission': 'read' if name == 'viewer' else 'write'})
    rt_http = LabHTTP('http://127.0.0.1:18787', ['127.0.0.1'])
    wait(lambda: rt_http.request('GET', '/api/config'))
    provider_info = rt_http.request('GET', '/api/providers')
    check('real_rt_only_fake_cli_enabled', all(not x['available'] for x in provider_info['providers'] if x['id'] != 'claude'),
          [{'id': x['id'], 'available': x['available'], 'command': x['command']} for x in provider_info['providers']])
    direct = DirectAdapter(OUT / 'workers')
    rt = RoundtableAdapter('http://127.0.0.1:18787', ['claude', 'claude'], allowed_hosts=['127.0.0.1'], timeout=5)
    client = GiteaClient('http://gitea:3000', ['gitea'], 'labadmin', PASSWORD, [REPO])
    runtime = Runtime(OUT / 'state', {'direct': direct, 'rt': rt}, client, actors, 'fixture-worker-secret', publish_status=True)
    secret = 'fixture-hook-' + uuid.uuid4().hex
    server = make_server(runtime, ('0.0.0.0', 18881), {a: 'fixture-' + a for a in actors}, secret)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    try:
        specs = [('alice', 'direct'), ('carol', 'direct'), ('bob', 'rt'), ('alice', 'rt')]
        tasks = []
        for index, (owner, adapter) in enumerate(specs):
            issue = gt('POST', f'/repos/{REPO}/issues', {'title': f'Fixture {index}: {owner} {adapter}', 'body': 'Synthetic initial contract r1'}, owner)
            code, created = call('POST', '/api/tasks', {'repo': REPO, 'issue': issue['number'], 'owner': owner,
                                'decision_owner': 'bob', 'adapter': adapter, 'text': 'INITIAL_R1'}, actor='labadmin')
            assert code == 201, (code, created)
            tasks.append({'id': created['id'], 'number': issue['number'], 'owner': owner, 'adapter': adapter})
        gt('POST', f'/repos/{REPO}/hooks', {'type': 'gitea', 'active': True, 'events': ['issue_comment'],
            'config': {'url': BASE + '/webhooks/gitea', 'content_type': 'json', 'secret': secret}})
        check('three_people_have_distinct_gitea_accounts', len({actors[x]['gitea_id'] for x in ['alice', 'bob', 'carol']}) == 3,
              {x: actors[x]['gitea_id'] for x in ['alice', 'bob', 'carol']})
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(command, t['id'], 'start', {'revision': 1}, t['owner']) for t in tasks]
            for t, future in zip(tasks, futures):
                code, result = future.result()
                assert code == 200, (code, result)
                t['run'] = result['run']; t['token'] = result['worker_token']
        check('four_parallel_runs_with_and_without_rt', all(task(t['id'])['status'] == 'working' for t in tasks),
              [{'task': t['id'], 'run': t['run']['id'], 'adapter': t['adapter']} for t in tasks])
        for t in tasks[2:]:
            observed = rt.observe(t['run']['handle'])
            check('real_rt_two_seats_' + t['owner'], len(observed['children']) == 2 and observed['active'], observed['children'])
        code, denial = command(tasks[0]['id'], 'stop', {'run_id': tasks[0]['run']['id']}, 'viewer')
        check('viewer_cannot_mutate', code == 403, denial)
        code, denial = command(tasks[0]['id'], 'start', {'revision': 1}, 'alice')
        check('same_task_second_run_rejected', code == 409, denial)

        # Real webhook: canonical comment read, durable revision, direct worker update.
        changed = post_comment(tasks[0]['number'], '/coord change 1\nINSTALLER_ROLLBACK_R2', 'bob')
        wait(lambda: task(tasks[0]['id'])['revision'] == 2)
        def direct_received_change():
            observed = direct.observe(tasks[0]['run']['handle'])
            return observed if observed.get('revision') == 2 else None
        direct_obs = wait(direct_received_change)
        check('gitea_webhook_changes_direct_worker', direct_obs.get('revision') == 2, direct_obs)
        code, denial = call('POST', f"/api/runs/{tasks[0]['run']['id']}/publish", {'operation_id': 'stale-r1', 'body': 'old'}, token=tasks[0]['token'])
        check('delivery_alone_does_not_authorize_publish', code == 409 and denial['error'] == 'stale_revision', denial)
        code, ack = call('POST', f"/api/runs/{tasks[0]['run']['id']}/ack", {'revision': 2}, token=tasks[0]['token'])
        assert code == 200, ack
        opbody = {'operation_id': 'installer-result-r2', 'body': 'Implemented and lab-verified r2; customer release pending.'}
        first = call('POST', f"/api/runs/{tasks[0]['run']['id']}/publish", opbody, token=tasks[0]['token'])
        second = call('POST', f"/api/runs/{tasks[0]['run']['id']}/publish", opbody, token=tasks[0]['token'])
        comments = client.comments(REPO, tasks[0]['number'])
        matches = [c for c in comments if 'isolated-coordinator:operation:installer-result-r2' in c['body']]
        check('one_real_gitea_result_after_retry', first[0] == 200 and second[0] == 200 and len(matches) == 1, {'count': len(matches), 'http': [first[0], second[0]]})

        # Existing comment edited: same source ID, new body hash, new revision.
        gt('PATCH', f'/repos/{REPO}/issues/comments/{changed["id"]}', {'body': '/coord change 2\nINSTALLER_EXIT_CODES_R3'}, 'bob')
        wait(lambda: task(tasks[0]['id'])['revision'] == 3)
        event_payload = {'repository': {'full_name': REPO}, 'issue': {'number': tasks[0]['number']}, 'comment': changed, 'action': 'created'}
        signed_delivery(event_payload, secret)
        check('edit_and_late_delivery_keep_latest_revision', task(tasks[0]['id'])['revision'] == 3, {'source_id': changed['id'], 'revision': 3})

        # RT live change travels through real RT into fake CLI stdin on both seats.
        rt_task = tasks[2]
        post_comment(rt_task['number'], '/coord change 1\nRT_CHANGED_R2', 'bob')
        wait(lambda: task(rt_task['id'])['revision'] == 2)
        def observations():
            return [json.loads(line) for path in (OUT / 'rt' / 'cli-observations').glob('*.jsonl') for line in path.read_text().splitlines()]
        wait(lambda: sum('RT_CHANGED_R2' in json.dumps(row) for row in observations() if row['event'] == 'input') >= 2)
        check('rt_change_reaches_two_fake_cli_seats', True, {'input_receipts': sum('RT_CHANGED_R2' in json.dumps(row) for row in observations() if row['event'] == 'input')})

        # Stop and resume one direct and one RT task, others keep running.
        for t in [tasks[0], tasks[2]]:
            old = t['run']['id']
            old_token = t['token']
            old_pids = {row['pid'] for row in observations() if row['event'] == 'input' and t['id'] in json.dumps(row)} if t['adapter'] == 'rt' else set()
            post_comment(t['number'], '/coord stop ' + old, t['owner'])
            wait(lambda: task(t['id'])['status'] == 'paused')
            stopped = runtime.store.run(old)
            check('stop_confirmed_' + t['adapter'], stopped['status'] == 'stopped', runtime.observe(old)['observation'])
            if old_pids:
                wait(lambda: all(not Path(f'/proc/{pid}').exists() for pid in old_pids))
                check('rt_stopped_cli_processes_are_gone', True, {'pids': sorted(old_pids), 'pid_namespace': 'shared only between isolated RT and test runner'})
            paused = task(t['id'])
            code, result = command(t['id'], 'resume', {'revision': paused['revision'], 'generation': paused['generation']}, t['owner'])
            assert code == 200, (code, result)
            t['run'] = result['run']; t['token'] = result['worker_token']
            check('new_run_latest_request_' + t['adapter'], t['run']['id'] != old and t['run']['revision'] == paused['revision'],
                  {'old': old, 'new': t['run']['id'], 'revision': t['run']['revision']})
            code, denial = command(t['id'], 'stop', {'run_id': old}, t['owner'])
            check('late_stop_cannot_stop_resumed_' + t['adapter'], code == 409 and task(t['id'])['status'] == 'working', denial)
            code, denial = call('POST', f'/api/runs/{old}/publish', {'operation_id': 'late-' + old, 'body': 'late'}, token=old_token)
            check('old_worker_cannot_publish_' + t['adapter'], code == 409, denial)
        check('unrelated_runs_continue_during_stop', all(runtime.store.run(t['run']['id'])['status'] == 'running' for t in [tasks[1], tasks[3]]),
              [t['run']['id'] for t in [tasks[1], tasks[3]]])

        # Two people concurrently change the same base: preserve conflict and resolve explicitly.
        t = tasks[1]
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda entry: post_comment(t['number'], '/coord change 1\n' + entry[1], entry[0]), [('carol', 'OPTION_A'), ('bob', 'OPTION_B')]))
        wait(lambda: task(t['id'])['conflict_pending'])
        code, result = command(t['id'], 'change', {'base_revision': task(t['id'])['revision'], 'text': 'DECISION_AGREED_R3'}, 'bob')
        check('conflicting_people_resolve_with_decision_owner', code == 200 and not task(t['id'])['conflict_pending'], result['status'])

        # Unknown external receipt: publish happened in Gitea, response lost locally.
        rid = t['run']['id']; token = t['token']
        call('POST', f'/api/runs/{rid}/ack', {'revision': task(t['id'])['revision']}, token=token)
        original_post = client.post
        def lost_receipt(repo, number, body):
            original_post(repo, number, body)
            raise TimeoutError('injected response loss after stored comment')
        client.post = lost_receipt
        lost = call('POST', f'/api/runs/{rid}/publish', {'operation_id': 'lost-receipt', 'body': 'Stored once; reconcile required.'}, token=token)
        client.post = original_post
        retry = call('POST', f'/api/runs/{rid}/publish', {'operation_id': 'lost-receipt', 'body': 'Stored once; reconcile required.'}, token=token)
        check('lost_receipt_is_not_republished', lost[0] == 503 and retry[0] == 409,
              {'initial_http': lost[0], 'retry_http': retry[0], 'status': runtime.store.operation('lost-receipt')['status']})
        code, reconciled = call('POST', '/api/operations/lost-receipt/reconcile', {}, actor='bob')
        check('reconcile_against_actual_gitea_comment', code == 200 and reconciled['status'] == 'succeeded', reconciled['receipt'])

        # One reusable status comment, no new runs from bot messages.
        before = len(runtime.store.runs())
        for _ in range(2):
            code, status = call('POST', '/api/tasks/' + quote(t['id'], safe='') + '/status', {}, actor='carol')
            assert code == 200, status
        status_rows = [c for c in client.comments(REPO, t['number']) if c['body'].startswith('<!-- isolated-coordinator:status:')]
        check('single_status_comment_and_no_self_wakeup', len(status_rows) == 1 and len(runtime.store.runs()) == before, {'status_comments': len(status_rows), 'runs': before})
        wait(lambda: all(any(c['body'].startswith('<!-- isolated-coordinator:status:') for c in client.comments(REPO, row['number'])) for row in tasks))
        check('event_driven_status_for_all_tasks', not runtime.status_errors, {'tasks': len(tasks), 'model_calls': 0})

        # Direct completion is an actual parent/child exit before the result is finalized.
        direct.complete(t['run']['handle'])
        code, finished = call('POST', f'/api/runs/{rid}/finish', {'result': {'implemented': True, 'customer_released': False},
                      'evidence': {'kind': 'fixture', 'revision': task(t['id'])['revision']}}, token=token)
        check('normal_finish_preserves_delivery_scope', code == 200 and task(t['id'])['status'] == 'done', finished)
        # Real RT normal completion, still a deterministic CLI response.
        t = tasks[3]
        rt.deliver(t['run']['handle'], {'revision': 1, 'text': 'LAB_FINISH'})
        wait(lambda: rt.observe(t['run']['handle'])['stopped'])
        code, finished = call('POST', f"/api/runs/{t['run']['id']}/finish", {'result': {'reviewed': True}, 'evidence': {'kind': 'fake_cli_only'}}, token=t['token'])
        check('real_rt_normal_finish', code == 200 and task(t['id'])['status'] == 'done', finished)
        result = {'scope': 'Real isolated Gitea and pinned RT; fake people and fake model CLI',
                  'checks': CHECKS, 'passed': sum(x['passed'] for x in CHECKS), 'total': len(CHECKS),
                  'model_calls': 0, 'gitea_version': SESSION['gitea_version'],
                  'rt_binary_sha256': SESSION['rt_binary_sha256'], 'tasks': runtime.store.tasks(),
                  'limits': ['single Docker host', 'fake model does not understand requirements', 'existing apps/services untouched', 'production auth and deployment not enabled']}
        (OUT / 'integration-results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps({'passed': result['passed'], 'total': result['total'], 'model_calls': 0}), flush=True)
    finally:
        server.shutdown(); server.server_close(); thread.join()
        runtime.close()


if __name__ == '__main__':
    main()
