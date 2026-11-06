"""Two physical hosts via existing SSH authentication, using fixed fake workers.

Only a fresh remote directory and owned worker/tunnel processes are created.
No real CLI, model, user credentials, host settings or existing service is used.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import base64
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import secrets
import shlex
import socket
import subprocess
import sys
import tarfile
import time
import uuid

from coordinator.remote import RemoteAdapter


ROOT = Path(__file__).resolve().parents[1]
REMOTE_HELPER = r'''
import base64, io, json, os, pathlib, re, shutil, signal, subprocess, sys, tarfile, time
request=json.load(sys.stdin)
identity=request['id']
if not re.fullmatch(r'[0-9a-f]{32}',identity): raise SystemExit('invalid_lab_id')
home=pathlib.Path.home().resolve()
root=home/('codex-coordinator-lab-'+identity)
if root.is_symlink() or root.resolve().parent!=home: raise SystemExit('invalid_lab_root')
if request['action']=='start':
    root.mkdir(mode=0o700)
    with tarfile.open(fileobj=io.BytesIO(base64.b64decode(request['archive'])),mode='r:gz') as archive:
        for member in archive.getmembers():
            target=root/member.name
            if not member.isfile() or member.name.startswith('/') or '..' in pathlib.PurePosixPath(member.name).parts:
                raise SystemExit('invalid_archive_member')
            if target.resolve().is_relative_to(root.resolve()) is not True: raise SystemExit('archive_path_escape')
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(archive.extractfile(member).read())
    config=root/'fixture-config.json'
    config.write_text(json.dumps(request['config']),encoding='utf-8')
    config.chmod(0o600)
    output=(root/'worker.log').open('wb')
    environment={'PATH':'/usr/bin:/bin:/opt/homebrew/bin','HOME':str(root),'PYTHONUTF8':'1','PYTHONNOUSERSITE':'1'}
    process=subprocess.Popen([sys.executable,'-B','-m','lab.physical_worker_launcher','--config',str(config)],
        cwd=root,env=environment,stdin=subprocess.DEVNULL,stdout=output,stderr=output,start_new_session=True)
    (root/'launch.json').write_text(json.dumps({'pid':process.pid,'nonce':request['config']['nonce']}))
    for _ in range(100):
        ready=root/'ready.json'
        if ready.exists(): print(ready.read_text()); break
        if process.poll() is not None: raise SystemExit('fixture_worker_start_failed')
        time.sleep(.1)
    else: raise SystemExit('fixture_worker_start_timeout')
elif request['action']=='stop':
    if not root.exists(): print(json.dumps({'cleaned':True,'already_absent':True})); raise SystemExit(0)
    launch=json.loads((root/'launch.json').read_text())
    if launch['nonce']!=request['nonce']: raise SystemExit('worker_nonce_mismatch')
    pid=int(launch['pid'])
    expected=str(root/'fixture-config.json')
    actual=subprocess.run(['/bin/ps','-p',str(pid),'-o','command='],capture_output=True,text=True).stdout
    if actual.strip():
        if expected not in actual or 'lab.physical_worker_launcher' not in actual:
            raise SystemExit('worker_process_identity_changed')
        os.kill(pid,signal.SIGTERM)
        for _ in range(100):
            actual=subprocess.run(['/bin/ps','-p',str(pid),'-o','command='],capture_output=True,text=True).stdout
            if not actual.strip(): break
            time.sleep(.1)
        else: raise SystemExit('fixture_worker_stop_unconfirmed')
    if root.is_symlink() or root.resolve().parent!=home or root.name!='codex-coordinator-lab-'+identity:
        raise SystemExit('cleanup_path_mismatch')
    shutil.rmtree(root)
    print(json.dumps({'cleaned':True,'worker_stopped':True}))
else: raise SystemExit('invalid_action')
'''


def free_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.1)
    raise RuntimeError('bounded_observation_timeout')


def run_spec(identity, owner, revision=1, generation=1, text='INITIAL_REQUEST', resume_from=None):
    value = {'id': identity, 'task_id': 'synthetic/' + owner + '#1', 'owner': owner,
             'adapter': 'fixture', 'revision': revision, 'generation': generation, 'text': text}
    if resume_from:
        value['resume_from'] = resume_from
    return value


def child_identity(observation):
    return [(child.get('pid'), child.get('id')) for child in observation.get('children', [])]


def archive_source():
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w:gz') as archive:
        for path in sorted((ROOT / 'coordinator').glob('*.py')):
            archive.add(path, arcname='coordinator/' + path.name, recursive=False)
        archive.add(Path(__file__).with_name('physical_worker_launcher.py'), arcname='lab/physical_worker_launcher.py', recursive=False)
    return base64.b64encode(buffer.getvalue()).decode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ssh-config', required=True)
    parser.add_argument('--ssh-host', required=True)
    parser.add_argument('--remote-python', default='/opt/homebrew/bin/python3')
    args = parser.parse_args()
    identity = uuid.uuid4().hex
    directory = ROOT / '.runtime' / ('physical-' + identity)
    directory.mkdir(parents=True)
    ssh = ['ssh', '-F', str(Path(args.ssh_config).resolve()), '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
           '-o', 'StrictHostKeyChecking=yes', '-o', 'UpdateHostKeys=no']
    remote_config = {'node_id': 'physical-node-b-' + identity, 'token': secrets.token_hex(32),
                     'nonce': secrets.token_hex(24), 'owners': ['bob']}
    local_config = {'node_id': 'physical-node-a-' + identity, 'token': secrets.token_hex(32),
                    'nonce': secrets.token_hex(24), 'owners': ['alice']}
    local_path = directory / 'fixture-config.json'
    local_path.write_text(json.dumps(local_config), encoding='utf-8')
    checks = []
    remote_created = False
    local_process = tunnel = None
    worker_log = tunnel_log = None
    all_handles = []

    def save():
        result = {'scope': 'Two physical computers; fixed Python workers; SSH loopback tunnel',
                  'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
                  'model_calls': 0, 'checks': checks, 'passed': sum(x['passed'] for x in checks), 'total': len(checks)}
        (directory / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        return result

    def check(name, condition, evidence):
        checks.append({'name': name, 'passed': bool(condition), 'evidence': evidence})
        save()
        print(json.dumps({'name': name, 'passed': bool(condition)}), flush=True)
        if not condition:
            raise AssertionError(name)

    def remote(action):
        request = {'id': identity, 'action': action, 'nonce': remote_config['nonce']}
        if action == 'start':
            request.update(archive=archive_source(), config=remote_config)
        result = subprocess.run([*ssh, args.ssh_host, shlex.quote(args.remote_python) + ' -c ' + shlex.quote(REMOTE_HELPER)],
                                input=json.dumps(request), text=True, encoding='utf-8', capture_output=True, timeout=40)
        if result.returncode:
            (directory / ('remote-' + action + '.log')).write_text(result.stderr, encoding='utf-8')
            raise RuntimeError('remote_fixture_' + action + '_failed')
        return json.loads(result.stdout)

    def start_tunnel(port, remote_port):
        process = subprocess.Popen([*ssh, '-o', 'ExitOnForwardFailure=yes', '-o', 'ServerAliveInterval=15',
                                    '-N', '-L', f'127.0.0.1:{port}:127.0.0.1:{remote_port}', args.ssh_host],
                                   stdin=subprocess.DEVNULL, stdout=tunnel_log, stderr=tunnel_log,
                                   creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        def connected():
            if process.poll() is not None:
                raise RuntimeError('ssh_tunnel_start_failed')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    return True
            except OSError:
                return False
        wait_for(connected)
        return process

    try:
        worker_log = (directory / 'local-worker.log').open('wb')
        environment = {k: os.environ[k] for k in ('SystemRoot', 'WINDIR', 'PATH') if k in os.environ}
        environment.update({'HOME': str(directory), 'USERPROFILE': str(directory), 'PYTHONUTF8': '1', 'PYTHONNOUSERSITE': '1'})
        local_process = subprocess.Popen([sys.executable, '-B', '-m', 'lab.physical_worker_launcher', '--config', str(local_path)],
                                         cwd=ROOT, env=environment, stdin=subprocess.DEVNULL, stdout=worker_log, stderr=worker_log,
                                         creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        ready_local = wait_for(lambda: json.loads((directory / 'ready.json').read_text()) if (directory / 'ready.json').exists() else None)
        remote_created = True
        ready_remote = remote('start')
        check('physical_operating_systems', ready_local['platform'] != ready_remote['platform'],
              {'node_a': {'platform': ready_local['platform'], 'python': ready_local['python']},
               'node_b': {'platform': ready_remote['platform'], 'python': ready_remote['python']}})
        tunnel_log = (directory / 'tunnel.log').open('wb')
        local_port = free_port()
        tunnel = start_tunnel(local_port, ready_remote['port'])
        local_url = f'http://127.0.0.1:{ready_local["port"]}'
        remote_url = f'http://127.0.0.1:{local_port}'
        def adapters():
            return (RemoteAdapter(local_url, local_config['node_id'], local_config['token'], 'fixture', ['alice']),
                    RemoteAdapter(remote_url, remote_config['node_id'], remote_config['token'], 'fixture', ['bob']))
        local, other = adapters()
        initial = [run_spec('local-' + identity, 'alice'), run_spec('remote-' + identity, 'bob')]
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(adapter.start, run) for adapter, run in zip((local, other), initial)]
            handles = [f.result() for f in futures]
        all_handles.extend(zip((local, other), handles))
        observed = [adapter.observe(handle) for adapter, handle in zip((local, other), handles)]
        check('parallel_workers_on_two_hosts', all(not item['stopped'] for item in observed),
              [{'children': item.get('children', []), 'stopped': item['stopped']} for item in observed])
        original_children = [child_identity(item) for item in observed]
        duplicate = other.start(initial[1])
        check('start_retransmission_does_not_spawn_again', duplicate == handles[1] and child_identity(other.observe(handles[1])) == original_children[1], 'same handle and child process identity')
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(adapter.deliver, handle, {'revision': 2, 'text': 'CHANGED_R2'})
                       for adapter, handle in zip((local, other), handles)]
            receipts = [future.result() for future in futures]
        observed = [adapter.observe(handle) for adapter, handle in zip((local, other), handles)]
        check('request_change_on_each_physical_host', all(item.get('revision', item.get('accepted_revision')) == 2 for item in observed), receipts)
        local.close(); other.close()
        local, other = adapters()
        recovered = [adapter.recover(handle) for adapter, handle in zip((local, other), handles)]
        check('new_coordinator_clients_recover_existing_workers', all(not item['stopped'] for item in recovered)
              and all(child_identity(item) == old for item, old in zip(recovered, original_children)), 'persistent handle, same child process identity')
        tunnel.terminate(); tunnel.wait(timeout=10); tunnel = None
        unavailable = False
        try:
            other.observe(handles[1])
        except Exception:
            unavailable = True
        check('tunnel_loss_does_not_stop_local_work', unavailable and not local.observe(handles[0])['stopped'], 'remote unavailable; local worker remains active')
        tunnel = start_tunnel(local_port, ready_remote['port'])
        local, other = adapters()
        recovered = other.recover(handles[1])
        check('tunnel_reconnect_reuses_remote_worker', not recovered['stopped'] and child_identity(recovered) == original_children[1], 'same remote child process identity')
        with ThreadPoolExecutor(max_workers=2) as pool:
            stopped = list(pool.map(lambda pair: pair[0].stop(pair[1]), zip((local, other), handles)))
        check('targeted_stop_on_both_hosts', all(item['stopped'] for item in stopped), 'each host confirms its own child stopped')
        resumed = [run_spec('resumed-local-' + identity, 'alice', 3, 3, 'LATEST_R3', initial[0]['id']),
                   run_spec('resumed-remote-' + identity, 'bob', 3, 3, 'LATEST_R3', initial[1]['id'])]
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(adapter.start, run) for adapter, run in zip((local, other), resumed)]
            newer = [future.result() for future in futures]
        all_handles.extend(zip((local, other), newer))
        latest = [adapter.observe(handle) for adapter, handle in zip((local, other), newer)]
        check('resume_uses_latest_request_on_both_hosts', all(not item['stopped'] and item.get('revision', item.get('accepted_revision')) == 3 for item in latest), 'new run identities; latest request revision 3')
        for adapter, old_handle in zip((local, other), handles):
            adapter.stop(old_handle)
        check('late_stop_does_not_touch_resumed_workers', all(not adapter.observe(handle)['stopped'] for adapter, handle in zip((local, other), newer)), 'old run stops are idempotent')
        for adapter, handle in zip((local, other), newer):
            adapter.stop(handle)
        check('final_children_stopped', all(adapter.observe(handle)['stopped'] for adapter, handle in zip((local, other), newer)), 'both hosts confirmed final stops')
    finally:
        # Only known handles and Popen objects created by this harness are touched.
        for adapter, handle in all_handles:
            try:
                adapter.stop(handle)
            except Exception:
                pass
        if tunnel is not None:
            tunnel.terminate(); tunnel.wait(timeout=10)
        if local_process is not None and local_process.poll() is None:
            local_process.terminate(); local_process.wait(timeout=10)
        if worker_log:
            worker_log.close()
        if tunnel_log:
            tunnel_log.close()
        if remote_created:
            cleanup = remote('stop')
            check('remote_fixture_cleanup', cleanup.get('cleaned') is True, 'owned remote worker stopped and dedicated directory removed')
        save()
    result = save()
    if all(row['passed'] for row in checks):
        result['limits'] = ['fake workers only on physical hosts', 'client/tunnel restart is not coordinator process crash',
                            'native CLI and RT behavior tested in separate isolated lab', 'not a production rollout']
        (ROOT / 'reports' / 'physical-pc-summary.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'passed': result['passed'], 'total': result['total'], 'model_calls': 0}), flush=True)


if __name__ == '__main__':
    main()
