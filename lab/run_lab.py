"""Own-container lifecycle only. No production service configuration is loaded."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
SESSION = ROOT / '.runtime' / 'lab-session.json'
PY_IMAGE = os.environ.get('COORDINATOR_LAB_IMAGE', 'agent-collaboration-coordinator-lab:local')
GITEA_IMAGE = 'gitea/gitea:latest'


def docker(*args, required=True):
    result = subprocess.run(['docker', *args], text=True, encoding='utf-8', capture_output=True)
    if required and result.returncode:
        raise RuntimeError(result.stderr[-1800:] or result.stdout[-1800:])
    return result


def up(binary):
    if SESSION.exists():
        previous = json.loads(SESSION.read_text(encoding='utf-8'))
        for name in previous['containers']:
            existing = docker('inspect', name, required=False)
            if existing.returncode == 0 and json.loads(existing.stdout)[0]['State']['Running']:
                raise RuntimeError('previous_lab_still_running; stop this lab before creating another')
    run_id = uuid.uuid4().hex[:10]
    prefix = 'codex-coord-' + run_id
    out = ROOT / '.runtime' / run_id
    out.mkdir(parents=True)
    source = Path(binary).resolve()
    target = out / 'roundtable-linux'
    shutil.copyfile(source, target)
    state = {'id': run_id, 'prefix': prefix, 'network': prefix, 'out': str(out),
             'containers': [], 'volumes': [], 'password': 'fixture-' + uuid.uuid4().hex,
             'rt_binary_sha256': hashlib.sha256(target.read_bytes()).hexdigest()}
    def save():
        SESSION.parent.mkdir(exist_ok=True)
        SESSION.write_text(json.dumps(state, indent=2), encoding='utf-8')
    save()
    docker('network', 'create', '--internal', '--label', 'codex.coordinator.lab=' + run_id, prefix)
    network_info = json.loads(docker('network', 'inspect', prefix).stdout)[0]
    state['subnet'] = network_info['IPAM']['Config'][0]['Subnet']
    volume = prefix + '-gitea'
    docker('volume', 'create', '--label', 'codex.coordinator.lab=' + run_id, volume)
    state['volumes'].append(volume)
    gitea_name = prefix + '-gitea'
    state['containers'].append(gitea_name); save()
    docker('run', '-d', '--name', gitea_name, '--label', 'codex.coordinator.lab=' + run_id,
           '--network', prefix, '--network-alias', 'gitea', '-v', volume + ':/data',
           '-e', 'GITEA__security__INSTALL_LOCK=true', '-e', 'GITEA__database__DB_TYPE=sqlite3',
           '-e', 'GITEA__server__ROOT_URL=http://gitea:3000/',
           '-e', 'GITEA__server__DOMAIN=gitea', '-e', 'GITEA__server__SSH_DOMAIN=gitea',
           '-e', 'GITEA__service__DISABLE_REGISTRATION=true',
           '-e', 'GITEA__webhook__ALLOWED_HOST_LIST=coordinator,' + state['subnet'],
           GITEA_IMAGE)
    for _ in range(40):
        ready = docker('exec', gitea_name, 'wget', '-qO-', 'http://127.0.0.1:3000/api/v1/version', required=False)
        if ready.returncode == 0:
            state['gitea_version'] = json.loads(ready.stdout)['version']
            break
        time.sleep(.5)
    else:
        raise RuntimeError('gitea_start_timeout')
    docker('exec', '--user', 'git', gitea_name, 'gitea', 'admin', 'user', 'create',
           '--username', 'labadmin', '--password', state['password'], '--email', 'admin@example.invalid',
           '--admin', '--must-change-password=false')
    rt_name = prefix + '-rt'
    state['containers'].append(rt_name); save()
    docker('run', '-d', '--name', rt_name, '--label', 'codex.coordinator.lab=' + run_id,
           '--network', prefix, '--network-alias', 'roundtable', '--network-alias', 'coordinator',
           '-v', str(ROOT) + ':/app:ro', '-v', str(out) + ':/out',
           '--entrypoint', 'sh', PY_IMAGE, '-c',
           'cp /out/roundtable-linux /tmp/roundtable && cp /app/lab/fake_claude.py /tmp/fake-claude && chmod +x /tmp/roundtable /tmp/fake-claude && exec /tmp/roundtable -addr 0.0.0.0:18787 -data /out/rt -claude /tmp/fake-claude -codex /nonexistent -agy /nonexistent -opencode /nonexistent -update-check off')
    state['python_image'] = json.loads(docker('image', 'inspect', PY_IMAGE).stdout)[0]['Id']
    state['gitea_image'] = json.loads(docker('image', 'inspect', GITEA_IMAGE).stdout)[0]['Id']
    save()
    print(json.dumps({'id': run_id, 'gitea_version': state['gitea_version'], 'network_internal': True, 'out': str(out)}))


def test():
    state = json.loads(SESSION.read_text(encoding='utf-8'))
    name = state['prefix'] + '-runner'
    state['containers'].append(name)
    SESSION.write_text(json.dumps(state, indent=2), encoding='utf-8')
    result = docker('run', '--name', name, '--label', 'codex.coordinator.lab=' + state['id'],
                    '--network', 'container:' + state['prefix'] + '-rt',
                    '--pid', 'container:' + state['prefix'] + '-rt',
                    '-v', str(ROOT) + ':/app:ro', '-v', state['out'] + ':/out',
                    '-w', '/app', '--entrypoint', 'python', PY_IMAGE,
                    '-B', '-m', 'lab.integration', required=False)
    print(result.stdout)
    if result.returncode:
        print(result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)


def down():
    state = json.loads(SESSION.read_text(encoding='utf-8'))
    out = Path(state['out'])
    for name in state['containers']:
        inspect = docker('inspect', name, required=False)
        if inspect.returncode:
            continue
        info = json.loads(inspect.stdout)[0]
        if info['Config']['Labels'].get('codex.coordinator.lab') != state['id']:
            raise RuntimeError('container_ownership_mismatch')
        docker('stop', '-t', '5', name)
        (out / (name + '.log')).write_text(docker('logs', name).stdout, encoding='utf-8')
    print(json.dumps({'stopped': state['containers'], 'artifacts': state['out']}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('up', 'test', 'down'))
    parser.add_argument('--rt-binary')
    args = parser.parse_args()
    if args.action == 'up':
        if not args.rt_binary:
            parser.error('up requires --rt-binary from the prior isolated RT build')
        up(args.rt_binary)
    elif args.action == 'test':
        test()
    else:
        down()
