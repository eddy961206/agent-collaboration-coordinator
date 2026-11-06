"""Owned Claude Code and Codex processes, independent of coordinator lifetime.

Fixture mode is for an externally network-isolated environment with local fake
providers. It never reads an existing CLI home or uses inherited credentials.
"""
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time
from urllib.parse import urlsplit
import uuid

from .adapters import AdapterError, snapshot_payload


class _WindowsJob:
    """Assign before any prompt is sent; prompt-created children join this job."""
    def __init__(self, process):
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateJobObjectW.restype = ctypes.c_void_p
        kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
        kernel.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        kernel.TerminateJobObject.argtypes = [ctypes.c_void_p, ctypes.c_uint]
        kernel.QueryInformationJobObject.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint, ctypes.c_void_p]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        self.kernel = kernel
        self.handle = kernel.CreateJobObjectW(None, None)
        if not self.handle or not kernel.AssignProcessToJobObject(self.handle, int(process._handle)):
            if self.handle:
                kernel.CloseHandle(self.handle)
            raise AdapterError('native_job_assignment_failed')

    def terminate(self):
        if self.handle and not self.kernel.TerminateJobObject(self.handle, 1):
            raise AdapterError('native_job_stop_failed')

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None

    def empty(self):
        class Accounting(ctypes.Structure):
            _fields_ = [('times', ctypes.c_longlong * 4), ('page_faults', ctypes.c_uint),
                        ('total_processes', ctypes.c_uint), ('active_processes', ctypes.c_uint),
                        ('terminated_processes', ctypes.c_uint)]
        info = Accounting()
        if not self.kernel.QueryInformationJobObject(self.handle, 1, ctypes.byref(info), ctypes.sizeof(info), None):
            raise AdapterError('native_job_state_unconfirmed')
        return info.active_processes == 0


class NativeEngine:
    def __init__(self, kind, executable, work_root, provider_config, checkpoint_callback=None):
        if kind not in ('claude', 'codex'):
            raise AdapterError('invalid_native_kind')
        command = list(executable) if isinstance(executable, (list, tuple)) else [str(executable)]
        if not command or not Path(command[0]).is_absolute() or not Path(command[0]).is_file():
            raise AdapterError('absolute_native_executable_required')
        mode = provider_config.get('mode')
        if mode not in ('fixture', 'live'):
            raise AdapterError('explicit_provider_mode_required')
        if mode == 'live' and provider_config.get('allow_model_calls') is not True:
            raise AdapterError('live_model_calls_not_enabled')
        if mode == 'live' and not provider_config.get('model'):
            raise AdapterError('explicit_live_model_required')
        base = urlsplit(provider_config.get('base_url', ''))
        if (base.scheme not in ('http', 'https') or not base.hostname or base.username or base.password
                or base.query or base.fragment):
            raise AdapterError('invalid_provider_endpoint')
        if mode == 'fixture' and (provider_config.get('network_isolated') is not True
                or base.scheme != 'http' or base.hostname not in provider_config.get('allowed_hosts', ['127.0.0.1', 'localhost', '::1'])):
            raise AdapterError('isolated_fixture_endpoint_required')
        self.kind, self.command, self.config = kind, command, dict(provider_config)
        self.root = Path(work_root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.callback = checkpoint_callback
        self.timeout = float(provider_config.get('timeout', 20))
        self.items = {}
        self.registry_lock = threading.RLock()

    def _environment(self, home):
        environment = {k: os.environ[k] for k in ('SystemRoot', 'WINDIR') if k in os.environ}
        environment.update({'HOME': str(home), 'USERPROFILE': str(home), 'TMP': str(home / 'tmp'),
                            'TEMP': str(home / 'tmp'), 'TMPDIR': str(home / 'tmp'),
                            'PATH': os.defpath, 'NO_PROXY': '*', 'PYTHONUTF8': '1',
                            'CLAUDE_CONFIG_DIR': str(home / 'claude'), 'CODEX_HOME': str(home / 'codex'),
                            'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC': '1', 'DISABLE_TELEMETRY': '1',
                            'DISABLE_ERROR_REPORTING': '1', 'DISABLE_AUTOUPDATER': '1'})
        key = 'coordinator-fixture-not-a-real-key'
        if self.config['mode'] == 'live':
            variable = self.config.get('api_key_env')
            if not variable or not os.environ.get(variable):
                raise AdapterError('configured_live_credential_missing')
            key = os.environ[variable]
        if self.kind == 'claude':
            environment['ANTHROPIC_BASE_URL'] = self.config['base_url'].rstrip('/')
            environment['ANTHROPIC_API_KEY'] = key
        else:
            environment['COORDINATOR_PROVIDER_KEY'] = key
        return environment

    def _arguments(self, session, resume):
        if self.kind == 'claude':
            tools = self.config.get('tools', '') if self.config['mode'] == 'live' else ''
            return ['--bare', '-p', '--input-format', 'stream-json', '--output-format', 'stream-json',
                    '--verbose', '--replay-user-messages', '--strict-mcp-config', '--mcp-config', '{"mcpServers":{}}',
                    '--setting-sources', '', '--disable-slash-commands', '--tools', tools,
                    '--permission-mode', 'dontAsk', '--model', self.config.get('model', 'claude-sonnet-4-5'),
                    '--resume' if resume else '--session-id', session]
        config = {
            'model_provider': 'coordinator', 'model': self.config.get('model', 'gpt-5.2-codex'),
            'model_providers.coordinator.name': 'Coordinator explicit provider',
            'model_providers.coordinator.base_url': self.config['base_url'].rstrip('/'),
            'model_providers.coordinator.env_key': 'COORDINATOR_PROVIDER_KEY',
            'model_providers.coordinator.wire_api': 'responses',
            'model_providers.coordinator.requires_openai_auth': False,
            'model_providers.coordinator.supports_websockets': False,
            'model_providers.coordinator.request_max_retries': 0,
            'model_providers.coordinator.stream_max_retries': 0,
            'features.plugins': False, 'features.apps': False, 'web_search': 'disabled',
            'analytics.enabled': False,
        }
        args = ['app-server']
        for name, value in config.items():
            args += ['-c', name + '=' + json.dumps(value)]
        return args

    def _item(self, handle):
        item = self.items.get(handle.get('run_id'))
        if not item or item['handle'] != handle:
            raise AdapterError('unknown_owned_native_run')
        return item

    def _save(self, item):
        path = item['directory'] / 'native-state.json'
        temp = path.with_suffix('.tmp')
        with item['checkpoint_lock']:
            observation = self._observation(item)
            with temp.open('w', encoding='utf-8') as stream:
                json.dump({'handle': item['handle'], 'observation': observation}, stream, ensure_ascii=False)
                stream.flush(); os.fsync(stream.fileno())
            temp.replace(path)
            if self.callback:
                self.callback(dict(item['handle']), observation)

    def _write(self, item, value):
        with item['write_lock']:
            if item['process'].poll() is not None:
                raise AdapterError('native_process_exited')
            try:
                item['process'].stdin.write(json.dumps(value, ensure_ascii=False) + '\n')
                item['process'].stdin.flush()
            except (OSError, ValueError) as exc:
                raise AdapterError('native_stdin_failed') from exc

    def _rpc(self, item, method, params):
        identity = uuid.uuid4().hex
        self._write(item, {'id': identity, 'method': method, 'params': params})
        deadline = time.monotonic() + self.timeout
        with item['condition']:
            while identity not in item['responses']:
                if item['process'].poll() is not None:
                    raise AdapterError('native_rpc_process_exited')
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AdapterError('native_rpc_timeout')
                item['condition'].wait(min(remaining, .2))
            packet = item['responses'].pop(identity)
        if 'error' in packet:
            raise AdapterError('native_rpc_rejected:' + str(packet['error'].get('code')))
        return packet.get('result', {})

    def _on_packet(self, item, packet):
        if self.kind == 'codex':
            if 'id' in packet and not packet.get('method'):
                with item['condition']:
                    item['responses'][packet['id']] = packet
                    item['condition'].notify_all()
                return
            if packet.get('method') and 'id' in packet:
                self._write(item, {'id': packet['id'], 'error': {'code': -32000, 'message': 'Host has not granted this tool or approval request'}})
                return
            method, params = packet.get('method'), packet.get('params', {})
            if method == 'turn/completed':
                item['turn_status'] = params.get('turn', {}).get('status', 'unknown')
                item['status'] = 'completed' if item['turn_status'] == 'completed' else 'stopped' if item['turn_status'] == 'interrupted' else 'failed'
                item['turn_done'].set()
                # Complete this owned app-server process after persisting the turn.
                self._complete_later(item)
            elif method == 'item/completed' and params.get('item', {}).get('type') == 'agentMessage':
                item['result'] = params['item'].get('text', '')
            elif method == 'thread/started':
                item['session_id'] = params.get('thread', {}).get('id', item['session_id'])
        else:
            if packet.get('session_id'):
                item['session_id'] = packet['session_id']
            if packet.get('type') == 'command_lifecycle' and packet.get('command_uuid') in item['pending_inputs']:
                identity = packet['command_uuid']
                state = packet.get('state')
                item['delivery_receipts'][identity] = state
                if state == 'started':
                    item['accepted_revision'] = max(item['accepted_revision'], item['pending_inputs'].pop(identity))
                with item['condition']:
                    item['condition'].notify_all()
            if packet.get('type') == 'system' and packet.get('subtype') == 'init':
                item['initialized'] = True
                item['ready'].set()
            if packet.get('type') == 'user' and packet.get('uuid') in item['pending_inputs']:
                identity = packet['uuid']
                revision = item['pending_inputs'].pop(identity)
                item['delivery_receipts'][identity] = 'started'
                item['accepted_revision'] = max(item['accepted_revision'], revision)
                with item['condition']:
                    item['condition'].notify_all()
            if packet.get('type') == 'result':
                item['result'] = packet.get('result', '')
                if packet.get('is_error') or not item['pending_inputs']:
                    item['status'] = 'failed' if packet.get('is_error') else 'completed'
                    item['turn_done'].set()
                    self._complete_later(item)
        self._save(item)

    def _complete_later(self, item):
        thread = threading.Thread(target=self._end_process, args=(item,), daemon=True)
        item['end_threads'].append(thread)
        thread.start()

    def _read(self, item):
        try:
            for line in item['process'].stdout:
                with (item['directory'] / 'native-events.jsonl').open('a', encoding='utf-8') as stream:
                    stream.write(line)
                try:
                    packet = json.loads(line)
                except ValueError:
                    item['protocol_error'] = 'invalid_native_json'
                    continue
                self._on_packet(item, packet)
        except Exception as exc:
            item['protocol_error'] = type(exc).__name__
        finally:
            item['ready'].set()
            with item['condition']:
                item['condition'].notify_all()

    def _end_process(self, item):
        with item['end_lock']:
            if item['process_exited'].is_set():
                return
            process = item['process']
            if process.poll() is None:
                try:
                    process.stdin.close()
                    process.wait(timeout=.5)
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    if os.name == 'nt':
                        item['job'].terminate()
                    else:
                        os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        if os.name == 'nt':
                            item['job'].terminate()
                        else:
                            os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=3)
            # Kill remaining prompt-created descendants in this owned group/job.
            if os.name == 'nt':
                item['job'].terminate()
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            deadline = time.monotonic() + 3
            while not self._tree_stopped(item):
                if time.monotonic() >= deadline:
                    raise AdapterError('native_children_stop_unconfirmed')
                time.sleep(.02)
            item['process_exited'].set()
            self._save(item)

    @staticmethod
    def _tree_stopped(item):
        if os.name == 'nt':
            return item['job'].empty()
        group = item['process'].pid
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return True
        # A reaped parent can leave killed zombie descendants until PID 1 reaps
        # them. Zombies cannot execute work; distinguish them from live children.
        proc = Path('/proc')
        if proc.is_dir():
            for path in proc.glob('[0-9]*/stat'):
                try:
                    fields = path.read_text().rsplit(')', 1)[1].split()
                    if int(fields[2]) == group and fields[0] != 'Z':
                        return False
                except (OSError, ValueError, IndexError):
                    continue
            return True
        return False

    def start(self, run):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', run['id']):
            raise AdapterError('invalid_run_id')
        payload = snapshot_payload(run)
        with self.registry_lock:
            if run['id'] in self.items:
                raise AdapterError('native_run_already_exists')
            if any(i['task_id'] == run['task_id'] and not self._observation(i)['stopped'] for i in self.items.values()):
                raise AdapterError('native_task_still_running')
            prior = run.get('resume_session')
            session = None
            if prior:
                previous = self._item(prior)
                if previous['task_id'] != run['task_id'] or not self._observation(previous)['stopped']:
                    raise AdapterError('native_resume_not_owned_or_stopped')
                session = previous['session_id']
                if not session:
                    raise AdapterError('native_session_not_recorded')
            task_root = self.root / 'tasks' / hashlib.sha256(run['task_id'].encode()).hexdigest()[:24]
            home, workspace = task_root / 'home', task_root / 'workspace'
            directory = self.root / 'runs' / run['id']
            directory.mkdir(parents=True)
            for path in (home / 'tmp', home / 'claude', home / 'codex', workspace):
                path.mkdir(parents=True, exist_ok=True)
            session = session or (str(uuid.uuid4()) if self.kind == 'claude' else None)
            environment = self._environment(home)
            stderr = (directory / 'native-stderr.log').open('w', encoding='utf-8')
            process = subprocess.Popen(self.command + self._arguments(session, bool(prior)),
                cwd=workspace, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=stderr, text=True, encoding='utf-8', bufsize=1, start_new_session=os.name != 'nt',
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            try:
                job = _WindowsJob(process) if os.name == 'nt' else None
            except Exception:
                process.kill(); process.wait(); stderr.close()
                raise
            handle = {'adapter': 'native', 'kind': self.kind, 'run_id': run['id'],
                      'task_id': run['task_id'], 'owner_token': uuid.uuid4().hex}
            item = {'handle': handle, 'task_id': run['task_id'], 'directory': directory,
                    'workspace': workspace, 'home': home, 'session_id': session,
                    'turn_id': None, 'turn_status': None, 'result': None, 'process': process,
                    'job': job, 'stderr': stderr, 'status': 'running', 'accepted_revision': 0,
                    'initialized': False,
                    'end_threads': [],
                    'responses': {}, 'pending_inputs': {}, 'delivery_receipts': {}, 'condition': threading.Condition(),
                    'write_lock': threading.Lock(), 'end_lock': threading.Lock(), 'checkpoint_lock': threading.Lock(),
                    'ready': threading.Event(), 'turn_done': threading.Event(), 'process_exited': threading.Event()}
            self.items[run['id']] = item
            reader = threading.Thread(target=self._read, args=(item,), daemon=True)
            item['reader'] = reader
            self._save(item); reader.start()
        try:
            if self.kind == 'codex':
                self._rpc(item, 'initialize', {'clientInfo': {'name': 'isolated_coordinator', 'version': '1.0'}, 'capabilities': {'experimentalApi': True}})
                self._write(item, {'method': 'initialized', 'params': {}})
                params = {'cwd': str(workspace), 'approvalPolicy': 'never',
                          'sandbox': 'read-only' if self.config['mode'] == 'fixture' else self.config.get('sandbox', 'workspace-write')}
                method = 'thread/resume' if prior else 'thread/start'
                if prior:
                    params['threadId'] = session
                thread = self._rpc(item, method, params)
                item['session_id'] = thread['thread']['id']
                self._save(item)
                response = self._rpc(item, 'turn/start', {'threadId': item['session_id'], 'input': [{'type': 'text', 'text': self._prompt(payload)}]})
                item['turn_id'] = response['turn']['id']
                item['accepted_revision'] = payload['revision']
            else:
                identity = uuid.uuid4().hex
                item['pending_inputs'][identity] = payload['revision']
                self._write(item, {'type': 'user', 'uuid': identity, 'message': {'role': 'user', 'content': self._prompt(payload)}})
                if not item['ready'].wait(self.timeout):
                    raise AdapterError('native_start_timeout')
                if not item['initialized']:
                    raise AdapterError('native_start_failed')
            self._save(item)
            return dict(handle)
        except Exception as exc:
            exc.handle = dict(handle)
            raise

    @staticmethod
    def _prompt(payload):
        return 'COORDINATOR_REQUEST ' + json.dumps(payload, ensure_ascii=False)

    def deliver(self, handle, snapshot):
        item, payload = self._item(handle), snapshot_payload(snapshot)
        if self._observation(item)['stopped'] or item['turn_done'].is_set():
            raise AdapterError('native_run_not_active')
        if payload['revision'] < item['accepted_revision']:
            raise AdapterError('stale_native_revision')
        if self.kind == 'codex':
            response = self._rpc(item, 'turn/steer', {'threadId': item['session_id'], 'expectedTurnId': item['turn_id'],
                                 'clientUserMessageId': uuid.uuid4().hex,
                                 'input': [{'type': 'text', 'text': self._prompt(payload)}]})
            if response.get('turnId') != item['turn_id']:
                raise AdapterError('native_turn_identity_mismatch')
            item['accepted_revision'] = payload['revision']
        else:
            identity = uuid.uuid4().hex
            item['pending_inputs'][identity] = payload['revision']
            self._write(item, {'type': 'user', 'uuid': identity, 'message': {'role': 'user', 'content': self._prompt(payload)}})
            deadline = time.monotonic() + self.timeout
            with item['condition']:
                while identity not in item['delivery_receipts']:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or item['process'].poll() is not None:
                        raise AdapterError('native_delivery_unconfirmed')
                    item['condition'].wait(min(remaining, .2))
        self._save(item)
        return {'delivered': True, 'revision': payload['revision'],
                'delivery_state': item['delivery_receipts'][identity] if self.kind == 'claude' else 'accepted',
                'acknowledgement': 'native_transport_only',
                'session_id': item['session_id'], 'turn_id': item['turn_id']}

    def stop(self, handle):
        item = self._item(handle)
        if self.kind == 'codex' and item['turn_id'] and not item['turn_done'].is_set() and item['process'].poll() is None:
            self._rpc(item, 'turn/interrupt', {'threadId': item['session_id'], 'turnId': item['turn_id']})
            item['turn_done'].wait(self.timeout)
        if item['status'] == 'running':
            item['status'] = 'stopped'
        self._end_process(item)
        return self.observe(handle)

    def _observation(self, item):
        stopped = item['process'].poll() is not None and item['process_exited'].is_set()
        return {'stopped': stopped, 'status': item['status'] if stopped or item['process'].poll() is None else 'unknown',
                'session_id': item['session_id'], 'turn_id': item['turn_id'], 'accepted_revision': item['accepted_revision'],
                'result': item['result'], 'pid': item['process'].pid,
                'children': [{'pid': item['process'].pid, 'status': 'stopped' if stopped else 'running_or_unconfirmed'}],
                'acknowledgement': 'native_transport_only', 'mode': self.config['mode']}

    def observe(self, handle):
        return self._observation(self._item(handle))

    def close(self):
        errors = []
        for item in list(self.items.values()):
            try:
                self.stop(item['handle'])
            except Exception as exc:
                errors.append(getattr(exc, 'code', type(exc).__name__))
            finally:
                item['reader'].join(timeout=2)
                for thread in item['end_threads']:
                    thread.join(timeout=5)
                item['process'].stdout.close()
                if not item['process'].stdin.closed:
                    item['process'].stdin.close()
                item['stderr'].close()
                if item['job']:
                    item['job'].close()
        if errors:
            raise AdapterError('native_cleanup_unconfirmed:' + ','.join(errors))
