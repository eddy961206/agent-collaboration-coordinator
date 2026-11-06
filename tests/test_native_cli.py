import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

if '--fake' in sys.argv:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coordinator.adapters import AdapterError
from coordinator.native_cli import NativeEngine


def fake_cli(kind):
    session = str(uuid.uuid4())
    turn = None
    def emit(value):
        print(json.dumps(value), flush=True)
    if kind == 'claude':
        args = sys.argv
        option = '--resume' if '--resume' in args else '--session-id'
        session = args[args.index(option) + 1]
        emit({'type': 'system', 'subtype': 'init', 'session_id': session})
    for line in sys.stdin:
        packet = json.loads(line)
        if kind == 'claude':
            emit({**packet, 'session_id': session})
            if 'COMPLETE' in json.dumps(packet):
                emit({'type': 'result', 'session_id': session, 'result': 'fake protocol result', 'is_error': False})
        else:
            method, identity, params = packet.get('method'), packet.get('id'), packet.get('params', {})
            result = {}
            if method == 'thread/start':
                result = {'thread': {'id': session}}
                if '--spawn-child' in sys.argv:
                    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
                    Path('child.pid').write_text(str(child.pid))
            elif method == 'thread/resume':
                session = params['threadId']; result = {'thread': {'id': session}}
            elif method == 'turn/start':
                turn = uuid.uuid4().hex; result = {'turn': {'id': turn}}
            elif method == 'turn/steer':
                result = {'turnId': turn}
            if identity:
                emit({'id': identity, 'result': result})
            if method == 'turn/interrupt' or (method == 'turn/start' and 'COMPLETE' in json.dumps(params)):
                emit({'method': 'item/completed', 'params': {'item': {'type': 'agentMessage', 'text': 'fake protocol result'}}})
                emit({'method': 'turn/completed', 'params': {'turn': {'id': turn, 'status': 'interrupted' if method == 'turn/interrupt' else 'completed'}}})


class NativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = {'mode': 'fixture', 'base_url': 'http://127.0.0.1:19091', 'network_isolated': True, 'timeout': 3}
        self.engines = []

    def tearDown(self):
        for engine in self.engines:
            engine.close()
        self.temp.cleanup()

    def engine(self, kind):
        engine = NativeEngine(kind, [sys.executable, str(Path(__file__).resolve()), '--fake', kind], self.root / kind, self.config)
        self.engines.append(engine)
        return engine

    def run_spec(self, revision=1, task='task-A', text='request'):
        return {'id': uuid.uuid4().hex, 'task_id': task, 'owner': 'alice', 'adapter': 'native',
                'generation': 1, 'revision': revision, 'text': text}

    def wait_stopped(self, engine, handle):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = engine.observe(handle)
            if state['stopped']:
                return state
            time.sleep(.02)
        self.fail('owned native process did not stop')

    def test_claude_change_stop_and_explicit_same_session_resume(self):
        engine = self.engine('claude')
        first = engine.start(self.run_spec())
        self.assertTrue(engine.deliver(first, {'revision': 2, 'text': 'changed'})['delivered'])
        self.assertEqual(2, engine.observe(first)['accepted_revision'])
        stopped = engine.stop(first)
        self.assertTrue(stopped['stopped'])
        second_spec = self.run_spec(3, text='COMPLETE')
        second_spec['resume_session'] = first
        second = engine.start(second_spec)
        done = self.wait_stopped(engine, second)
        self.assertEqual(stopped['session_id'], done['session_id'])
        self.assertEqual('completed', done['status'])

    def test_codex_rpc_steer_interrupt_resume(self):
        engine = self.engine('codex')
        first = engine.start(self.run_spec())
        initial = engine.observe(first)
        self.assertTrue(initial['session_id'])
        self.assertTrue(initial['turn_id'])
        self.assertEqual(2, engine.deliver(first, {'revision': 2, 'text': 'change'})['revision'])
        self.assertTrue(engine.stop(first)['stopped'])
        spec = self.run_spec(3, text='COMPLETE'); spec['resume_session'] = first
        second = engine.start(spec)
        done = self.wait_stopped(engine, second)
        self.assertEqual(initial['session_id'], done['session_id'])
        self.assertNotEqual(initial['turn_id'], done['turn_id'])
        self.assertEqual('completed', done['status'])

    def test_fixture_env_drops_all_inherited_auth_and_proxy(self):
        engine = self.engine('claude')
        with patch.dict(os.environ, {'OPENAI_API_KEY': 'must-not-inherit', 'ANTHROPIC_AUTH_TOKEN': 'must-not-inherit', 'HTTP_PROXY': 'must-not-inherit'}):
            env = engine._environment(self.root)
        self.assertNotIn('OPENAI_API_KEY', env)
        self.assertNotIn('ANTHROPIC_AUTH_TOKEN', env)
        self.assertNotIn('HTTP_PROXY', env)
        self.assertEqual('coordinator-fixture-not-a-real-key', env['ANTHROPIC_API_KEY'])

    def test_configuration_requires_explicit_isolation_or_live_opt_in(self):
        for config in ({}, {'mode': 'fixture', 'base_url': 'https://api.anthropic.com', 'network_isolated': True},
                       {'mode': 'fixture', 'base_url': 'http://127.0.0.1:1'},
                       {'mode': 'live', 'base_url': 'https://api.example.invalid'}):
            with self.assertRaises(AdapterError):
                NativeEngine('claude', sys.executable, self.root, config)
        with self.assertRaisesRegex(AdapterError, 'explicit_live_model_required'):
            NativeEngine('codex', sys.executable, self.root,
                         {'mode': 'live', 'allow_model_calls': True, 'base_url': 'https://api.example.invalid'})

    def test_resume_rejects_foreign_task_and_running_previous_session(self):
        engine = self.engine('claude')
        first = engine.start(self.run_spec())
        spec = self.run_spec(); spec['resume_session'] = first
        with self.assertRaises(AdapterError):
            engine.start(spec)
        engine.stop(first)
        spec = self.run_spec(task='task-B'); spec['resume_session'] = first
        with self.assertRaises(AdapterError):
            engine.start(spec)

    def test_handle_and_checkpoint_are_scoped_to_owned_run(self):
        engine = self.engine('codex')
        handle = engine.start(self.run_spec())
        changed = {**handle, 'owner_token': 'not-owned'}
        with self.assertRaises(AdapterError):
            engine.stop(changed)
        checkpoint = self.root / 'codex' / 'runs' / handle['run_id'] / 'native-state.json'
        value = json.loads(checkpoint.read_text())
        self.assertEqual(engine.observe(handle)['session_id'], value['observation']['session_id'])

    def test_stop_confirms_prompt_created_child_tree(self):
        engine = NativeEngine('codex', [sys.executable, str(Path(__file__).resolve()), '--fake', 'codex', '--spawn-child'], self.root / 'tree', self.config)
        self.engines.append(engine)
        handle = engine.start(self.run_spec())
        item = engine._item(handle)
        self.assertTrue((item['workspace'] / 'child.pid').is_file())
        self.assertTrue(engine.stop(handle)['stopped'])
        self.assertTrue(engine._tree_stopped(item))

    def test_repeated_old_stop_does_not_touch_resumed_execution(self):
        engine = self.engine('codex')
        first = engine.start(self.run_spec())
        engine.stop(first)
        spec = self.run_spec(2); spec['resume_session'] = first
        second = engine.start(spec)
        self.assertTrue(engine.stop(first)['stopped'])
        self.assertFalse(engine.observe(second)['stopped'])
        self.assertEqual('running', engine.observe(second)['status'])


if __name__ == '__main__':
    if '--fake' in sys.argv:
        fake_cli(sys.argv[sys.argv.index('--fake') + 1])
    else:
        unittest.main()
