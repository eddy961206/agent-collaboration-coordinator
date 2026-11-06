"""Durable worker RPC with real fixture processes and no model calls."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

from coordinator.adapters import AdapterError, DirectAdapter
from coordinator.remote import RemoteAdapter, canonical_json, handle_for
from coordinator.worker_host import WorkerHost, make_worker_server


def run(identity='run-a', task='fake/team#1', owner='alice', revision=1, generation=1, **extra):
    return {'id': identity, 'task_id': task, 'owner': owner, 'adapter': 'pc-a-direct',
            'revision': revision, 'generation': generation, 'text': 'request ' + str(revision), **extra}


class CountingDirect(DirectAdapter):
    def __init__(self, root):
        super().__init__(root)
        self.starts = 0
        self.deliveries = 0
        self.stops = 0
        self.start_inputs = []

    def start(self, value):
        self.starts += 1
        self.start_inputs.append(value)
        return super().start(value)

    def deliver(self, handle, snapshot):
        self.deliveries += 1
        return super().deliver(handle, snapshot)

    def stop(self, handle):
        self.stops += 1
        return super().stop(handle)


class WorkerHostTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.engine = CountingDirect(self.root / 'workspaces')
        self.host = WorkerHost(self.root / 'state', 'pc-a', 'fixture-node-key', {'direct': self.engine}, ['alice', 'bob'])
        self.server = make_worker_server(self.host, ('127.0.0.1', 0))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_address[1])
        self.remote = RemoteAdapter(self.base, 'pc-a', 'fixture-node-key', 'direct', ['alice', 'bob'])

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def tearDown(self):
        self.stop_server()
        self.host.close()
        self.engine.close()
        self.temp.cleanup()

    def raw(self, path, body=None, token='fixture-node-key', node='pc-a'):
        request = Request(self.base + path,
                          canonical_json(body).encode() if body is not None else None,
                          {'Authorization': 'Bearer ' + token, 'X-Worker-Node': node,
                           'Content-Type': 'application/json'}, method='POST' if body is not None else 'GET')
        try:
            with urlopen(request, timeout=8) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_prepare_is_read_only_and_new_client_recovers_same_actual_process(self):
        prepared = self.remote.prepare(run())
        self.assertEqual(0, self.engine.starts)
        with self.host.db() as db:
            self.assertEqual(0, db.execute('SELECT count(*) FROM jobs').fetchone()[0])
        handle = self.remote.start_prepared(run(), prepared)
        first = self.remote.observe(handle)
        replacement = RemoteAdapter(self.base, 'pc-a', 'fixture-node-key', 'direct', ['alice'])
        self.remote.close()
        recovered = replacement.recover(handle)
        self.assertEqual(first['children'][0]['pid'], recovered['children'][0]['pid'])
        self.assertFalse(recovered['stopped'])
        self.assertEqual(1, self.engine.starts)
        self.assertEqual(handle['ledger_id'], recovered['ledger_id'])

    def test_concurrent_duplicate_start_has_one_process(self):
        handle = self.remote.prepare(run())
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda _: self.remote.start_prepared(run(), handle), range(8)))
        self.assertTrue(all(x == handle for x in values))
        self.assertEqual(1, self.engine.starts)

    def test_same_task_cannot_start_second_active_job(self):
        self.remote.start(run())
        with self.assertRaisesRegex(AdapterError, 'worker_task_already_active'):
            self.remote.start(run('run-b', generation=2))
        self.assertEqual(1, self.engine.starts)
        other = self.remote.start(run('run-c', task='fake/team#2', owner='bob'))
        self.assertFalse(self.remote.observe(other)['stopped'])
        self.assertEqual(2, self.engine.starts)

    def test_changed_payload_with_same_run_id_is_rejected(self):
        self.remote.start(run())
        with self.assertRaisesRegex(AdapterError, 'worker_operation_collision'):
            self.remote.start(run(revision=2))
        self.assertEqual(1, self.engine.starts)

    def test_duplicate_delivery_changes_worker_once(self):
        handle = self.remote.start(run())
        snapshot = {'revision': 2, 'text': 'changed'}
        first = self.remote.deliver(handle, snapshot)
        second = self.remote.deliver(handle, snapshot)
        self.assertEqual(first, second)
        self.assertEqual(1, self.engine.deliveries)
        observation = self.remote.observe(handle)
        self.assertEqual([1, 2], observation['revisions'])
        self.assertEqual(2, observation['accepted_revision'])

    def test_stop_resume_and_late_old_stop_do_not_stop_new_run(self):
        old = self.remote.start(run())
        self.assertTrue(self.remote.stop(old)['stopped'])
        fresh = self.remote.start(run('run-b', generation=3, revision=2, resume_from='run-a'))
        self.assertIn('resume_session', self.engine.start_inputs[-1])
        self.assertEqual('run-a', self.engine.start_inputs[-1]['resume_session']['run_id'])
        stops = self.engine.stops
        self.assertTrue(self.remote.stop(old)['stopped'])
        self.assertEqual(stops, self.engine.stops)
        self.assertFalse(self.remote.observe(fresh)['stopped'])

    def test_resume_from_other_task_or_running_job_is_rejected(self):
        old = self.remote.start(run())
        with self.assertRaisesRegex(AdapterError, 'resume_source_not_stopped'):
            self.remote.start(run('premature', resume_from='run-a', generation=2))
        self.remote.stop(old)
        with self.assertRaisesRegex(AdapterError, 'resume_source_mismatch'):
            self.remote.start(run('foreign', task='fake/team#9', resume_from='run-a'))

    def test_auth_node_owner_and_proof_are_enforced(self):
        self.assertEqual(401, self.raw('/v1/info', token='wrong')[0])
        self.assertEqual(403, self.raw('/v1/info', node='other')[0])
        with self.assertRaisesRegex(AdapterError, 'worker_owner_not_allowed'):
            self.remote.prepare(run(owner='outsider'))
        host_handle = handle_for(run(owner='outsider'), 'pc-a', 'fixture-node-key', 'direct', self.host.ledger_id)
        status, result = self.raw('/v1/start', {'handle': host_handle, 'run': run(owner='outsider'), 'operation_id': 'outsider'})
        self.assertEqual(403, status)
        self.assertEqual('worker_owner_not_allowed', result['error'])
        handle = self.remote.start(run())
        status, result = self.raw('/v1/stop', {'handle': {**handle, 'run_id': 'foreign'}, 'operation_id': 'stop-foreign'})
        self.assertEqual('remote_handle_proof_invalid', result['error'])
        self.assertFalse(self.remote.observe(handle)['stopped'])

    def test_not_found_is_distinct_from_unknown(self):
        handle = self.remote.prepare(run())
        with self.assertRaisesRegex(AdapterError, 'remote_run_not_found'):
            self.remote.recover(handle)
        self.remote.start_prepared(run(), handle)
        with self.host.db(True) as db:
            db.execute("UPDATE jobs SET status='unknown' WHERE run_id='run-a'")
        observed = self.remote.recover(handle)
        self.assertEqual('unknown', observed['status'])
        self.assertFalse(observed['stopped'])
        with self.assertRaisesRegex(AdapterError, 'worker_launch_unknown'):
            self.remote.start_prepared(run(), handle)
        self.assertEqual(1, self.engine.starts)

    def test_worker_restart_preserves_ledger_but_does_not_claim_old_process(self):
        handle = self.remote.start(run())
        old_instance = self.host.instance_id
        ledger = self.host.ledger_id
        self.stop_server()
        self.host.close(stop_engines=False)
        self.host = WorkerHost(self.root / 'state', 'pc-a', 'fixture-node-key', {'direct': self.engine}, ['alice'])
        self.server = make_worker_server(self.host, ('127.0.0.1', 0))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.remote = RemoteAdapter('http://127.0.0.1:' + str(self.server.server_address[1]), 'pc-a', 'fixture-node-key', 'direct', ['alice'])
        observation = self.remote.recover(handle)
        self.assertEqual(ledger, observation['ledger_id'])
        self.assertNotEqual(old_instance, observation['node_instance_id'])
        self.assertEqual('unknown', observation['status'])
        self.assertEqual(1, self.engine.starts)

    def test_replaced_ledger_rejects_old_prepared_handle_before_launch(self):
        handle = self.remote.prepare(run())
        self.stop_server()
        self.host.close()
        self.host = WorkerHost(self.root / 'new-state', 'pc-a', 'fixture-node-key', {'direct': self.engine}, ['alice'])
        self.server = make_worker_server(self.host, ('127.0.0.1', 0))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.remote = RemoteAdapter('http://127.0.0.1:' + str(self.server.server_address[1]), 'pc-a', 'fixture-node-key', 'direct', ['alice'])
        with self.assertRaisesRegex(AdapterError, 'remote_ledger_changed'):
            self.remote.recover(handle)
        with self.assertRaisesRegex(AdapterError, 'remote_ledger_changed'):
            self.remote.start_prepared(run(), handle)
        self.assertEqual(0, self.engine.starts)

    def test_ambiguous_delivery_is_not_retried(self):
        handle = self.remote.start(run())
        original = self.engine.deliver
        def accepted_then_lost(engine_handle, snapshot):
            original(engine_handle, snapshot)
            raise TimeoutError('response lost after actual fixture input')
        with patch.object(self.engine, 'deliver', side_effect=accepted_then_lost):
            with self.assertRaises(AdapterError):
                self.remote.deliver(handle, {'revision': 2, 'text': 'changed'})
        with self.assertRaisesRegex(AdapterError, 'worker_command_unknown'):
            self.remote.deliver(handle, {'revision': 2, 'text': 'changed'})
        self.assertEqual([1, 2], self.remote.observe(handle)['revisions'])
        self.assertEqual(1, self.engine.deliveries)

    def test_native_engine_checkpoint_is_durable_in_host_ledger(self):
        handle = self.remote.start(run())
        with self.host.db() as db:
            row = db.execute('SELECT engine_handle,launch FROM jobs WHERE run_id=?', (handle['run_id'],)).fetchone()
        checkpoint = json.loads(row['engine_handle'])
        self.assertEqual('run-a', checkpoint['run_id'])
        self.assertEqual('alice', json.loads(row['launch'])['owner'])
        self.assertGreater(checkpoint['pid'], 0)

    def test_revision_and_task_identity_cannot_be_bypassed_with_new_operation_id(self):
        handle = self.remote.start(run())
        self.remote.deliver(handle, {'revision': 3, 'text': 'latest'})
        with self.assertRaisesRegex(AdapterError, 'stale_worker_revision'):
            self.remote.deliver(handle, {'revision': 2, 'text': 'old'})
        with self.assertRaisesRegex(AdapterError, 'worker_revision_conflict'):
            self.remote.deliver(handle, {'revision': 3, 'text': 'different same revision'})
        with self.assertRaisesRegex(AdapterError, 'worker_snapshot_identity_mismatch'):
            self.remote.deliver(handle, {'revision': 4, 'text': 'foreign', 'task_id': 'another-task'})
        self.assertEqual([1, 3], self.remote.observe(handle)['revisions'])
        self.assertEqual(1, self.engine.deliveries)

    def test_unknown_stop_can_retry_same_owned_target(self):
        handle = self.remote.start(run())
        original = self.engine.stop
        with patch.object(self.engine, 'stop', side_effect=TimeoutError('temporary adapter outage')):
            with self.assertRaises(AdapterError):
                self.remote.stop(handle)
        self.assertTrue(self.remote.stop(handle)['stopped'])
        self.assertEqual(1, self.engine.stops)

    def test_unknown_delivery_cannot_be_bypassed_by_another_id(self):
        handle = self.remote.start(run())
        original = self.engine.deliver
        def accepted_then_lost(engine_handle, snapshot):
            original(engine_handle, snapshot)
            raise TimeoutError('lost')
        with patch.object(self.engine, 'deliver', side_effect=accepted_then_lost):
            with self.assertRaises(AdapterError):
                self.remote.deliver(handle, {'revision': 2, 'text': 'changed'})
        status, reply = self.raw('/v1/deliver', {'handle': handle, 'snapshot': {'revision': 3, 'text': 'new'}, 'operation_id': 'bypass'})
        self.assertEqual(409, status)
        self.assertEqual('worker_delivery_unconfirmed', reply['error'])
        self.assertEqual(1, self.engine.deliveries)
        self.assertTrue(self.remote.stop(handle)['stopped'])

    def test_partial_output_from_failed_engine_is_not_completed(self):
        handle = self.remote.start(run())
        job = self.host._job(handle)
        self.engine.complete(job['engine_handle'])
        original = self.engine.observe
        def failed(engine_handle):
            return {**original(engine_handle), 'status': 'failed', 'result': 'partial output before failure'}
        with patch.object(self.engine, 'observe', side_effect=failed):
            observation = self.remote.observe(handle)
        self.assertTrue(observation['stopped'])
        self.assertEqual('failed', observation['status'])
        self.assertEqual('failed', self.remote.recover(handle)['status'])
        with self.host.db() as db:
            row = db.execute('SELECT status FROM jobs WHERE run_id=?', (handle['run_id'],)).fetchone()
        self.assertEqual('stopped', row['status'])

    def test_worker_bind_and_transport_require_local_or_tls(self):
        with self.assertRaisesRegex(ValueError, 'worker_bind_loopback_only'):
            make_worker_server(self.host, ('0.0.0.0', 0))
        with self.assertRaisesRegex(AdapterError, 'worker_requires_ssh_tunnel_or_tls'):
            RemoteAdapter('http://192.0.2.5:18882', 'pc-a', 'key', 'direct', ['alice'])
        with self.assertRaisesRegex(AdapterError, 'worker_endpoint_invalid'):
            RemoteAdapter('http://user:pass@127.0.0.1:18882', 'pc-a', 'key', 'direct', ['alice'])

    def test_separate_controller_process_recovers_persisted_handle(self):
        handle = self.remote.start(run())
        saved = self.root / 'handle.json'
        saved.write_text(json.dumps(handle), encoding='utf-8')
        code = ('import json,sys; from coordinator.remote import RemoteAdapter; '
                'h=json.load(open(sys.argv[2])); a=RemoteAdapter(sys.argv[1],"pc-a","fixture-node-key","direct",["alice"]); '
                'print(json.dumps(a.recover(h))); a.close()')
        result = subprocess.run([sys.executable, '-c', code, self.base, str(saved)],
                                capture_output=True, text=True, encoding='utf-8', timeout=15,
                                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertFalse(json.loads(result.stdout)['stopped'])
        self.assertEqual(1, self.engine.starts)
        self.assertFalse(self.remote.observe(handle)['stopped'])


if __name__ == '__main__':
    unittest.main()
