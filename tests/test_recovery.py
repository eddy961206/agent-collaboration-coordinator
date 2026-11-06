"""Recovery with a living worker host and new coordinator objects/adapters.

The separate-process crash test lives in the distributed integration lab.
"""
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from coordinator.adapters import AdapterError
from coordinator.remote import RemoteAdapter
from coordinator.runtime import Runtime
from coordinator.store import CoordinationError
from coordinator.worker_host import WorkerHost, make_worker_server
from test_runtime import FakeGitea
from test_worker_host import CountingDirect


class OfflineRemote(RemoteAdapter):
    def _request(self, *args, **kwargs):
        raise AdapterError('worker_unreachable')


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.engine = CountingDirect(self.root / 'workers')
        self.host = WorkerHost(self.root / 'node', 'fixture-pc', 'fixture-node-secret',
                               {'direct': self.engine}, ['alice', 'bob'])
        self.node_server = make_worker_server(self.host, ('127.0.0.1', 0))
        self.node_thread = threading.Thread(target=self.node_server.serve_forever, daemon=True)
        self.node_thread.start()
        self.gitea = FakeGitea()
        self.actors = {'alice': {'role': 'operator', 'gitea_id': 1},
                       'bob': {'role': 'operator', 'gitea_id': 2}}
        self.runtime = self.new_runtime()
        self.task_id = 'fake/team#1'
        self.runtime.store.create_task(self.task_id, 'fake/team', 1, 'alice', 'remote-a', 'initial request', 'bob')

    def remote(self, offline=False):
        cls = OfflineRemote if offline else RemoteAdapter
        return cls('http://127.0.0.1:' + str(self.node_server.server_address[1]),
                   'fixture-pc', 'fixture-node-secret', 'direct', ['alice', 'bob'], timeout=2)

    def new_runtime(self, offline=False):
        return Runtime(self.root / 'coordinator', {'remote-a': self.remote(offline)}, self.gitea,
                       self.actors, 'fixture-worker-secret')

    def restart(self, offline=False):
        self.runtime.close()
        self.runtime = self.new_runtime(offline)
        return self.runtime

    def tearDown(self):
        self.runtime.close()
        self.node_server.shutdown()
        self.node_server.server_close()
        self.node_thread.join()
        self.host.close()
        self.engine.close()
        self.temp.cleanup()

    def start(self, key='start-1'):
        return self.runtime.command(key, self.task_id, 'alice', 'start', {'revision': 1})['run']

    def begin_start_hole(self, key, prepare=True, dispatch=False):
        payload = {'actor': 'alice', 'action': 'start', 'data': {'revision': 1}}
        self.runtime.journal.begin(key, self.task_id, payload)
        run = self.runtime.store.start(self.task_id, 'alice', expected_revision=1, command_key=key)
        launch = self.runtime.store.launch(run['id'])
        if prepare:
            remote = self.runtime.adapters['remote-a']
            handle = remote.prepare(launch)
            self.runtime.store.set_handle(run['id'], handle)
            if dispatch:
                remote.start_prepared(launch, handle)
        return self.runtime.store.run(run['id'])

    def test_new_coordinator_and_adapter_reuse_same_run_and_process(self):
        run = self.start()
        prior = self.runtime.adapters['remote-a'].observe(run['handle'])
        self.restart()
        recovered = self.runtime.store.run(run['id'])
        after = self.runtime.adapters['remote-a'].observe(recovered['handle'])
        self.assertEqual('working', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual(run['generation'], recovered['generation'])
        self.assertEqual(run['id'], self.runtime.store.task(self.task_id)['active_run_id'])
        self.assertEqual(prior['children'][0]['pid'], after['children'][0]['pid'])
        self.assertEqual(1, self.engine.starts)

    def test_crash_after_prepared_handle_before_worker_dispatch_replays_exact_launch(self):
        run = self.begin_start_hole('prepared-hole')
        saved = self.runtime.store.launch(run['id'])
        self.assertEqual(0, self.engine.starts)
        self.restart()
        self.assertEqual(1, self.engine.starts)
        self.assertEqual(saved, self.runtime.store.launch(run['id']))
        self.assertEqual(run['handle'], self.runtime.store.run(run['id'])['handle'])
        receipt = self.runtime.command('prepared-hole', self.task_id, 'alice', 'start', {'revision': 1})
        self.assertTrue(receipt['duplicate'])
        self.assertEqual(1, self.engine.starts)
        self.assertFalse(self.runtime.store.task(self.task_id)['recovery_pending'])

    def test_crash_before_preparation_does_not_lose_durable_launch(self):
        run = self.begin_start_hole('no-handle', prepare=False)
        self.restart()
        self.assertEqual(1, self.engine.starts)
        self.assertEqual(run['id'], self.runtime.store.task(self.task_id)['active_run_id'])
        self.assertIsNotNone(self.runtime.store.run(run['id'])['handle'])

    def test_durable_start_intent_before_launch_transaction_is_recovered_once(self):
        key = 'start-before-launch-transaction'
        self.runtime.journal.begin(key, self.task_id,
                                   {'actor': 'alice', 'action': 'start', 'data': {'revision': 1}})
        self.assertIsNone(self.runtime.store.task(self.task_id)['active_run_id'])
        self.restart()
        self.assertEqual('working', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual(1, self.engine.starts)
        self.assertFalse(self.runtime.journal.unresolved())
        result = self.runtime.command(key, self.task_id, 'alice', 'start', {'revision': 1})
        self.assertTrue(result['duplicate'])
        self.assertEqual(1, self.engine.starts)

    def test_crash_after_worker_start_before_command_receipt_does_not_start_twice(self):
        run = self.begin_start_hole('receipt-hole', dispatch=True)
        before = self.runtime.adapters['remote-a'].observe(run['handle'])
        self.restart()
        after = self.runtime.adapters['remote-a'].observe(run['handle'])
        self.assertEqual(before['children'][0]['pid'], after['children'][0]['pid'])
        self.assertEqual(1, self.engine.starts)
        self.assertFalse(self.runtime.journal.unresolved())

    def test_unreachable_worker_stays_fenced_then_recovers_without_relaunch(self):
        run = self.start()
        self.restart(offline=True)
        self.assertEqual('recovering', self.runtime.store.task(self.task_id)['status'])
        with self.assertRaises(CoordinationError):
            self.runtime.store.reserve_operation(run['id'], 'blocked', {'body': 'stale'})
        self.assertEqual(1, self.engine.starts)
        self.runtime.adapters['remote-a'] = self.remote()
        self.runtime.recovery.tick()
        self.assertEqual('working', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual(1, self.engine.starts)

    def test_worker_ledger_replacement_never_replays_prior_job(self):
        run = self.start()
        self.runtime.close()
        self.node_server.shutdown(); self.node_server.server_close(); self.node_thread.join()
        self.host.close(stop_engines=False)
        self.host = WorkerHost(self.root / 'replacement-node', 'fixture-pc', 'fixture-node-secret',
                               {'direct': self.engine}, ['alice'])
        self.node_server = make_worker_server(self.host, ('127.0.0.1', 0))
        self.node_thread = threading.Thread(target=self.node_server.serve_forever, daemon=True)
        self.node_thread.start()
        self.runtime = self.new_runtime()
        self.assertEqual('recovering', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual('remote_ledger_changed', self.runtime.recovery.errors[self.task_id])
        self.assertEqual(1, self.engine.starts)
        self.assertEqual(run['handle'], self.runtime.store.run(run['id'])['handle'])

    def test_source_review_block_is_not_cleared_by_worker_recovery(self):
        run = self.start()
        self.runtime.store.set_source_pending(self.task_id, True)
        self.restart()
        self.assertEqual('working', self.runtime.store.task(self.task_id)['status'])
        self.assertTrue(self.runtime.store.task(self.task_id)['source_pending'])
        with self.assertRaisesRegex(CoordinationError, 'source_pending'):
            self.runtime.store.reserve_operation(run['id'], 'source-blocked', {'body': 'premature'})

    def test_changed_request_during_downtime_is_delivered_without_implicit_ack(self):
        run = self.start()
        self.gitea.add(31, '/coord change 1\nnew requirement', actor='bob', actor_id=2)
        self.restart()
        self.assertEqual(2, self.runtime.store.task(self.task_id)['revision'])
        self.assertEqual(1, self.runtime.store.run(run['id'])['revision'])
        observation = self.runtime.adapters['remote-a'].observe(run['handle'])
        self.assertEqual(2, observation['accepted_revision'])
        with self.assertRaisesRegex(CoordinationError, 'stale_revision'):
            self.runtime.store.reserve_operation(run['id'], 'unacked', {'body': 'premature'})

    def test_downtime_resume_comment_on_paused_task_is_backfilled(self):
        run = self.start()
        self.runtime.command('stop-1', self.task_id, 'alice', 'stop', {'run_id': run['id']})
        paused = self.runtime.store.task(self.task_id)
        self.gitea.add(32, f"/coord resume {paused['generation']} {paused['revision']}", actor='alice', actor_id=1)
        self.restart()
        task = self.runtime.store.task(self.task_id)
        self.assertEqual('working', task['status'])
        self.assertNotEqual(run['id'], task['active_run_id'])
        self.assertEqual(2, self.engine.starts)

    def test_durable_stop_intent_before_state_transition_is_recovered(self):
        run = self.start()
        key = 'stop-before-transition'
        payload = {'actor': 'alice', 'action': 'stop', 'data': {'run_id': run['id']}}
        self.runtime.journal.begin(key, self.task_id, payload)
        self.restart()
        self.assertEqual('paused', self.runtime.store.task(self.task_id)['status'])
        self.assertTrue(self.runtime.adapters['remote-a'].observe(run['handle'])['stopped'])
        self.assertFalse(self.runtime.journal.unresolved())

    def test_stop_crash_preserves_unknown_external_effect_and_resource(self):
        run = self.start()
        self.runtime.store.acquire_resource('lab-environment', run['id'])
        self.runtime.store.reserve_operation(run['id'], 'external-before-crash', {'action': 'fixture'}, resource='lab-environment', environment=1)
        self.runtime.store.begin_operation('external-before-crash')
        payload = {'actor': 'alice', 'action': 'stop', 'data': {'run_id': run['id']}}
        self.runtime.journal.begin('stop-midflight', self.task_id, payload)
        self.runtime.store.stop(self.task_id, 'alice', expected_run_id=run['id'])
        self.restart()
        task = self.runtime.store.task(self.task_id)
        self.assertEqual('paused', task['status'])
        self.assertEqual('unknown', self.runtime.store.operation('external-before-crash')['status'])
        with self.assertRaisesRegex(CoordinationError, 'unresolved_operations'):
            self.runtime.command('resume-blocked', self.task_id, 'alice', 'resume',
                                 {'revision': task['revision'], 'generation': task['generation']})

    def test_expired_generation_is_stopped_not_restored_as_current(self):
        run = self.start()
        self.runtime.store.expire(run['id'])
        self.restart()
        self.assertEqual('paused', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual('stopped', self.runtime.store.run(run['id'])['status'])
        with self.assertRaisesRegex(CoordinationError, 'stale_generation'):
            self.runtime.store.acknowledge(run['id'], 1)

    def test_delivery_failure_after_reconnect_remains_retryable(self):
        run = self.start()
        self.gitea.add(33, '/coord change 1\nnew requirement', actor='bob', actor_id=2)
        with patch.object(RemoteAdapter, 'deliver', side_effect=AdapterError('worker_unreachable')):
            self.restart()
        task = self.runtime.store.task(self.task_id)
        self.assertTrue(task['status'] == 'recovering' or task['recovery_pending'])
        self.runtime.recovery.tick()
        self.assertEqual(2, self.runtime.adapters['remote-a'].observe(run['handle'])['accepted_revision'])
        self.assertEqual('working', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual(1, self.engine.starts)

    def test_stop_of_launch_without_prepared_handle_finishes_without_remote_start(self):
        run = self.begin_start_hole('cancel-before-prepare', prepare=False)
        self.runtime.store.stop(self.task_id, 'alice', expected_run_id=run['id'])
        self.restart()
        self.assertEqual('paused', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual('stopped', self.runtime.store.run(run['id'])['status'])
        self.assertEqual(0, self.engine.starts)

    def completed_engine_observation(self, run):
        job = self.host._job(run['handle'])
        self.engine.complete(job['engine_handle'])
        original = self.engine.observe
        def completed(handle):
            observation = original(handle)
            if handle['run_id'] == run['id'] and observation.get('stopped'):
                return {**observation, 'status': 'completed', 'accepted_revision': run['revision'],
                        'result': {'text': 'deterministic fixture result', 'customer_released': False}}
            return observation
        return completed

    def test_remote_completion_is_saved_for_review_without_publication(self):
        run = self.start()
        with patch.object(self.engine, 'observe', side_effect=self.completed_engine_observation(run)):
            self.runtime.recovery.tick()
        saved = self.runtime.store.run(run['id'])
        self.assertEqual('result_ready', self.runtime.store.task(self.task_id)['status'])
        self.assertEqual('result_ready', saved['status'])
        self.assertEqual('deterministic fixture result', saved['result']['text'])
        self.assertEqual(0, self.gitea.post_calls)
        self.assertEqual(1, self.engine.starts)

    def test_completion_during_controller_downtime_survives_restart(self):
        run = self.begin_start_hole('completed-before-receipt', dispatch=True)
        with patch.object(self.engine, 'observe', side_effect=self.completed_engine_observation(run)):
            self.restart()
        saved = self.runtime.store.run(run['id'])
        self.assertEqual('result_ready', saved['status'])
        self.assertEqual('deterministic fixture result', saved['result']['text'])
        self.assertFalse(self.runtime.journal.unresolved())
        self.restart()
        self.assertEqual(saved['result'], self.runtime.store.run(run['id'])['result'])
        self.assertEqual(1, self.engine.starts)
        self.assertEqual(0, self.gitea.post_calls)


if __name__ == '__main__':
    unittest.main()
