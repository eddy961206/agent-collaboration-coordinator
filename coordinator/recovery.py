"""Reconnect to independently owned workers; never infer death from a timeout."""
from .store import CoordinationError


class RecoveryManager:
    def __init__(self, runtime):
        self.runtime = runtime
        self.errors = {}

    def is_remote(self, task):
        adapter = self.runtime.adapters.get(task['adapter'])
        return bool(adapter and getattr(adapter, 'durable_remote', False))

    def startup(self):
        rt = self.runtime
        rt.store.recover_operations()
        remote_tasks = set()
        for run in rt.store.runs():
            if run['status'] not in ('running', 'recovering', 'stopping', 'unknown'):
                continue
            task = rt.store.task(run['task_id'])
            if self.is_remote(task):
                rt.store.begin_recovery(run['id'])
                remote_tasks.add(task['id'])
            elif run['status'] == 'running':
                rt.store.expire(run['id'])
        for row in rt.journal.unresolved():
            if not row['task']:
                continue
            task = rt.store.task(row['task'])
            if self.is_remote(task):
                rt.store.set_recovery_pending(task['id'], True)
                remote_tasks.add(task['id'])
            else:
                rt.store.set_source_pending(task['id'], True)
        remote_tasks.update(task['id'] for task in rt.store.tasks() if self.is_remote(task))
        for task_id in remote_tasks:
            try:
                self.backfill(task_id)
            except Exception as exc:
                self.errors[task_id] = 'source_backfill_' + type(exc).__name__
                rt.store.set_source_pending(task_id, True)
            self.task(task_id)

    def backfill(self, task_id):
        """Replay durable source comments after downtime; command receipts deduplicate."""
        rt = self.runtime
        task = rt.store.task(task_id)
        for comment in rt.gitea.comments(task['repo'], task['issue']):
            try:
                rt.webhook({'repository': {'full_name': task['repo']}, 'issue': {'number': task['issue']},
                            'comment': comment, 'action': 'created'})
            except CoordinationError as exc:
                if exc.code not in ('forbidden', 'unknown_source_author', 'stale_stop_target', 'command_rejected',
                                    'stale_revision', 'stale_generation', 'explicit_resume_required', 'task_not_startable', 'command_unknown'):
                    raise

    def task(self, task_id):
        rt = self.runtime
        with rt.lock(task_id):
            task = rt.store.task(task_id)
            if not self.is_remote(task):
                raise CoordinationError('adapter_not_recoverable')
            result = {'task_id': task_id, 'recovered': False}
            if task.get('active_run_id'):
                run = rt.store.run(task['active_run_id'])
                if run['status'] in ('recovering', 'stopping', 'unknown'):
                    try:
                        result = self.run(run)
                        self.errors.pop(task_id, None)
                    except Exception as exc:
                        self.errors[task_id] = getattr(exc, 'code', type(exc).__name__)
                        result = {'task_id': task_id, 'recovered': False, 'reason': self.errors[task_id]}
            self.commands(task_id)
            return {**result, 'task': rt.store.task(task_id)}

    def run(self, run):
        rt = self.runtime
        adapter = rt.adapters[run['adapter']]
        launch = rt.store.launch(run['id'])
        handle = run.get('handle')
        if not handle:
            if not launch:
                raise CoordinationError('launch_record_missing')
            if run['status'] in ('stopping', 'unknown'):
                # The prepared handle is committed before every mutating remote
                # RPC. No handle means this launch never crossed that boundary.
                result = rt.store.confirm_stopped(run['id'], True)
                return {'recovered': result['confirmed'], 'state': 'cancelled_before_dispatch'}
            handle = adapter.prepare(launch)
            rt.store.set_handle(run['id'], handle)
        try:
            observation = adapter.recover(handle)
        except Exception as exc:
            if getattr(exc, 'code', '') != 'remote_run_not_found' or not launch:
                raise
            if run['status'] in ('stopping', 'unknown'):
                # Same worker ledger proves start never reserved a job.
                return rt.store.confirm_stopped(run['id'], True)
            adapter.start_prepared(launch, handle)
            observation = adapter.recover(handle)
        if run['status'] in ('stopping', 'unknown'):
            if not observation.get('stopped'):
                adapter.stop(handle)
                observation = adapter.recover(handle)
            result = rt.store.confirm_stopped(run['id'], bool(observation.get('stopped')))
            return {'recovered': result['confirmed'], 'observation': observation}
        if observation.get('status') in ('unknown', 'starting', 'launching'):
            return {'recovered': False, 'reason': 'worker_state_unconfirmed', 'observation': observation}
        if observation.get('stopped') and self.output(observation) is not None:
            observation = {**observation, 'status': 'completed'}
        if observation.get('stopped') and observation.get('status') not in ('completed', 'done', 'finished'):
            rt.store.stop(run['task_id'], run['owner'], expected_run_id=run['id'])
            rt.store.confirm_stopped(run['id'], True)
            return {'recovered': True, 'observation': observation, 'state': 'paused'}
        normalized = {**observation, 'status': 'completed' if observation.get('stopped') else 'running'}
        restored = rt.store.restore_run(run['id'], normalized)
        task = rt.store.task(run['task_id'])
        if not observation.get('stopped') and task['revision'] != restored['revision']:
            # This reuses the remote delivery receipt. It never grants semantic ack.
            try:
                adapter.deliver(handle, rt.store.get_snapshot(run['id']))
            except Exception:
                rt.store.begin_recovery(run['id'])
                raise
        rt.queue_status(run['task_id'])
        self.collect(run['id'], observation)
        return {'recovered': True, 'run_id': run['id'], 'observation': observation}

    @staticmethod
    def output(observation):
        result = observation.get('result') if observation.get('status') in ('completed', 'done', 'succeeded') else None
        if result is None and isinstance(observation.get('messages'), list):
            latest = {}
            for message in observation['messages']:
                if message.get('kind') == 'agent':
                    latest[message.get('author')] = message
            if latest and all(message.get('status') == 'done' for message in latest.values()):
                result = {author: message.get('text', '') for author, message in latest.items()}
        if result is None:
            return None
        return result

    def collect(self, run_id, observation):
        if not observation.get('stopped'):
            return None
        result = self.output(observation)
        if result is None:
            return None
        evidence = {'kind': 'native_worker_output', 'reviewed': False,
                    'execution_revision': observation.get('accepted_revision'),
                    'session_id': observation.get('session_id'), 'node_id': observation.get('node_id')}
        received = self.runtime.store.stage_result(run_id, result, evidence)
        self.runtime.queue_status(received['task_id'])
        return received

    def commands(self, task_id):
        rt = self.runtime
        for row in rt.journal.unresolved():
            if row['task'] != task_id or row['state'] != 'unknown' or not row.get('payload'):
                continue
            payload = row['payload']
            action, data = payload.get('action'), payload.get('data', {})
            if action in ('start', 'resume'):
                run = rt.store.launch_for_command(row['id'])
                if run is None:
                    # The launch row is committed atomically before any worker RPC.
                    # Absence therefore permits replay of this exact durable intent.
                    try:
                        rt.task_permission(task_id, payload['actor'])
                        rt.store.set_recovery_pending(task_id, False)
                        result = rt._command(task_id, payload['actor'], action, data, row['id'])
                        rt.journal.finish(row['id'], 'done', {**result, 'recovered': True})
                    except CoordinationError as exc:
                        rt.journal.finish(row['id'], 'rejected', {'error': exc.code})
                    except Exception as exc:
                        self.errors[task_id] = getattr(exc, 'code', type(exc).__name__)
                    finally:
                        rt.store.set_recovery_pending(task_id, True)
                elif run['status'] in ('running', 'stopped', 'done', 'result_ready'):
                    rt.journal.finish(row['id'], 'done', {'run': run, 'worker_token': rt.worker_token(run['id']), 'recovered': True})
            elif action == 'change':
                change = rt.store.change_for_command(task_id, row['id'])
                if change:
                    rt.journal.finish(row['id'], 'done', {**change, 'task': rt.store.task(task_id), 'recovered': True})
                else:
                    try:
                        rt.task_permission(task_id, payload['actor'])
                        result = rt._command(task_id, payload['actor'], action, data, row['id'])
                        rt.journal.finish(row['id'], 'done', {**result, 'recovered': True})
                    except CoordinationError as exc:
                        rt.journal.finish(row['id'], 'rejected', {'error': exc.code})
            elif action == 'stop' and data.get('run_id'):
                run = rt.store.run(data['run_id'])
                task = rt.store.task(task_id)
                if run['task_id'] != task_id or (run['status'] != 'stopped' and task.get('active_run_id') != run['id']):
                    rt.journal.finish(row['id'], 'rejected', {'error': 'stale_stop_target'})
                    continue
                if run['status'] != 'stopped':
                    try:
                        rt.task_permission(task_id, payload['actor'])
                        rt.store.stop(task_id, payload['actor'], expected_run_id=run['id'])
                        self.run(rt.store.run(run['id']))
                    except Exception as exc:
                        self.errors[task_id] = getattr(exc, 'code', type(exc).__name__)
                    run = rt.store.run(run['id'])
                if run['task_id'] == task_id and run['status'] == 'stopped':
                    rt.journal.finish(row['id'], 'done', {'run': run, 'recovered': True})
            elif action in ('observe', 'recover'):
                task = rt.store.task(task_id)
                if task['status'] not in ('recovering', 'stopping', 'unknown'):
                    rt.journal.finish(row['id'], 'done', {'task': task, 'recovered': True})
            elif action == 'deliver' and data.get('run_id'):
                run = rt.store.run(data['run_id'])
                if run['task_id'] != task_id or run['status'] in ('stopped', 'done', 'result_ready'):
                    rt.journal.finish(row['id'], 'rejected', {'error': 'delivery_superseded'})
                elif run['status'] == 'running':
                    try:
                        receipt = rt.deliver(run['id'])
                        rt.journal.finish(row['id'], 'done', receipt)
                    except Exception as exc:
                        self.errors[task_id] = getattr(exc, 'code', type(exc).__name__)
        unresolved = any(row['task'] == task_id for row in rt.journal.unresolved())
        rt.store.set_recovery_pending(task_id, unresolved)

    def tick(self):
        for task in self.runtime.store.tasks():
            if self.is_remote(task) and (task['status'] in ('recovering', 'stopping', 'unknown') or task['recovery_pending']):
                self.task(task['id'])
            elif self.is_remote(task) and task['status'] == 'working' and task.get('active_run_id'):
                with self.runtime.lock(task['id']):
                    try:
                        run = self.runtime.store.run(task['active_run_id'])
                        observation = self.runtime.adapters[run['adapter']].observe(run['handle'])
                        self.collect(run['id'], observation)
                    except Exception as exc:
                        self.errors[task['id']] = getattr(exc, 'code', type(exc).__name__)
                        self.runtime.store.begin_recovery(task['active_run_id'])
