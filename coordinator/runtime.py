"""Task-scoped orchestration. Only the dedicated lab publisher has side effects."""
import hashlib
import hmac
import json
import threading
import queue
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit

from .journal import Journal, digest
from .instance import InstanceLock
from .recovery import RecoveryManager
from .store import Store, CoordinationError


class Runtime:
    def __init__(self, state_dir, adapters, gitea, actors, worker_secret, publish_status=False, recover_background=False):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.instance = InstanceLock(self.state_dir / '.coordinator.lock')
        try:
            self.store = Store(self.state_dir / 'coordination.sqlite')
            self.journal = Journal(self.state_dir / 'commands.sqlite')
        except Exception:
            self.instance.close()
            raise
        self.adapters = adapters
        self.gitea = gitea
        self.actors = actors
        self.worker_secret = worker_secret.encode()
        self.locks = {}
        self.lock_guard = threading.Lock()
        self.status_queue = queue.Queue() if publish_status else None
        self.status_thread = None
        self.status_errors = {}
        self.closing = False
        self.recovery = RecoveryManager(self)
        self.recovery_stop = threading.Event()
        self.recovery_thread = None
        try:
            self.recovery.startup()
        except Exception:
            self.instance.close()
            raise
        if recover_background:
            self.recovery_thread = threading.Thread(target=self._recovery_worker, daemon=True)
            self.recovery_thread.start()
        if self.status_queue is not None:
            self.status_thread = threading.Thread(target=self._status_writer, daemon=True)
            self.status_thread.start()

    def queue_status(self, task_id):
        if self.status_queue is not None:
            self.status_queue.put(task_id)

    def _recovery_worker(self):
        while not self.recovery_stop.wait(2):
            try:
                self.recovery.tick()
            except Exception as exc:
                self.recovery.errors['service'] = type(exc).__name__

    def _status_writer(self):
        while True:
            task_id = self.status_queue.get()
            if task_id is None:
                self.status_queue.task_done()
                return
            try:
                if self.closing:
                    continue
                self.sync_status(task_id)
                self.status_errors.pop(task_id, None)
            except Exception as exc:
                self.status_errors[task_id] = getattr(exc, 'code', type(exc).__name__)
            finally:
                self.status_queue.task_done()

    def lock(self, task_id):
        with self.lock_guard:
            return self.locks.setdefault(task_id, threading.RLock())

    def actor(self, name):
        actor = self.actors.get(name)
        if not actor or actor.get('role') == 'viewer':
            raise CoordinationError('forbidden')
        return actor

    def task_permission(self, task_id, actor):
        self.actor(actor)
        task = self.store.task(task_id)
        if actor not in (task['owner'], task['decision_owner']):
            raise CoordinationError('forbidden')
        return task

    def worker_token(self, run_id):
        return hmac.new(self.worker_secret, run_id.encode(), hashlib.sha256).hexdigest()

    def command(self, key, task_id, actor, action, data):
        self.actor(actor)
        with self.lock(task_id):
            self.task_permission(task_id, actor)
            prior = self.journal.begin(key, task_id, {'actor': actor, 'action': action, 'data': data})
            if not prior['new']:
                if prior['state'] == 'done':
                    return {'duplicate': True, 'result': prior['result']}
                raise CoordinationError('command_' + prior['state'])
            try:
                result = self._command(task_id, actor, action, data, key)
            except CoordinationError as exc:
                self.journal.finish(key, 'rejected', {'error': exc.code})
                raise
            except Exception:
                self.journal.finish(key, 'unknown', {'error': 'execution_outcome_unconfirmed'})
                if self.recovery.is_remote(self.store.task(task_id)):
                    self.store.set_recovery_pending(task_id, True)
                else:
                    self.store.set_source_pending(task_id, True)
                raise
            self.journal.finish(key, 'done', result)
            if self.recovery.is_remote(self.store.task(task_id)):
                self.recovery.commands(task_id)
            self.queue_status(task_id)
            return result

    def _command(self, task_id, actor, action, data, key):
        if action == 'recover':
            return self.recovery.task(task_id)
        if action == 'review_sources':
            task = self.store.task(task_id)
            if actor != task['decision_owner']:
                raise CoordinationError('forbidden')
            if int(data['base_revision']) != task['revision']:
                raise CoordinationError('stale_revision')
            source_events = [e for e in self.store.events(task_id) if e.get('type') == 'source_recorded']
            latest = max((e['id'] for e in source_events), default=0)
            if int(data['through_event_id']) != latest or not str(data.get('reason', '')).strip():
                raise CoordinationError('source_review_stale_or_missing')
            if any(x['task'] == task_id and x['id'] != key for x in self.journal.unresolved()):
                raise CoordinationError('unresolved_commands')
            self.store.record_event(task_id, 'review:' + key, '1', digest(data), actor,
                                    json.dumps(data, ensure_ascii=False), 'reviewed')
            self.store.set_source_pending(task_id, False)
            return {'reviewed_through': latest, 'task': self.store.task(task_id)}
        if action == 'change':
            result = self.store.change(task_id, actor, int(data['base_revision']), data['text'], key)
            task = self.store.task(task_id)
            if result['status'] == 'accepted' and task.get('active_run_id'):
                run = self.store.run(task['active_run_id'])
                if run['status'] == 'running':
                    try:
                        result['delivery'] = self.deliver(run['id'])
                    except Exception as exc:
                        result['delivery'] = {'state': 'pending', 'error': getattr(exc, 'code', type(exc).__name__)}
                else:
                    result['delivery'] = {'state': 'after_resume'}
            return result
        if action in ('start', 'resume'):
            task = self.store.task(task_id)
            if int(data['revision']) != task['revision']:
                raise CoordinationError('stale_revision')
            if action == 'start' and task['status'] != 'ready':
                raise CoordinationError('explicit_resume_required')
            if action == 'resume' and int(data['generation']) != task['generation']:
                raise CoordinationError('stale_generation')
            adapter = self.adapters[task['adapter']]
            extra = {}
            if action == 'resume' and getattr(adapter, 'durable_remote', False):
                previous = sorted(self.store.runs(task_id), key=lambda row: row['created_at'])
                if previous:
                    extra['resume_from'] = previous[-1]['id']
            run = self.store.start(task_id, actor, expected_revision=int(data['revision']),
                                   expected_generation=int(data['generation']) if action == 'resume' else None,
                                   command_key=key, launch_extra=extra)
            launch = self.store.launch(run['id'])
            try:
                if getattr(adapter, 'durable_remote', False):
                    handle = adapter.prepare(launch)
                    self.store.set_handle(run['id'], handle)
                    adapter.start_prepared(launch, handle)
                else:
                    handle = adapter.start(run)
                self.store.set_handle(run['id'], handle)
            except Exception as exc:
                if getattr(exc, 'handle', None):
                    self.store.set_handle(run['id'], exc.handle)
                if getattr(adapter, 'durable_remote', False):
                    self.store.begin_recovery(run['id'])
                else:
                    self.store.expire(run['id'])
                raise
            return {'run': self.store.run(run['id']), 'worker_token': self.worker_token(run['id'])}
        if action == 'stop':
            task = self.store.task(task_id)
            if not data.get('run_id') or task.get('active_run_id') != data['run_id']:
                raise CoordinationError('stale_stop_target')
            self.store.stop(task_id, actor, expected_run_id=data['run_id'])
            task = self.store.task(task_id)
            rid = task.get('active_run_id')
            if rid:
                run = self.store.run(rid)
                if run.get('handle'):
                    self.adapters[run['adapter']].stop(run['handle'])
                return self.observe(rid)
            return {'task': task}
        if action == 'deliver':
            if self.store.run(data['run_id'])['task_id'] != task_id:
                raise CoordinationError('run_task_mismatch')
            return self.deliver(data['run_id'])
        if action == 'observe':
            if self.store.run(data['run_id'])['task_id'] != task_id:
                raise CoordinationError('run_task_mismatch')
            return self.observe(data['run_id'])
        raise CoordinationError('unknown_action')

    def deliver(self, run_id):
        run = self.store.run(run_id)
        snapshot = self.store.get_snapshot(run_id)
        if run['status'] not in ('running', 'change_pending'):
            raise CoordinationError('run_not_deliverable')
        return self.adapters[run['adapter']].deliver(run['handle'], snapshot)

    def observe(self, run_id):
        run = self.store.run(run_id)
        if not run.get('handle'):
            return {'run': run, 'observation': {'stopped': False, 'reason': 'handle_missing'}}
        observation = self.adapters[run['adapter']].observe(run['handle'])
        if run['status'] in ('stopping', 'unknown'):
            self.store.confirm_stopped(run_id, bool(observation.get('stopped')))
        return {'run': self.store.run(run_id), 'observation': observation}

    def acknowledge(self, run_id, revision):
        # Explicit worker acknowledgement; delivery alone never invokes this.
        result = self.store.acknowledge(run_id, revision)
        self.queue_status(result['task_id'])
        return result

    def publish(self, run_id, operation_id, body):
        run = self.store.run(run_id)
        task = self.store.task(run['task_id'])
        with self.lock(task['id']):
            # Re-read source existence before publication; unseen later comments
            # are outside the accepted-revision guarantee.
            self.gitea.issue(task['repo'], task['issue'])
            payload = {'kind': 'comment', 'repo': task['repo'], 'issue': task['issue'], 'body': body}
            operation = self.store.reserve_operation(run_id, operation_id, payload)
            if operation['status'] == 'succeeded':
                return operation
            operation = self.store.begin_operation(operation_id)
            if not operation.get('dispatch'):
                raise CoordinationError('operation_not_dispatchable')
            marker = '<!-- isolated-coordinator:operation:' + operation_id + ' -->'
            try:
                comment = self.gitea.post(task['repo'], task['issue'], marker + '\n' + body)
            except Exception:
                self.store.finish_operation(operation_id, None)
                raise
            return self.store.finish_operation(operation_id, True, {'comment_id': comment['id'], 'url': comment.get('html_url')})

    def reconcile_publish(self, operation_id):
        operation = self.store.operation(operation_id)
        if operation['status'] != 'unknown':
            raise CoordinationError('operation_not_unknown')
        payload = operation['payload']
        marker = '<!-- isolated-coordinator:operation:' + operation_id + ' -->'
        found = [c for c in self.gitea.comments(payload['repo'], payload['issue'])
                 if c['user']['login'] == self.gitea.user and c['body'] == marker + '\n' + payload['body']]
        if len(found) != 1:
            raise CoordinationError('external_result_unresolved')
        return self.store.reconcile_operation(operation_id, True, {'comment_id': found[0]['id']})

    def sync_status(self, task_id):
        with self.lock(task_id):
            task = self.store.task(task_id)
            marker = '<!-- isolated-coordinator:status:' + task_id + ' -->'
            run = self.store.run(task['active_run_id']) if task.get('active_run_id') else None
            execution = f"작업자 확인 r{run['revision']} · 실행 `{run['id']}`" if run else '현재 실행 없음'
            pending = f"원문 검토 대기: {task['source_pending']} · 요구 충돌: {task['conflict_pending']}"
            body = f"{marker}\n요구 r{task['revision']} · {task['status']} · 담당 {task['owner']}\n\n{execution}\n\n{pending}\n\n{task['text']}\n\n실험용 상태. 고객 배포를 의미하지 않습니다."
            cid = self.journal.status_comment(task_id)
            if cid is None:
                existing = [x for x in self.gitea.comments(task['repo'], task['issue'])
                            if x['user']['login'] == self.gitea.user and x['body'].startswith(marker)]
                if len(existing) > 1:
                    raise CoordinationError('multiple_status_comments')
                if existing:
                    cid = existing[0]['id']
                else:
                    if not self.journal.reserve_status_post(task_id):
                        raise CoordinationError('status_creation_unknown')
                    try:
                        cid = self.gitea.post(task['repo'], task['issue'], body)['id']
                    except Exception:
                        self.journal.finish_status_post(task_id, False)
                        raise
                self.journal.status_comment(task_id, cid)
                self.journal.finish_status_post(task_id, True)
            return self.gitea.edit(task['repo'], cid, body)

    def webhook(self, payload):
        repo = payload.get('repository', {}).get('full_name')
        if repo not in self.gitea.repos:
            raise CoordinationError('repository_not_allowed')
        issue = payload.get('issue', {})
        number = int(issue['number'])
        task_id = f'{repo}#{number}'
        # Only explicitly registered tasks are managed. Existing jobs are never adopted.
        self.store.task(task_id)
        comment = payload.get('comment')
        if not comment:
            return {'ignored': 'not_a_comment'}
        source_id = f'gitea:{repo}:comment:{int(comment["id"])}'
        with self.lock(task_id):
            try:
                current = self.gitea.comment(repo, comment['id'])
            except HTTPError as exc:
                if exc.code == 404:
                    self.store.set_source_pending(task_id, True)
                    return self.store.record_event(task_id, source_id, 'deleted', '', 'source', '', 'needs_triage')
                self.store.set_source_pending(task_id, True)
                raise
            actor = current['user']['login']
            body = current['body']
            expected_issue = f'/{repo}/issues/{number}'
            if urlsplit(current.get('issue_url', '')).path != expected_issue:
                self.store.set_source_pending(task_id, True)
                raise CoordinationError('source_issue_mismatch')
            body_hash = hashlib.sha256(body.encode()).hexdigest()
            version = current.get('updated_at', current.get('created_at', '')) + ':' + body_hash
            if actor == self.gitea.user and body.startswith('<!-- isolated-coordinator:'):
                return {'ignored': 'service_status_or_receipt'}
            registered = self.actors.get(actor)
            if not registered or registered.get('gitea_id') != current['user']['id']:
                raise CoordinationError('unknown_source_author')
            self.task_permission(task_id, actor)
            # Commands are typed. Free-form interpretation is never faked by a model stub.
            try:
                command = parse_comment(body)
            except CoordinationError:
                self.store.record_event(task_id, source_id, version, body_hash, actor, body, 'invalid_command_needs_triage')
                self.store.set_source_pending(task_id, True)
                self.queue_status(task_id)
                return {'needs_triage': 'invalid_command_syntax'}
            disposition = 'command' if command else 'needs_triage'
            event = self.store.record_event(task_id, source_id, version, body_hash, actor, body, disposition)
            if not command:
                if event['status'] != 'duplicate':
                    self.store.set_source_pending(task_id, True)
                    self.queue_status(task_id)
                return event
            action, data = command
            return self.command(source_id + ':' + version, task_id, actor, action, data)

    def close(self):
        try:
            self.recovery_stop.set()
            if self.recovery_thread:
                self.recovery_thread.join(timeout=20)
            if self.status_thread:
                self.closing = True
                self.status_queue.put(None)
                self.status_thread.join(timeout=10)
            for adapter in self.adapters.values():
                adapter.close()
        finally:
            self.instance.close()


def parse_comment(body):
    lines = body.splitlines()
    if not lines or not lines[0].startswith('/coord '):
        return None
    parts = lines[0].split()
    if len(parts) == 3 and parts[1] == 'start' and parts[2].isdigit():
        return 'start', {'revision': int(parts[2])}
    if len(parts) == 3 and parts[1] == 'stop':
        return 'stop', {'run_id': parts[2]}
    if len(parts) == 4 and parts[1] == 'resume' and parts[2].isdigit() and parts[3].isdigit():
        return 'resume', {'generation': int(parts[2]), 'revision': int(parts[3])}
    if len(parts) == 3 and parts[1] == 'change' and parts[2].isdigit():
        text = '\n'.join(lines[1:]).strip()
        if text:
            return 'change', {'base_revision': int(parts[2]), 'text': text}
    raise CoordinationError('invalid_command_syntax')
