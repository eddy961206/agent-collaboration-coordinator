"""Isolated public-behavior tests; fake Gitea plus actual fixed worker processes."""
import hashlib
import hmac
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from coordinator.adapters import DirectAdapter
from coordinator.runtime import Runtime
from coordinator.server import make_server
from coordinator.store import CoordinationError


class FakeGitea:
    user = "coordinator-bot"
    repos = frozenset({"fake/team"})

    def __init__(self):
        self.rows = {}
        self.post_calls = 0
        self.edit_calls = 0
        self.issue_calls = 0
        self.comment_calls = 0
        self.lose_next_receipt = False

    def issue(self, repo, number):
        assert repo in self.repos
        self.issue_calls += 1
        return {"number": number, "repository": {"full_name": repo}}

    def comment(self, repo, comment_id):
        assert repo in self.repos
        self.comment_calls += 1
        if comment_id not in self.rows:
            raise HTTPError("http://fake.invalid/comment", 404, "missing", {}, None)
        return dict(self.rows[comment_id])

    def comments(self, repo, number):
        assert repo in self.repos
        return [dict(row) for row in self.rows.values() if row["issue_url"].endswith("/" + str(number))]

    def add(self, comment_id, body, actor="alice", actor_id=1, issue=1, updated="2026-11-06T10:00:00Z"):
        row = {"id": comment_id, "body": body, "user": {"login": actor, "id": actor_id},
               "created_at": "2026-11-06T09:00:00Z", "updated_at": updated,
               "html_url": "http://fake.invalid/fake/team/issues/" + str(issue) + "#issuecomment-" + str(comment_id),
               "issue_url": "http://fake.invalid/fake/team/issues/" + str(issue)}
        self.rows[comment_id] = row
        return dict(row)

    def post(self, repo, number, body):
        assert repo in self.repos
        self.post_calls += 1
        row = self.add(1000 + self.post_calls, body, self.user, 99, number)
        if self.lose_next_receipt:
            self.lose_next_receipt = False
            raise TimeoutError("synthetic lost response after stored comment")
        return row

    def edit(self, repo, comment_id, body):
        assert repo in self.repos
        self.edit_calls += 1
        self.rows[comment_id]["body"] = body
        return dict(self.rows[comment_id])


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.adapter = DirectAdapter(self.directory / "workspaces")
        self.gitea = FakeGitea()
        self.actors = {"alice": {"role": "operator", "gitea_id": 1},
                       "bob": {"role": "operator", "gitea_id": 2},
                       "viewer": {"role": "viewer", "gitea_id": 3}}
        self.runtime = Runtime(self.directory / "state", {"direct": self.adapter},
                               self.gitea, self.actors, "fixture-worker-secret")
        self.tokens = {name: "fixture-token-" + name for name in self.actors}
        self.server = make_server(self.runtime, ("127.0.0.1", 0), self.tokens, "fixture-webhook-secret")
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:" + str(self.server.server_address[1])
        self.task1 = "fake/team#1"
        self.task2 = "fake/team#2"
        self.runtime.store.create_task(self.task1, "fake/team", 1, "alice", "direct", "initial", "bob")
        self.runtime.store.create_task(self.task2, "fake/team", 2, "bob", "direct", "separate", "bob")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.runtime.close()
        self.temp.cleanup()

    def api(self, method, path, body=None, actor="alice", token=None, headers=None):
        request_headers = dict(headers or {})
        if token is not None or actor is not None:
            request_headers["Authorization"] = "Bearer " + (token if token is not None else self.tokens[actor])
        raw = json.dumps(body).encode() if body is not None else None
        request = Request(self.url + path, raw, request_headers, method=method)
        try:
            with urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            with exc:
                return exc.code, json.loads(exc.read())

    def task_path(self, task, suffix=""):
        return "/api/tasks/" + quote(task, safe="") + suffix

    def start(self, task=None, actor="alice", key="start-1"):
        task = task or self.task1
        result = self.runtime.command(key, task, actor, "start", {"revision": self.runtime.store.task(task)["revision"]})
        return result["run"]

    def payload(self, comment, issue=1, body="untrusted payload", actor="forged"):
        return {"repository": {"full_name": "fake/team"}, "issue": {"number": issue},
                "action": "created", "comment": {"id": comment, "body": body,
                "user": {"login": actor, "id": 12345}}}

    def test_http_auth_viewer_and_unrelated_mutation(self):
        self.assertEqual(self.api("GET", "/api/tasks", actor=None)[0], 401)
        self.assertEqual(self.api("GET", "/api/tasks", token="wrong")[0], 401)
        self.assertEqual(self.api("GET", "/api/tasks", actor="viewer")[0], 200)
        body = {"repo": "fake/team", "issue": 3, "adapter": "direct", "text": "new"}
        self.assertEqual(self.api("POST", "/api/tasks", body, actor="viewer")[0], 403)
        status, data = self.api("POST", self.task_path(self.task2, "/commands"),
                                {"action": "start"}, headers={"Idempotency-Key": "wrong-owner"})
        self.assertEqual((status, data["error"]), (403, "forbidden"))

    def test_cross_task_run_delivery_is_denied(self):
        other = self.start(self.task2, "bob", "start-bob")
        status, body = self.api("POST", self.task_path(self.task1, "/commands"),
                                {"action": "deliver", "data": {"run_id": other["id"]}},
                                headers={"Idempotency-Key": "cross-task"})
        self.assertEqual((status, body["error"]), (409, "run_task_mismatch"))

    def test_worker_token_is_bound_to_one_run(self):
        first = self.start()
        second = self.start(self.task2, "bob", "start-bob")
        path = "/api/runs/" + second["id"] + "/ack"
        status, body = self.api("POST", path, {"revision": 1}, token=self.runtime.worker_token(first["id"]))
        self.assertEqual((status, body["error"]), (401, "worker_token_required"))
        self.assertEqual(self.api("POST", path, {"revision": 1}, actor="bob")[0], 401)

    def test_different_people_run_in_parallel_and_duplicate_start_does_not_fork(self):
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(self.runtime.command, key, task, actor, "start", {"revision": 1})
                       for key, task, actor in [("parallel-alice", self.task1, "alice"),
                                                ("parallel-bob", self.task2, "bob"),
                                                ("parallel-alice", self.task1, "alice")]]
            results = [f.result() for f in futures]
        active = self.runtime.store.runs()
        self.assertEqual(len(active), 2)
        self.assertEqual({r["owner"] for r in active}, {"alice", "bob"})
        self.assertTrue(all(not self.adapter.observe(r["handle"])["stopped"] for r in active))
        self.assertEqual(sum(bool(r.get("duplicate")) for r in results), 1)

    def test_change_delivery_does_not_ack_or_allow_stale_publish(self):
        run = self.start()
        result = self.runtime.command("change-2", self.task1, "bob", "change",
                                      {"base_revision": 1, "text": "new requirement"})
        self.assertEqual(result["delivery"]["revision"], 2)
        self.assertEqual(self.adapter.observe(run["handle"])["revision"], 2)
        self.assertEqual(self.runtime.store.run(run["id"])["revision"], 1)
        with self.assertRaisesRegex(CoordinationError, "stale_revision"):
            self.runtime.publish(run["id"], "old-publication", "old result")
        self.assertEqual(self.gitea.post_calls, 0)
        token = self.runtime.worker_token(run["id"])
        self.assertEqual(self.api("POST", "/api/runs/" + run["id"] + "/ack", {"revision": 2}, token=token)[0], 200)
        self.assertEqual(self.runtime.publish(run["id"], "new-publication", "new result")["status"], "succeeded")

    def test_duplicate_publication_never_posts_twice(self):
        run = self.start()
        first = self.runtime.publish(run["id"], "publication", "result")
        second = self.runtime.publish(run["id"], "publication", "result")
        self.assertEqual(first["receipt"], second["receipt"])
        self.assertEqual(self.gitea.post_calls, 1)
        with self.assertRaisesRegex(CoordinationError, "operation_id_collision"):
            self.runtime.publish(run["id"], "publication", "changed payload")

    def test_lost_response_requires_readback_not_blind_retry(self):
        run = self.start()
        self.gitea.lose_next_receipt = True
        with self.assertRaises(TimeoutError):
            self.runtime.publish(run["id"], "uncertain", "result")
        self.assertEqual(self.runtime.store.operation("uncertain")["status"], "unknown")
        with self.assertRaisesRegex(CoordinationError, "operation_not_dispatchable"):
            self.runtime.publish(run["id"], "uncertain", "result")
        self.assertEqual(self.gitea.post_calls, 1)
        self.assertEqual(self.runtime.reconcile_publish("uncertain")["status"], "succeeded")
        self.assertEqual(self.runtime.publish(run["id"], "uncertain", "result")["status"], "succeeded")
        self.assertEqual(self.gitea.post_calls, 1)

    def test_webhook_reads_canonical_author_body_and_same_timestamp_edit(self):
        self.gitea.add(11, "/coord change 1\ncanonical version two", "bob", 2)
        payload = self.payload(11)
        self.assertEqual(self.runtime.webhook(payload)["status"], "accepted")
        self.assertEqual(self.runtime.store.task(self.task1)["text"], "canonical version two")
        self.assertTrue(self.runtime.webhook(payload)["duplicate"])
        self.gitea.add(11, "/coord change 2\ncanonical version three", "bob", 2)
        self.assertEqual(self.runtime.webhook(payload)["status"], "accepted")
        self.assertEqual(self.runtime.store.task(self.task1)["revision"], 3)
        source_events = [e for e in self.runtime.store.events(self.task1) if e["type"] == "source_recorded"]
        self.assertEqual(len(source_events), 2)
        self.assertTrue(all(e["payload"]["actor"] == "bob" for e in source_events))

    def test_webhook_rejects_unregistered_canonical_author(self):
        self.gitea.add(11, "/coord start 1", "stranger", 44)
        with self.assertRaisesRegex(CoordinationError, "unknown_source_author"):
            self.runtime.webhook(self.payload(11, actor="alice"))
        self.assertEqual(self.runtime.store.runs(), [])

    def test_canonical_comment_must_belong_to_selected_issue(self):
        self.gitea.add(11, "/coord change 1\nwrong issue content", "bob", 2, issue=2)
        with self.assertRaisesRegex(CoordinationError, "source_issue_mismatch"):
            self.runtime.webhook(self.payload(11, issue=1))
        self.assertEqual(self.runtime.store.task(self.task1)["revision"], 1)
        self.assertEqual(self.runtime.store.task(self.task2)["revision"], 1)
        self.assertTrue(self.runtime.store.task(self.task1)["source_pending"])

    def test_malformed_change_is_retained_and_requires_review(self):
        run = self.start()
        self.gitea.add(11, "/coord change invalid\n새 요구인데 버전 표기를 잘못함")
        result = self.runtime.webhook(self.payload(11))
        self.assertEqual(result["needs_triage"], "invalid_command_syntax")
        self.assertTrue(self.runtime.store.task(self.task1)["source_pending"])
        source = [e for e in self.runtime.store.events(self.task1) if e["type"] == "source_recorded"]
        self.assertEqual(source[0]["payload"]["disposition"], "invalid_command_needs_triage")
        with self.assertRaisesRegex(CoordinationError, "source_pending"):
            self.runtime.publish(run["id"], "after-malformed-change", "result")

    def test_status_comment_is_updated_once_and_does_not_wake_worker(self):
        first = self.runtime.sync_status(self.task1)
        second = self.runtime.sync_status(self.task1)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(self.gitea.post_calls, 1)
        self.assertEqual(self.runtime.webhook(self.payload(first["id"]))["ignored"], "service_status_or_receipt")
        self.assertEqual(self.runtime.store.runs(), [])
        self.assertFalse(self.runtime.store.task(self.task1)["source_pending"])

    def test_deleted_source_blocks_publication(self):
        run = self.start()
        self.runtime.webhook(self.payload(404))
        self.assertTrue(self.runtime.store.task(self.task1)["source_pending"])
        with self.assertRaisesRegex(CoordinationError, "source_pending"):
            self.runtime.publish(run["id"], "after-deleted-source", "result")

    def test_webhook_signature_required_before_source_fetch(self):
        self.gitea.add(11, "/coord change 1\nnew")
        payload = self.payload(11)
        status, body = self.api("POST", "/webhooks/gitea", payload, actor=None,
                                headers={"X-Gitea-Event": "issue_comment", "X-Gitea-Signature": "wrong"})
        self.assertEqual((status, body["error"]), (401, "invalid_webhook_signature"))
        self.assertEqual(self.gitea.comment_calls, 0)
        raw = json.dumps(payload).encode()
        signature = hmac.new(b"fixture-webhook-secret", raw, hashlib.sha256).hexdigest()
        self.assertEqual(self.api("POST", "/webhooks/gitea", payload, actor=None,
                                 headers={"X-Gitea-Event": "issue_comment", "X-Gitea-Signature": signature})[0], 200)

    def test_crashed_command_journal_is_not_replayed(self):
        command = {"actor": "alice", "action": "start", "data": {"revision": 1}}
        self.runtime.journal.begin("crashed-command", self.task1, command)
        # Model coordinator death only: its OS lock is gone, while external
        # fake workers (if any) are intentionally not stopped by this fixture.
        self.runtime.instance.close()
        restarted = Runtime(self.directory / "state", {"direct": self.adapter},
                            self.gitea, self.actors, "fixture-worker-secret")
        self.runtime = restarted
        self.assertTrue(restarted.store.task(self.task1)["source_pending"])
        with self.assertRaisesRegex(CoordinationError, "command_unknown"):
            restarted.command("crashed-command", self.task1, "alice", "start", {"revision": 1})
        self.assertEqual(restarted.store.runs(), [])

    def test_restart_cannot_assume_old_worker_stopped(self):
        run = self.start()
        self.runtime.instance.close()
        restarted = Runtime(self.directory / "state", {"direct": self.adapter},
                            self.gitea, self.actors, "fixture-worker-secret")
        self.runtime = restarted
        self.assertEqual(restarted.store.run(run["id"])["status"], "unknown")
        self.assertFalse(restarted.observe(run["id"])["observation"]["stopped"])
        with self.assertRaisesRegex(CoordinationError, "task_not_startable"):
            restarted.command("resume-unconfirmed", self.task1, "alice", "resume",
                              {"revision": 1, "generation": restarted.store.task(self.task1)["generation"]})

    def test_second_runtime_cannot_expire_first_runtime_workers(self):
        run = self.start()
        with self.assertRaisesRegex(RuntimeError, "^coordinator_already_running$"):
            Runtime(self.directory / "state", {"direct": self.adapter},
                    self.gitea, self.actors, "fixture-worker-secret")
        self.assertEqual(self.runtime.store.run(run["id"])["status"], "running")
        self.assertEqual(self.runtime.store.task(self.task1)["status"], "working")
        self.assertFalse(self.adapter.observe(run["handle"])["stopped"])

    def test_idempotency_key_cannot_alias_another_task(self):
        # Bob is decision owner of task 1 and owner of task 2. Identical input
        # on different tasks must not return the other task's run capability.
        first = self.start(self.task1, "bob", "same-key")
        with self.assertRaisesRegex((ValueError, CoordinationError), "command_id_payload_conflict|command_id_task_conflict"):
            self.runtime.command("same-key", self.task2, "bob", "start", {"revision": 1})
        self.assertEqual(self.runtime.store.task(self.task2)["status"], "ready")
        self.assertEqual(len(self.runtime.store.runs()), 1)
        self.assertEqual(first["task_id"], self.task1)

    def test_delayed_stop_cannot_stop_resumed_run(self):
        old = self.start()
        self.runtime.command("stop-old", self.task1, "bob", "stop", {"run_id": old["id"]})
        paused = self.runtime.store.task(self.task1)
        self.assertEqual(paused["status"], "paused")
        new = self.runtime.command("resume-new", self.task1, "alice", "resume",
                                   {"revision": paused["revision"], "generation": paused["generation"]})["run"]
        with self.assertRaisesRegex(CoordinationError, "stale_stop_target"):
            self.runtime.command("late-old-stop", self.task1, "bob", "stop", {"run_id": old["id"]})
        self.assertEqual(self.runtime.store.task(self.task1)["active_run_id"], new["id"])
        self.assertFalse(self.adapter.observe(new["handle"])["stopped"])

    def test_only_decision_owner_can_review_exact_source_watermark(self):
        run = self.start()
        self.gitea.add(11, "참고 댓글. 현재 요구를 바꾸지는 않음.")
        self.runtime.webhook(self.payload(11))
        latest = max(e["id"] for e in self.runtime.store.events(self.task1) if e["type"] == "source_recorded")
        review = {"base_revision": 1, "through_event_id": latest, "reason": "검토 결과 참고 자료"}
        with self.assertRaisesRegex(CoordinationError, "forbidden"):
            self.runtime.command("review-not-owner", self.task1, "alice", "review_sources", review)
        self.gitea.add(12, "검토 중 도착한 추가 댓글", "bob", 2)
        self.runtime.webhook(self.payload(12))
        with self.assertRaisesRegex(CoordinationError, "source_review_stale_or_missing"):
            self.runtime.command("review-old-watermark", self.task1, "bob", "review_sources", review)
        review["through_event_id"] = max(e["id"] for e in self.runtime.store.events(self.task1) if e["type"] == "source_recorded")
        self.runtime.command("review-current", self.task1, "bob", "review_sources", review)
        self.assertFalse(self.runtime.store.task(self.task1)["source_pending"])
        self.assertEqual(self.runtime.publish(run["id"], "after-review", "result")["status"], "succeeded")

    def test_source_review_does_not_hide_unknown_command(self):
        self.runtime.journal.begin("unknown-command", self.task1, {"action": "unknown"})
        self.runtime.journal.finish("unknown-command", "unknown", {})
        self.runtime.store.set_source_pending(self.task1, True)
        with self.assertRaisesRegex(CoordinationError, "unresolved_commands"):
            self.runtime.command("review-unknown", self.task1, "bob", "review_sources",
                                  {"base_revision": 1, "through_event_id": 0, "reason": "cannot infer side effects"})
        self.assertTrue(self.runtime.store.task(self.task1)["source_pending"])


if __name__ == "__main__":
    unittest.main()
