"""Failure-path regressions with in-memory transports and no model or network."""
from pathlib import Path
import tempfile
import unittest

from coordinator.runtime import Runtime
from coordinator.store import CoordinationError
from test_runtime import FakeGitea


class DelayedPostGitea(FakeGitea):
    """A timed-out POST is still in flight and is not yet visible to GET."""
    def __init__(self):
        super().__init__()
        self.pending_post = None

    def post(self, repo, number, body):
        assert repo in self.repos
        self.post_calls += 1
        self.pending_post = (1000 + self.post_calls, body, self.user, 99, number)
        raise TimeoutError("synthetic response lost while POST is still in flight")

    def complete_pending_post(self):
        row = self.add(*self.pending_post)
        self.pending_post = None
        return row


class ControllableAdapter:
    def __init__(self):
        self.deliveries = []
        self.fail_delivery = False

    def start(self, run):
        return {"adapter": "direct", "run_id": run["id"]}

    def deliver(self, handle, snapshot):
        self.deliveries.append((handle["run_id"], snapshot["revision"]))
        if self.fail_delivery:
            raise RuntimeError("synthetic adapter delivery failure")
        return {"delivered": True, "revision": snapshot["revision"]}

    def close(self):
        pass


class FailurePathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.adapter = ControllableAdapter()
        self.gitea = DelayedPostGitea()
        self.actors = {"alice": {"role": "operator", "gitea_id": 1},
                       "bob": {"role": "operator", "gitea_id": 2}}
        self.runtime = self.make_runtime()
        self.task_id = "fake/team#1"
        self.runtime.store.create_task(self.task_id, "fake/team", 1, "alice", "direct", "initial", "bob")

    def make_runtime(self):
        return Runtime(self.directory / "state", {"direct": self.adapter}, self.gitea,
                       self.actors, "fixture-worker-secret")

    def tearDown(self):
        self.runtime.close()
        self.temp.cleanup()

    def start(self):
        return self.runtime.command("start", self.task_id, "alice", "start", {"revision": 1})["run"]

    def test_delayed_status_post_is_not_repeated_and_later_readback_resolves(self):
        with self.assertRaises(TimeoutError):
            self.runtime.sync_status(self.task_id)
        self.assertEqual([], self.gitea.comments("fake/team", 1))
        self.assertIsNone(self.runtime.journal.status_comment(self.task_id))
        with self.assertRaisesRegex(CoordinationError, "status_creation_unknown"):
            self.runtime.sync_status(self.task_id)
        self.assertEqual(1, self.gitea.post_calls)

        # The uncertain creation is durable, including after releasing and
        # reacquiring the actual state-directory instance lock.
        self.runtime.close()
        self.runtime = self.make_runtime()
        with self.assertRaisesRegex(CoordinationError, "status_creation_unknown"):
            self.runtime.sync_status(self.task_id)
        self.assertEqual(1, self.gitea.post_calls)

        original = self.gitea.complete_pending_post()
        observed = self.runtime.sync_status(self.task_id)
        self.assertEqual(original["id"], observed["id"])
        self.assertEqual(original["id"], self.runtime.journal.status_comment(self.task_id))
        self.assertEqual(original["id"], self.runtime.sync_status(self.task_id)["id"])
        self.assertEqual(1, self.gitea.post_calls)
        self.assertEqual(1, len(self.gitea.comments("fake/team", 1)))

    def assert_change_deferred_until_resume(self, status):
        run = self.start()
        if status == "stopping":
            self.runtime.store.stop(self.task_id, "alice", expected_run_id=run["id"])
            self.runtime.store.confirm_stopped(run["id"], False)
        else:
            self.runtime.store.expire(run["id"])
        change = {"base_revision": 1, "text": "changed while " + status}
        result = self.runtime.command("changed", self.task_id, "bob", "change", change)
        self.assertEqual("accepted", result["status"])
        self.assertEqual("after_resume", result["delivery"]["state"])
        self.assertEqual(2, self.runtime.store.task(self.task_id)["revision"])
        self.assertEqual(1, self.runtime.store.run(run["id"])["revision"])
        self.assertEqual([], self.adapter.deliveries)
        with self.assertRaisesRegex(CoordinationError, "stale_generation"):
            self.runtime.acknowledge(run["id"], 2)
        duplicate = self.runtime.command("changed", self.task_id, "bob", "change", change)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual("accepted", duplicate["result"]["status"])
        self.assertEqual(2, self.runtime.store.task(self.task_id)["revision"])
        self.assertEqual([], self.runtime.journal.unresolved())

        self.runtime.store.confirm_stopped(run["id"], True)
        paused = self.runtime.store.task(self.task_id)
        resumed = self.runtime.command("resume", self.task_id, "alice", "resume",
                                       {"revision": 2, "generation": paused["generation"]})["run"]
        self.assertEqual(change["text"], resumed["text"])
        self.assertEqual(2, resumed["revision"])

    def test_change_while_children_are_stopping_is_accepted_for_resume(self):
        self.assert_change_deferred_until_resume("stopping")

    def test_change_while_execution_is_unknown_is_accepted_for_resume(self):
        self.assert_change_deferred_until_resume("unknown")

    def test_delivery_failure_keeps_accepted_change_without_worker_ack(self):
        run = self.start()
        self.adapter.fail_delivery = True
        change = {"base_revision": 1, "text": "accepted despite transport failure"}
        result = self.runtime.command("changed", self.task_id, "bob", "change", change)
        self.assertEqual("accepted", result["status"])
        self.assertEqual("pending", result["delivery"]["state"])
        self.assertEqual(2, self.runtime.store.task(self.task_id)["revision"])
        self.assertEqual(1, self.runtime.store.run(run["id"])["revision"])
        with self.assertRaisesRegex(CoordinationError, "stale_revision"):
            self.runtime.acknowledge(run["id"], 1)
        with self.assertRaisesRegex(CoordinationError, "stale_revision"):
            self.runtime.publish(run["id"], "stale-publication", "old result")
        self.assertEqual(0, self.gitea.post_calls)
        self.assertEqual([], self.runtime.journal.unresolved())
        duplicate = self.runtime.command("changed", self.task_id, "bob", "change", change)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(1, len(self.adapter.deliveries))

        self.adapter.fail_delivery = False
        delivered = self.runtime.command("deliver-again", self.task_id, "alice", "deliver", {"run_id": run["id"]})
        self.assertTrue(delivered["delivered"])
        self.assertEqual(1, self.runtime.store.run(run["id"])["revision"])
        self.runtime.acknowledge(run["id"], 2)
        self.assertEqual(2, self.runtime.store.run(run["id"])["revision"])


if __name__ == "__main__":
    unittest.main()
