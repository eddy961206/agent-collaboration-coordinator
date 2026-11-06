"""Behavior tests against the real SQLite implementation, without model calls."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import multiprocessing
from pathlib import Path
import tempfile
import threading
import unittest

from coordinator.store import CoordinationError, Store


def _process_start(path, task_id, ready, release, results):
    store = Store(path)
    ready.put(True)
    release.wait(10)
    try:
        result = store.start(task_id, "alice")
        results.put(("started", result["id"]))
    except CoordinationError as error:
        results.put((error.code, None))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "coord.sqlite3"
        self.store = Store(self.path)
        self.store.create_task("T1", "fake/project", 1, "alice", "direct", "initial", "bob")

    def assert_rejected(self, code, call, *args, **kwargs):
        with self.assertRaises(CoordinationError) as caught:
            call(*args, **kwargs)
        self.assertEqual(code, caught.exception.code)

    def second_run(self):
        self.store.create_task("T2", "fake/project", 2, "carol", "rt", "independent")
        return self.store.start("T2", "carol")

    def test_persisted_task_and_run_snapshot(self):
        run = self.store.start("T1", "alice")
        self.store.set_handle(run["id"], {"adapter": "direct", "worker": "fake-1"})
        reopened = Store(self.path)
        self.assertEqual("working", reopened.task("T1")["status"])
        self.assertEqual({"adapter": "direct", "worker": "fake-1"}, reopened.run(run["id"])["handle"])
        self.assertEqual("initial", reopened.get_snapshot(run["id"])["text"])
        self.assertFalse(reopened.get_snapshot(run["id"])["stale"])

    def test_actor_permissions_do_not_change_state(self):
        self.assert_rejected("forbidden_actor", self.store.start, "T1", "outsider")
        self.assert_rejected("forbidden_actor", self.store.change, "T1", "outsider", 1, "bad", "evil")
        self.assert_rejected("forbidden_actor", self.store.stop, "T1", "outsider")
        self.assertEqual(1, self.store.task("T1")["revision"])
        self.assertEqual([], self.store.runs())

    def test_same_issue_cannot_have_alias_tasks(self):
        self.assert_rejected("issue_already_registered", self.store.create_task, "alias", "FAKE/PROJECT", 1, "bob", "rt", "duplicate")
        self.assertEqual(["T1"], [task["id"] for task in self.store.tasks()])

    def test_concurrent_issue_registration_has_one_dispatcher(self):
        barrier = threading.Barrier(8)
        def create(index):
            barrier.wait()
            try:
                return self.store.create_task("alias-" + str(index), "fake/project", 50, "alice", "direct", "same issue")["id"]
            except CoordinationError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(create, range(8)))
        self.assertEqual(7, results.count("issue_already_registered"))
        self.assertEqual(1, sum(task["issue"] == 50 for task in self.store.tasks()))

    def test_concurrent_same_task_starts_have_one_winner(self):
        barrier = threading.Barrier(12)
        def start(_):
            barrier.wait()
            try:
                return self.store.start("T1", "alice")["id"]
            except CoordinationError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(start, range(12)))
        self.assertEqual(11, results.count("task_not_startable"))
        self.assertEqual(1, len(self.store.runs("T1")))
        other = self.second_run()
        self.assertEqual("running", other["status"])
        self.assertEqual(2, sum(run["status"] == "running" for run in self.store.runs()))

    def test_different_processes_cannot_claim_same_task(self):
        context = multiprocessing.get_context("spawn")
        ready, results, release = context.Queue(), context.Queue(), context.Event()
        processes = [context.Process(target=_process_start, args=(str(self.path), "T1", ready, release, results)) for _ in range(2)]
        try:
            for process in processes:
                process.start()
            for _ in processes:
                self.assertTrue(ready.get(timeout=15))
            release.set()
            observed = [results.get(timeout=15)[0] for _ in processes]
            for process in processes:
                process.join(timeout=15)
                self.assertEqual(0, process.exitcode)
            self.assertCountEqual(["started", "task_not_startable"], observed)
        finally:
            release.set()
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join(timeout=5)
            ready.close()
            results.close()

    def test_conflicting_human_changes_preserved_until_decision_owner_resolves(self):
        run = self.store.start("T1", "alice")
        barrier = threading.Barrier(2)
        def change(actor):
            barrier.wait()
            return self.store.change("T1", actor, 1, "request by " + actor, actor)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(change, ("alice", "bob")))
        self.assertCountEqual(["accepted", "conflict"], [result["status"] for result in results])
        self.assertTrue(self.store.task("T1")["conflict_pending"])
        self.store.acknowledge(run["id"], 2)
        self.assert_rejected("conflict_pending", self.store.reserve_operation, run["id"], "publish", {"message": "done"})
        self.assertEqual("conflict", self.store.change("T1", "alice", 2, "override", "attempt-override")["status"])
        accepted = self.store.change("T1", "bob", 2, "agreed request", "resolution")
        self.assertEqual(3, accepted["task"]["revision"])
        self.assertFalse(accepted["task"]["conflict_pending"])
        conflicts = [event for event in self.store.events("T1") if event["type"] == "change_conflict"]
        self.assertEqual(2, len(conflicts))
        self.store.acknowledge(run["id"], 3)
        self.assertEqual("reserved", self.store.reserve_operation(run["id"], "agreed", {"message": "done"})["status"])

    def test_change_requires_worker_acknowledgement(self):
        run = self.store.start("T1", "alice")
        self.store.change("T1", "bob", 1, "changed", "change-1")
        snapshot = self.store.get_snapshot(run["id"])
        self.assertTrue(snapshot["stale"])
        self.assertEqual(1, snapshot["run"]["revision"])
        self.assertEqual(2, snapshot["revision"])
        self.assert_rejected("stale_revision", self.store.reserve_operation, run["id"], "stale-publish", {"text": "old result"})
        self.assert_rejected("stale_revision", self.store.finish, run["id"], {"ok": True}, [])
        self.assert_rejected("stale_revision", self.store.acknowledge, run["id"], 1)
        self.store.acknowledge(run["id"], 2)
        self.assertFalse(self.store.get_snapshot(run["id"])["stale"])
        self.assertEqual("changed", self.store.run(run["id"])["text"])

    def test_change_redelivery_does_not_create_new_revision(self):
        self.store.change("T1", "alice", 1, "new", "change-1")
        result = self.store.change("T1", "alice", 1, "new", "change-1")
        self.assertEqual("duplicate", result["status"])
        self.assertEqual(2, result["task"]["revision"])
        self.assert_rejected("event_id_collision", self.store.change, "T1", "alice", 1, "different", "change-1")
        self.assertEqual(2, Store(self.path).task("T1")["revision"])

    def test_source_dedup_preserves_edits_and_ambiguous_version(self):
        digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
        first = self.store.record_event("T1", "comment:7", "v1", digest("old"), "bob", "old", "request")
        duplicate = self.store.record_event("T1", "comment:7", "v1", digest("old"), "bob", "old", "request")
        edited = self.store.record_event("T1", "comment:7", "v2", digest("new"), "bob", "new", "request")
        self.assertEqual("recorded", first["status"])
        self.assertEqual("duplicate", duplicate["status"])
        self.assertEqual(first["event"]["id"], duplicate["event"]["id"])
        self.assertNotEqual(first["event"]["id"], edited["event"]["id"])
        self.assert_rejected("source_version_conflict", self.store.record_event, "T1", "comment:7", "v2", digest("ambiguous"), "bob", "ambiguous", "request")
        self.assertTrue(Store(self.path).task("T1")["source_pending"])

    def test_parent_stop_does_not_release_child_resource_or_allow_resume(self):
        run = self.store.start("T1", "alice")
        other = self.second_run()
        self.store.acquire_resource("shared-test", run["id"])
        self.store.stop("T1", "bob")
        observation = self.store.confirm_stopped(run["id"], False)
        self.assertFalse(observation["confirmed"])
        self.assertEqual("stopping", observation["task"]["status"])
        self.assert_rejected("task_not_startable", self.store.start, "T1", "alice")
        self.assert_rejected("stop_not_confirmed", self.store.release_resource, "shared-test", run["id"])
        self.assert_rejected("resource_busy", self.store.acquire_resource, "shared-test", other["id"])
        self.assert_rejected("stale_generation", self.store.acknowledge, run["id"], 1)
        self.store.confirm_stopped(run["id"], True)
        self.store.acquire_resource("shared-test", other["id"])
        resumed = self.store.start("T1", "alice")
        self.assertNotEqual(run["id"], resumed["id"])
        self.assertGreater(resumed["generation"], run["generation"])

    def test_lost_heartbeat_keeps_resource_until_observed_stop(self):
        run = self.store.start("T1", "alice")
        other = self.second_run()
        self.store.acquire_resource("shared-test", run["id"])
        self.store.expire(run["id"])
        reopened = Store(self.path)
        self.assertEqual("unknown", reopened.task("T1")["status"])
        self.assert_rejected("resource_busy", reopened.acquire_resource, "shared-test", other["id"])
        self.assert_rejected("task_not_startable", reopened.start, "T1", "alice")
        self.assert_rejected("stale_generation", reopened.heartbeat, run["id"])
        reopened.confirm_stopped(run["id"], True)
        self.assertEqual(other["id"], reopened.acquire_resource("shared-test", other["id"])["run_id"])

    def test_reserved_operation_is_not_revalidated_by_new_worker_ack(self):
        run = self.store.start("T1", "alice")
        self.store.reserve_operation(run["id"], "old-publish", {"text": "old result"})
        self.store.change("T1", "bob", 1, "new request", "change")
        self.store.acknowledge(run["id"], 2)
        self.assert_rejected("stale_revision", self.store.begin_operation, "old-publish")
        self.assertEqual("rejected", Store(self.path).operation("old-publish")["status"])
        self.store.reserve_operation(run["id"], "new-publish", {"text": "new result"})
        self.assertTrue(self.store.begin_operation("new-publish")["dispatch"])

    def test_stop_after_reservation_blocks_dispatch(self):
        run = self.store.start("T1", "alice")
        self.store.reserve_operation(run["id"], "restart", {"action": "restart"})
        self.store.stop("T1", "alice")
        self.assert_rejected("stale_generation", self.store.begin_operation, "restart")
        self.assertEqual("rejected", self.store.operation("restart")["status"])

    def test_operation_id_cannot_change_payload_or_run(self):
        run = self.store.start("T1", "alice")
        other = self.second_run()
        self.store.reserve_operation(run["id"], "publish", {"a": 1, "b": 2})
        self.assertEqual("reserved", self.store.reserve_operation(run["id"], "publish", {"b": 2, "a": 1})["status"])
        self.assert_rejected("operation_id_collision", self.store.reserve_operation, run["id"], "publish", {"a": 3})
        self.assert_rejected("operation_id_collision", self.store.reserve_operation, other["id"], "publish", {"a": 1, "b": 2})

    def test_concurrent_dispatch_has_exactly_one_admission(self):
        run = self.store.start("T1", "alice")
        self.store.reserve_operation(run["id"], "publish", {"text": "result"})
        barrier = threading.Barrier(8)
        def dispatch(_):
            barrier.wait()
            return self.store.begin_operation("publish")
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(dispatch, range(8)))
        self.assertEqual(1, sum(item["dispatch"] for item in results))
        self.store.finish_operation("publish", True, {"comment_id": 1})
        self.assertFalse(Store(self.path).begin_operation("publish")["dispatch"])

    def test_unknown_effect_survives_restart_and_blocks_resume_until_reconciled(self):
        run = self.store.start("T1", "alice")
        other = self.second_run()
        self.store.acquire_resource("shared-test", run["id"])
        self.store.reserve_operation(run["id"], "publish", {"text": "result"}, "shared-test")
        self.assertTrue(self.store.begin_operation("publish")["dispatch"])
        self.store.finish_operation("publish", None, {"reason": "response lost"})
        reopened = Store(self.path)
        self.assertFalse(reopened.begin_operation("publish")["dispatch"])
        self.assert_rejected("unresolved_operations", reopened.finish, run["id"], {"ok": True}, [])
        reopened.stop("T1", "alice")
        reopened.confirm_stopped(run["id"], True)
        self.assert_rejected("unresolved_operations", reopened.start, "T1", "alice")
        self.assert_rejected("resource_busy", reopened.acquire_resource, "shared-test", other["id"])
        reopened.reconcile_operation("publish", True, {"observed_comment_id": 7})
        self.assertFalse(reopened.begin_operation("publish")["dispatch"])
        reopened.acquire_resource("shared-test", other["id"])
        self.assertEqual("running", reopened.start("T1", "alice")["status"])

    def test_crash_during_external_operation_becomes_unknown(self):
        run = self.store.start("T1", "alice")
        self.store.reserve_operation(run["id"], "publish", {"text": "result"})
        self.store.begin_operation("publish")
        reopened = Store(self.path)
        self.assertFalse(reopened.begin_operation("publish")["dispatch"])
        reopened.expire(run["id"])
        self.assertEqual("unknown", reopened.operation("publish")["status"])
        self.assert_rejected("operation_not_executing", reopened.finish_operation, "publish", True, {"late": True})
        reopened.reconcile_operation("publish", False, {"source_checked": "no matching effect"})
        reopened.confirm_stopped(run["id"], True)
        self.assertEqual("running", reopened.start("T1", "alice")["status"])

    def test_environment_change_invalidates_reserved_proof(self):
        run = self.store.start("T1", "alice")
        lease = self.store.acquire_resource("camera-lab", run["id"])
        self.store.reserve_operation(run["id"], "test-proof", {"test": "passed"}, "camera-lab", lease["environment"])
        self.store.bump_environment("camera-lab")
        self.assert_rejected("stale_environment", self.store.begin_operation, "test-proof")
        self.assertEqual(2, Store(self.path).environment("camera-lab")["generation"])

    def test_old_worker_cannot_finish_after_resume(self):
        run = self.store.start("T1", "alice")
        self.store.stop("T1", "alice")
        self.store.confirm_stopped(run["id"], True)
        current = self.store.start("T1", "alice")
        self.assert_rejected("stale_generation", self.store.acknowledge, run["id"], 1)
        self.assert_rejected("stale_generation", self.store.finish, run["id"], "old", [])
        self.assertEqual("done", self.store.finish(current["id"], "current", ["fake check"])["status"])

    def test_late_stop_confirmation_does_not_release_new_run_resource(self):
        old = self.store.start("T1", "alice")
        other = self.second_run()
        self.store.acquire_resource("test-server", old["id"])
        self.store.stop("T1", "alice")
        self.store.confirm_stopped(old["id"], True)
        current = self.store.start("T1", "alice")
        self.store.acquire_resource("test-server", current["id"])
        self.store.confirm_stopped(old["id"], True)
        self.assertEqual(current["id"], self.store.task("T1")["active_run_id"])
        self.assertEqual("running", self.store.run(current["id"])["status"])
        self.assert_rejected("resource_busy", self.store.acquire_resource, "test-server", other["id"])
        self.assert_rejected("resource_not_owned", self.store.release_resource, "test-server", old["id"])

    def test_old_stop_command_cannot_stop_resumed_execution(self):
        old = self.store.start("T1", "alice")
        self.store.stop("T1", "alice", expected_run_id=old["id"])
        self.store.confirm_stopped(old["id"], True)
        current = self.store.start("T1", "alice")
        self.store.acquire_resource("test-server", current["id"])
        before = self.store.task("T1")
        self.assert_rejected("stale_stop_target", self.store.stop, "T1", "bob", expected_run_id=old["id"])
        after = self.store.task("T1")
        self.assertEqual(before, after)
        self.assertEqual("running", self.store.run(current["id"])["status"])
        self.assertEqual(current["id"], self.store.acquire_resource("test-server", current["id"])["run_id"])

    def test_resume_requires_expected_request_and_execution_generation(self):
        old = self.store.start("T1", "alice")
        self.store.stop("T1", "alice", expected_run_id=old["id"])
        self.store.confirm_stopped(old["id"], True)
        paused = self.store.task("T1")
        self.store.change("T1", "bob", 1, "changed while paused", "paused-change")
        self.assert_rejected("stale_revision", self.store.start, "T1", "alice", expected_revision=paused["revision"], expected_generation=paused["generation"])
        self.assert_rejected("stale_generation", self.store.start, "T1", "alice", expected_revision=2, expected_generation=old["generation"])
        resumed = self.store.start("T1", "alice", expected_revision=2, expected_generation=paused["generation"])
        self.assertEqual("changed while paused", resumed["text"])
        self.assertEqual(2, resumed["revision"])

    def test_concurrent_resume_checks_generation_in_same_transaction(self):
        old = self.store.start("T1", "alice")
        self.store.stop("T1", "alice", expected_run_id=old["id"])
        self.store.confirm_stopped(old["id"], True)
        paused = self.store.task("T1")
        barrier = threading.Barrier(8)
        def resume(_):
            barrier.wait()
            try:
                return self.store.start("T1", "alice", expected_revision=paused["revision"], expected_generation=paused["generation"])["id"]
            except CoordinationError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(resume, range(8)))
        self.assertEqual(7, results.count("stale_generation"))
        self.assertEqual(1, sum(run["status"] == "running" for run in self.store.runs("T1")))

    def test_resource_cannot_be_released_while_effect_is_executing(self):
        run = self.store.start("T1", "alice")
        other = self.second_run()
        self.store.acquire_resource("test-server", run["id"])
        self.store.reserve_operation(run["id"], "restart", {"action": "restart"}, "test-server")
        self.store.begin_operation("restart")
        self.assert_rejected("unresolved_operations", self.store.release_resource, "test-server", run["id"])
        self.assert_rejected("resource_busy", self.store.acquire_resource, "test-server", other["id"])
        self.store.finish_operation("restart", True, {"observed": "new process"})
        self.store.release_resource("test-server", run["id"])
        self.store.acquire_resource("test-server", other["id"])

    def test_unknown_effect_blocks_new_ids_and_already_reserved_effects(self):
        run = self.store.start("T1", "alice")
        self.store.reserve_operation(run["id"], "first", {"text": "first"})
        self.store.reserve_operation(run["id"], "queued", {"text": "queued"})
        self.store.begin_operation("first")
        self.store.finish_operation("first", None, {"transport": "lost response"})
        self.assert_rejected("unresolved_operations", self.store.reserve_operation, run["id"], "new-id", {"text": "first"})
        self.assert_rejected("unresolved_operations", self.store.begin_operation, "queued")
        self.store.reconcile_operation("first", False, {"observed": "external request was not applied"})
        self.store.reserve_operation(run["id"], "retry-after-confirmation", {"text": "first"})
        self.assertTrue(self.store.begin_operation("retry-after-confirmation")["dispatch"])

    def test_unresolved_source_blocks_effects_and_completion(self):
        run = self.store.start("T1", "alice")
        self.store.set_source_pending("T1", True)
        self.assert_rejected("source_pending", self.store.reserve_operation, run["id"], "publish", {})
        self.assert_rejected("source_pending", self.store.finish, run["id"], {}, [])
        self.store.set_source_pending("T1", False)
        self.assertEqual(1, self.store.task("T1")["revision"])
        self.assertEqual("done", self.store.finish(run["id"], {"implemented": True, "released": False}, ["fixture-only"])["status"])
        self.assertFalse(self.store.run(run["id"])["result"]["released"])

    def test_racing_change_and_dispatch_follow_durable_order(self):
        # Verify actual SQLite transactions, not a simulated interleaving list.
        for iteration in range(12):
            task_id = "race-" + str(iteration)
            self.store.create_task(task_id, "fake/project", 100 + iteration, "alice", "direct", "old", "bob")
            run = self.store.start(task_id, "alice")
            operation_id = "op-" + str(iteration)
            self.store.reserve_operation(run["id"], operation_id, {"text": "old"})
            barrier = threading.Barrier(2)
            def amend():
                barrier.wait()
                return self.store.change(task_id, "bob", 1, "new", "changed")
            def dispatch():
                barrier.wait()
                try:
                    return self.store.begin_operation(operation_id)["dispatch"]
                except CoordinationError as error:
                    self.assertEqual("stale_revision", error.code)
                    return False
            with ThreadPoolExecutor(max_workers=2) as pool:
                changing, dispatching = pool.submit(amend), pool.submit(dispatch)
                changing.result()
                admitted = dispatching.result()
            relevant = [event["type"] for event in self.store.events(task_id) if event["type"] in ("change_accepted", "operation_dispatched", "operation_rejected")]
            if admitted:
                self.assertEqual(["operation_dispatched", "change_accepted"], relevant)
            else:
                self.assertEqual(["change_accepted", "operation_rejected"], relevant)


if __name__ == "__main__":
    unittest.main()
