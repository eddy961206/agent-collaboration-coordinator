import json
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from coordinator.adapters import AdapterError, DirectAdapter, RoundtableAdapter


def run(run_id="run-1", revision=1):
    return {"id": run_id, "task_id": "task-1", "generation": 1,
            "revision": revision, "text": "요구 v" + str(revision)}


class DirectTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.adapter = DirectAdapter(self.temp.name)

    def tearDown(self):
        self.adapter.close()
        self.temp.cleanup()

    def test_independent_actual_processes_change_stop_resume(self):
        with ThreadPoolExecutor(max_workers=3) as pool:
            handles = list(pool.map(self.adapter.start, [run("A"), run("B"), run("C")]))
        self.assertEqual(len({h["pid"] for h in handles}), 3)
        self.assertEqual(len({h["workspace"] for h in handles}), 3)
        before = [self.adapter.observe(h) for h in handles]
        self.assertTrue(all(not item["stopped"] for item in before))
        self.assertTrue(all(item["children"][0]["status"] == "running" for item in before))
        receipt = self.adapter.deliver(handles[0], {"revision": 2, "text": "changed requirement"})
        self.assertEqual(receipt["revision"], 2)
        self.assertEqual(self.adapter.observe(handles[0])["text"], "changed requirement")
        self.assertEqual(self.adapter.observe(handles[1])["revision"], 1)
        stopped = self.adapter.stop(handles[0])
        self.assertTrue(stopped["stopped"])
        self.assertTrue(all(c["status"] == "stopped" for c in stopped["children"]))
        self.assertFalse(self.adapter.observe(handles[1])["stopped"])
        resumed = self.adapter.start(run("A-resumed", 2))
        self.assertNotEqual(resumed["pid"], handles[0]["pid"])
        self.assertEqual(self.adapter.observe(resumed)["revision"], 2)

    def test_finish_confirms_parent_and_child_exit(self):
        handle = self.adapter.start(run())
        receipt = self.adapter.complete(handle)
        self.assertTrue(receipt["finished"])
        self.assertTrue(receipt["stopped"])
        self.assertTrue(self.adapter.observe(handle)["stopped"])

    def test_stale_delivery_is_rejected(self):
        handle = self.adapter.start(run(revision=2))
        with self.assertRaisesRegex(AdapterError, "stale_revision"):
            self.adapter.deliver(handle, {"revision": 1, "text": "stale"})
        self.assertEqual(self.adapter.observe(handle)["revision"], 2)

    def test_credential_environment_is_not_inherited(self):
        original = __import__("subprocess").Popen
        captured = []

        def record(*args, **kwargs):
            captured.append(kwargs["env"])
            return original(*args, **kwargs)

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "fake-unwanted", "OPENAI_API_KEY": "fake-unwanted",
                                     "CLAUDE_CONFIG_DIR": "unwanted", "HTTP_PROXY": "http://unwanted"}):
            with patch("coordinator.adapters.subprocess.Popen", side_effect=record):
                self.adapter.start(run())
        self.assertFalse({"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "CLAUDE_CONFIG_DIR", "HTTP_PROXY"} & captured[0].keys())
        self.assertTrue(Path(captured[0]["HOME"]).is_relative_to(Path(self.temp.name)))

    def test_handle_cannot_control_another_process(self):
        handle = self.adapter.start(run())
        with self.assertRaisesRegex(AdapterError, "unknown_owned_worker"):
            self.adapter.stop({**handle, "pid": os.getpid()})
        self.assertFalse(self.adapter.observe(handle)["stopped"])

    def test_new_adapter_does_not_claim_old_pid_is_owned(self):
        handle = self.adapter.start(run())
        other = DirectAdapter(self.temp.name)
        with self.assertRaisesRegex(AdapterError, "unknown_owned_worker"):
            other.observe(handle)

    def test_path_escape_and_workspace_reuse_rejected(self):
        with self.assertRaisesRegex(AdapterError, "invalid_run_id"):
            self.adapter.start(run("../escape"))
        handle = self.adapter.start(run())
        self.adapter.stop(handle)
        with self.assertRaisesRegex(AdapterError, "worker_already_exists"):
            self.adapter.start(run())


class FakeRTHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def send_json(self, data, status=200):
        raw = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
        state = self.server.fixture
        if state.get("redirect"):
            self.send_response(302)
            self.send_header("Location", "http://unlisted.invalid:80/leak")
            self.end_headers()
            return
        if self.path == "/api/rooms":
            room_id = "room-" + str(len(state["rooms"]) + 1)
            seats = [{"id": p + "-" + str(i), "status": "idle"} for i, p in enumerate(body["participants"])]
            room = {"id": room_id, "participants": seats}
            state["rooms"][room_id] = {"room": room, "active": False, "messages": []}
            self.send_json(room, 201)
            return
        room_id = self.path.split("/")[3]
        current = state["rooms"][room_id]
        if self.path.endswith("/messages"):
            if state.get("fail_target") == body["target"]:
                self.send_json({"error": "synthetic target failure"}, 503)
                return
            message = {"id": "message-" + str(len(current["messages"])), "kind": "user", "status": "done", **body}
            current["messages"].append(message)
            current["active"] = True
            for p in current["room"]["participants"]:
                if p["id"] == body["target"]:
                    p["status"] = "running"
            self.send_json(message, 201)
        elif self.path.endswith("/stop"):
            current["active"] = False
            for p in current["room"]["participants"]:
                p["status"] = "running" if state.get("child_stuck") else "idle"
            self.send_json({"ok": True})

    def do_GET(self):
        self.send_json(self.server.fixture["rooms"][self.path.split("/")[3]])


class RTTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeRTHandler)
        self.server.fixture = {"rooms": {}}
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = "http://127.0.0.1:" + str(self.server.server_address[1])
        self.adapter = RoundtableAdapter(self.base, ["claude", "claude"], allowed_hosts=["127.0.0.1"], timeout=.2)

    def tearDown(self):
        self.server.fixture["child_stuck"] = False
        self.server.fixture["redirect"] = False
        self.adapter.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_all_seats_receive_change_and_stop(self):
        handle = self.adapter.start(run())
        self.assertEqual(len(handle["participants"]), 2)
        receipt = self.adapter.deliver(handle, {"revision": 2, "text": "updated"})
        self.assertEqual(receipt["acknowledgement"], "transport_only")
        self.assertEqual(len(receipt["message_ids"]), 2)
        state = self.adapter.observe(handle)
        self.assertFalse(state["stopped"])
        self.assertEqual(len(state["messages"]), 4)
        self.assertTrue(all('"revision": 2' in m["text"] for m in state["messages"][-2:]))
        self.assertTrue(self.adapter.stop(handle)["stopped"])

    def test_parent_idle_is_not_child_stopped(self):
        handle = self.adapter.start(run())
        self.server.fixture["child_stuck"] = True
        result = self.adapter.stop(handle)
        self.assertFalse(result["stopped"])
        self.assertFalse(result["active"])
        self.assertEqual(result["children"][0]["status"], "running")

    def test_partial_start_failure_preserves_handle_for_stop(self):
        self.server.fixture["fail_target"] = "claude-1"
        with self.assertRaisesRegex(AdapterError, "rt_http_503") as caught:
            self.adapter.start(run())
        handle = caught.exception.handle
        observed = self.adapter.observe(handle)
        self.assertFalse(observed["stopped"])
        self.assertEqual(len(observed["messages"]), 1)
        self.assertEqual(observed["children"][0]["status"], "running")
        self.assertTrue(self.adapter.stop(handle)["stopped"])
        self.assertEqual(len(self.server.fixture["rooms"]), 1)

    def test_room_ownership_and_no_redirect(self):
        handle = self.adapter.start(run())
        with self.assertRaisesRegex(AdapterError, "unknown_owned_room"):
            self.adapter.stop({**handle, "room_id": "other-room"})
        self.server.fixture["redirect"] = True
        with self.assertRaisesRegex(AdapterError, "rt_http_302"):
            self.adapter.deliver(handle, {"revision": 2, "text": "safe"})

    def test_rejects_production_and_unlisted_endpoints(self):
        for url, hosts in [("http://production.example.invalid:8787", ["127.0.0.1"]),
                           ("http://127.0.0.1:8787", ["127.0.0.1"]),
                           (self.base + "/hidden", ["127.0.0.1"]),
                           ("https://127.0.0.1:1234", ["127.0.0.1"])]:
            with self.assertRaises(AdapterError):
                RoundtableAdapter(url, ["claude"], allowed_hosts=hosts)


if __name__ == "__main__":
    unittest.main()
