"""Bounded adapters for owned fake processes and a separately configured lab RT."""
import json
import os
from pathlib import Path
import queue
import re
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
import uuid


class AdapterError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def snapshot_payload(value):
    task = value.get("task", value)
    revision = value.get("revision", task.get("revision"))
    text = value.get("text", task.get("text", task.get("demand")))
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        raise AdapterError("invalid_revision")
    if not isinstance(text, str):
        raise AdapterError("invalid_text")
    return {"revision": revision, "text": text,
            "task_id": value.get("task_id", task.get("id")),
            "generation": value.get("generation", task.get("generation"))}


class DirectAdapter:
    """Execute only fake_worker.py, never a model CLI or caller-supplied command."""
    def __init__(self, workspace_root, timeout=5):
        self.workspace_root = Path(workspace_root).resolve()
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self._workers = {}
        self._registry_lock = threading.Lock()

    def _worker(self, handle):
        item = self._workers.get(handle.get("run_id"))
        if item is None or item["handle"] != handle:
            raise AdapterError("unknown_owned_worker")
        return item

    @staticmethod
    def _reader(process, output):
        try:
            for line in process.stdout:
                try:
                    output.put(json.loads(line))
                except (ValueError, TypeError):
                    output.put({"error": "invalid_worker_output"})
        finally:
            output.put({"error": "worker_pipe_closed"})

    def _request(self, item, operation, snapshot=None):
        with item["lock"]:
            if item["process"].poll() is not None:
                raise AdapterError("worker_exited")
            request_id = uuid.uuid4().hex
            request = {"request_id": request_id, "operation": operation}
            if snapshot is not None:
                request["snapshot"] = snapshot_payload(snapshot)
            try:
                item["process"].stdin.write(json.dumps(request) + "\n")
                item["process"].stdin.flush()
                receipt = item["output"].get(timeout=self.timeout)
            except (BrokenPipeError, OSError) as exc:
                raise AdapterError("worker_pipe_failed") from exc
            except queue.Empty as exc:
                raise AdapterError("worker_timeout") from exc
            if receipt.get("error"):
                raise AdapterError(receipt["error"])
            if receipt.get("request_id") != request_id:
                raise AdapterError("worker_receipt_mismatch")
            item["last"] = receipt
            return receipt

    def start(self, run):
        run_id = run["id"]
        if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
            raise AdapterError("invalid_run_id")
        snapshot_payload(run)
        with self._registry_lock:
            if run_id in self._workers:
                raise AdapterError("worker_already_exists")
            workspace = self.workspace_root / run_id
            try:
                workspace.mkdir()
            except FileExistsError as exc:
                raise AdapterError("workspace_already_exists") from exc
            temp = workspace / "tmp"
            temp.mkdir()
            environment = {key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ}
            environment.update({"HOME": str(workspace), "USERPROFILE": str(workspace),
                                "TMP": str(temp), "TEMP": str(temp), "TMPDIR": str(temp),
                                "PYTHONUTF8": "1", "PYTHONNOUSERSITE": "1"})
            stderr = (workspace / "worker-stderr.log").open("w", encoding="utf-8")
            process = subprocess.Popen(
                [sys.executable, "-I", "-S", str(Path(__file__).with_name("fake_worker.py"))],
                cwd=workspace, env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=stderr, text=True, encoding="utf-8", bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            handle = {"adapter": "direct", "run_id": run_id, "pid": process.pid,
                      "workspace": str(workspace), "owner_token": uuid.uuid4().hex}
            item = {"process": process, "handle": handle, "output": queue.Queue(),
                    "lock": threading.Lock(), "last": {}, "stderr": stderr}
            self._workers[run_id] = item
            thread = threading.Thread(target=self._reader, args=(process, item["output"]), daemon=True)
            thread.start()
            item["reader"] = thread
        try:
            self._request(item, "start", run)
        except Exception:
            self._close_item(item)
            raise
        return dict(handle)

    def deliver(self, handle, snapshot):
        receipt = self._request(self._worker(handle), "change", snapshot)
        return {"delivered": True, "revision": receipt["revision"],
                "acknowledgement": "deterministic_fixture", "model_calls": 0}

    def stop(self, handle):
        return self._end(handle, "stop")

    def complete(self, handle):
        return self._end(handle, "finish")

    def _end(self, handle, operation):
        item = self._worker(handle)
        if item["last"].get("stopped") is True and item["process"].poll() == 0:
            return self.observe(handle)
        receipt = self._request(item, operation)
        try:
            item["process"].wait(timeout=self.timeout)
        except subprocess.TimeoutExpired as exc:
            raise AdapterError("parent_stop_unconfirmed") from exc
        receipt["stopped"] = receipt.get("stopped") is True and item["process"].returncode == 0
        return receipt

    def observe(self, handle):
        item = self._worker(handle)
        if item["process"].poll() is None:
            return self._request(item, "snapshot")
        last = item["last"]
        stopped = last.get("stopped") is True and item["process"].returncode == 0
        return {"stopped": stopped, "status": "stopped" if stopped else "unknown",
                "children": last.get("children", []), "exit_code": item["process"].returncode,
                "model_calls": 0}

    def _close_item(self, item):
        process = item["process"]
        if process.poll() is None:
            try:
                self.stop(item["handle"])
            except AdapterError:
                # EOF asks our fixed parent to clean up its fixed child.
                process.stdin.close()
                try:
                    process.wait(timeout=self.timeout)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=self.timeout)
        if not process.stdin.closed:
            process.stdin.close()
        item["reader"].join(timeout=self.timeout)
        process.stdout.close()
        item["stderr"].close()

    def close(self):
        for item in list(self._workers.values()):
            self._close_item(item)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class RoundtableAdapter:
    """An explicitly allowlisted lab RT. Transport receipts are not model decisions."""
    def __init__(self, base_url, participants, *, allowed_hosts, timeout=5):
        parsed = urlsplit(base_url)
        if (parsed.scheme != "http" or parsed.hostname not in set(allowed_hosts)
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path not in ("", "/") or not parsed.port):
            raise AdapterError("rt_endpoint_not_allowlisted")
        if parsed.hostname in ("localhost", "127.0.0.1", "::1") and parsed.port in (8787, 8790, 8791):
            raise AdapterError("production_rt_port_forbidden")
        if not participants or any(not re.fullmatch(r"[A-Za-z0-9_-]+", p) for p in participants):
            raise AdapterError("invalid_rt_participants")
        self.base_url = base_url.rstrip("/")
        self.participants = list(participants)
        self.timeout = timeout
        self._opener = build_opener(ProxyHandler({}), _NoRedirect())
        self._handles = {}
        self._lock = threading.Lock()

    def _request(self, method, path, body=None):
        encoded = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(self.base_url + path, data=encoded, method=method,
                          headers={"Content-Type": "application/json"})
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                if len(raw) > 4 * 1024 * 1024:
                    raise AdapterError("rt_response_too_large")
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            exc.close()
            raise AdapterError("rt_http_" + str(exc.code)) from exc
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            raise AdapterError("rt_request_failed") from exc

    def _handle(self, handle):
        stored = self._handles.get(handle.get("run_id"))
        if stored != handle:
            raise AdapterError("unknown_owned_room")
        return "/api/rooms/" + quote(handle["room_id"], safe="")

    def _message(self, handle, snapshot):
        payload = snapshot_payload(snapshot)
        responses = []
        for target in handle["participants"]:
            responses.append(self._request("POST", self._handle(handle) + "/messages", {
                "text": "COORDINATOR_FIXTURE " + json.dumps(payload, ensure_ascii=False),
                "target": target,
            }))
        return {"delivered": True, "revision": payload["revision"],
                "message_ids": [response.get("id") for response in responses],
                "live": [response.get("live") for response in responses],
                "acknowledgement": "transport_only"}

    def start(self, run):
        snapshot_payload(run)
        run_id = run["id"]
        with self._lock:
            if run_id in self._handles:
                raise AdapterError("room_already_exists")
            room = self._request("POST", "/api/rooms", {
                "title": "Isolated coordinator " + str(run_id),
                "mode": "chat", "participants": self.participants,
            })
            room_id = room.get("id")
            if not isinstance(room_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", room_id):
                raise AdapterError("invalid_rt_room_id")
            seats = room.get("participants")
            if (not isinstance(seats, list) or len(seats) != len(self.participants)
                    or any(not isinstance(p, dict) or not isinstance(p.get("id"), str)
                           or not re.fullmatch(r"[A-Za-z0-9_-]+", p["id"]) for p in seats)):
                raise AdapterError("invalid_rt_participant_ids")
            handle = {"adapter": "rt", "run_id": run_id, "room_id": room_id,
                      "participants": [p["id"] for p in seats],
                      "owner_token": uuid.uuid4().hex}
            self._handles[run_id] = handle
        try:
            self._message(handle, run)
        except AdapterError as exc:
            # Room creation already succeeded. Preserve its owned identity even
            # if only some seats accepted the initial message before an error.
            exc.handle = dict(handle)
            raise
        return dict(handle)

    def deliver(self, handle, snapshot):
        return self._message(handle, snapshot)

    def stop(self, handle):
        path = self._handle(handle)
        self._request("POST", path + "/stop", {})
        deadline = time.monotonic() + self.timeout
        while True:
            observed = self.observe(handle)
            if observed["stopped"] or time.monotonic() >= deadline:
                return {**observed, "stop_requested": True}
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))

    def observe(self, handle):
        state = self._request("GET", self._handle(handle))
        participants = state.get("room", {}).get("participants")
        messages = state.get("messages")
        known = isinstance(participants, list) and isinstance(messages, list) and isinstance(state.get("active"), bool)
        children = [{"id": p.get("id"), "status": p.get("status", "unknown")}
                    for p in participants or []]
        inactive = {"idle", "done", "cancelled", "stopped", "error"}
        messages_idle = all(m.get("status") in inactive for m in messages or [] if m.get("kind") == "agent")
        stopped = bool(known and not state["active"] and messages_idle
                       and all(p["status"] in inactive for p in children))
        return {"stopped": stopped, "status": "idle" if stopped else "active_or_unknown",
                "children": children, "active": state.get("active"), "room_id": handle["room_id"],
                "messages": messages if isinstance(messages, list) else [],
                "acknowledgement": "transport_only"}

    def close(self):
        errors = []
        for handle in list(self._handles.values()):
            try:
                if not self.stop(handle)["stopped"]:
                    errors.append(handle["run_id"])
            except AdapterError:
                errors.append(handle["run_id"])
        if errors:
            raise AdapterError("rt_stop_unconfirmed:" + ",".join(errors))
