"""Fixed, deterministic worker used only by the isolated coordinator lab.

There are no model SDKs, network clients, plugins, shells, or user commands here.
The parent starts one copy of this file as its child and tracks its actual exit.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys


def emit(request, **fields):
    print(json.dumps({"request_id": request.get("request_id"), **fields}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    options = parser.parse_args()
    child = None
    child_stopped = False
    current = None
    revisions = []
    if not options.child:
        child = subprocess.Popen(
            [sys.executable, "-I", "-S", str(Path(__file__).resolve()), "--child"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr,
            text=True, encoding="utf-8", bufsize=1,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )

    def forward(request):
        if child is None:
            return None
        child.stdin.write(json.dumps(request) + "\n")
        child.stdin.flush()
        line = child.stdout.readline()
        if not line:
            raise RuntimeError("fake_child_exited")
        reply = json.loads(line)
        if reply.get("request_id") != request.get("request_id"):
            raise RuntimeError("fake_child_receipt_mismatch")
        return reply

    try:
        for line in sys.stdin:
            request = json.loads(line)
            operation = request.get("operation")
            if operation in ("start", "change"):
                snapshot = request["snapshot"]
                if current and snapshot["revision"] < current["revision"]:
                    emit(request, error="stale_revision")
                    continue
                forward(request)
                current = snapshot
                revisions.append(snapshot["revision"])
                Path("worker-state-child.json" if options.child else "worker-state.json").write_text(
                    json.dumps({"current": current, "revisions": revisions}), encoding="utf-8")
                emit(request, accepted=True, revision=current["revision"],
                     acknowledgement="deterministic_fixture", model_calls=0)
            elif operation == "snapshot":
                children = []
                if child is not None:
                    child_reply = forward(request)
                    children.append({"pid": child.pid, "status": "running",
                                     "revision": child_reply["revision"]})
                emit(request, stopped=False, children=children,
                     revision=current["revision"] if current else None,
                     text=current["text"] if current else None,
                     revisions=revisions, model_calls=0)
            elif operation in ("stop", "finish"):
                if child is not None:
                    reply = forward(request)
                    child.wait(timeout=3)
                    child_stopped = reply.get("stopped") is True and child.returncode == 0
                else:
                    child_stopped = True
                emit(request, stopped=child_stopped,
                     children=([{"pid": child.pid, "status": "stopped" if child_stopped else "unknown"}]
                               if child is not None else []), model_calls=0,
                     finished=operation == "finish")
                break
            else:
                emit(request, error="unsupported_operation")
    finally:
        if child is not None:
            if child.poll() is None:
                # Closing only this owned child's pipe makes its fixed program exit.
                child.stdin.close()
                try:
                    child.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    child.terminate()
                    child.wait(timeout=3)
            if not child.stdin.closed:
                child.stdin.close()
            child.stdout.close()


if __name__ == "__main__":
    main()
