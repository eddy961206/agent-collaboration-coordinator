"""Real OS/process lock tests. No network or model calls."""
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest

from coordinator.instance import InstanceLock


PROBE = r'''
import os
import sys
from coordinator.instance import InstanceLock
try:
    lock = InstanceLock(sys.argv[1])
except RuntimeError as exc:
    print(str(exc), flush=True)
    sys.exit(9)
print("acquired", flush=True)
if sys.argv[2] == "hold":
    sys.stdin.readline()
elif sys.argv[2] == "abrupt":
    os._exit(0)
lock.close()
'''


class InstanceLockTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "state" / ".coordinator.lock"
        self.children = []
        self.locks = []

    def tearDown(self):
        for child in self.children:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)
            for stream in (child.stdin, child.stdout, child.stderr):
                if stream and not stream.closed:
                    stream.close()
        for lock in self.locks:
            lock.close()
        self.temp.cleanup()

    def acquire(self):
        lock = InstanceLock(self.path)
        self.locks.append(lock)
        return lock

    def spawn(self, mode):
        # No inherited user/model configuration is needed by this fixed probe.
        environment = {key: os.environ[key] for key in ("SystemRoot", "WINDIR") if key in os.environ}
        environment.update({"PYTHONUTF8": "1", "PYTHONNOUSERSITE": "1"})
        child = subprocess.Popen(
            [sys.executable, "-S", "-c", PROBE, str(self.path), mode],
            cwd=Path(__file__).resolve().parents[1], env=environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", close_fds=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.children.append(child)
        return child

    def first_line(self, child):
        output = queue.Queue()
        thread = threading.Thread(target=lambda: output.put(child.stdout.readline()), daemon=True)
        thread.start()
        try:
            line = output.get(timeout=5)
        except queue.Empty:
            child.terminate()
            child.wait(timeout=5)
            self.fail("lock subprocess did not produce a bounded response")
        thread.join(timeout=1)
        return line.strip()

    def test_same_process_second_instance_rejected_until_close(self):
        first = self.acquire()
        with self.assertRaisesRegex(RuntimeError, "^coordinator_already_running$"):
            InstanceLock(self.path)
        first.close()
        first.close()
        self.acquire()

    def test_another_process_rejected_without_waiting(self):
        self.acquire()
        child = self.spawn("probe")
        output, error = child.communicate(timeout=5)
        self.assertEqual(child.returncode, 9, error)
        self.assertEqual(output.strip(), "coordinator_already_running")

    def test_release_allows_another_process_and_preserves_lock_file(self):
        first = self.acquire()
        inode = self.path.stat().st_ino
        first.close()
        child = self.spawn("probe")
        output, error = child.communicate(timeout=5)
        self.assertEqual(child.returncode, 0, error)
        self.assertEqual(output.strip(), "acquired")
        self.assertTrue(self.path.exists())
        self.assertEqual(self.path.stat().st_ino, inode)
        self.assertEqual(self.path.read_bytes(), b"0")

    def test_abrupt_exit_releases_os_lock(self):
        child = self.spawn("abrupt")
        output, error = child.communicate(timeout=5)
        self.assertEqual(child.returncode, 0, error)
        self.assertEqual(output.strip(), "acquired")
        self.acquire()

    def test_killed_holder_releases_os_lock(self):
        child = self.spawn("hold")
        self.assertEqual(self.first_line(child), "acquired")
        with self.assertRaisesRegex(RuntimeError, "^coordinator_already_running$"):
            InstanceLock(self.path)
        child.terminate()
        child.wait(timeout=5)
        self.acquire()


if __name__ == "__main__":
    unittest.main()
