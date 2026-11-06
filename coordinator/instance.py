"""Nonblocking OS lock for one coordinator per state directory.

The lock file must remain in place after release: deleting it can let different
processes lock different inodes. OS process exit releases the owned handle.
"""
import errno
import os
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl


class InstanceLock:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = None
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        stream = os.fdopen(descriptor, "r+b", buffering=0)
        try:
            # Windows byte-range locking needs one stable byte. Never truncate
            # an existing file: another process may already hold its lock.
            if os.fstat(stream.fileno()).st_size == 0:
                stream.write(b"0")
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            stream.close()
            if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise RuntimeError("coordinator_already_running") from exc
            raise
        except BaseException:
            stream.close()
            raise
        self._file = stream

    def close(self):
        stream, self._file = self._file, None
        if stream is None:
            return
        try:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        finally:
            stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
