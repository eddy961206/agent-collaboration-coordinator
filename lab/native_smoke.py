"""Real native CLI protocol smoke; run only inside a network-none fixture container."""
import argparse
import json
from pathlib import Path
import socket
import threading
import time
import uuid

from coordinator.native_cli import NativeEngine
from lab.fake_model_api import FixtureServer


def wait(predicate, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise AssertionError('native fixture observation timed out')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--claude', required=True)
    parser.add_argument('--codex', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True)
    server = FixtureServer(('127.0.0.1', 0), out)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    base = 'http://127.0.0.1:' + str(server.server_address[1])
    results = []
    try:
        try:
            with socket.create_connection(('1.1.1.1', 443), timeout=1):
                raise AssertionError('fixture must have no external network')
        except OSError:
            pass
        for kind in ('claude', 'codex'):
            config = {'mode': 'fixture', 'network_isolated': True, 'base_url': base + ('/v1' if kind == 'codex' else ''), 'timeout': 12}
            engine = NativeEngine(kind, getattr(args, kind), out / kind, config)
            start_count = len(server.requests)
            server.hold = True; server.release.clear()
            try:
                run = {'id': uuid.uuid4().hex, 'task_id': 'native-smoke-' + kind, 'owner': 'fixture',
                       'adapter': kind, 'revision': 1, 'generation': 1, 'text': 'INITIAL_NATIVE_R1'}
                handle = engine.start(run)
                wait(lambda: len(server.requests) > start_count)
                first = engine.observe(handle)
                print(json.dumps({'stage': 'started', 'kind': kind, 'observation': first}), flush=True)
                receipt = engine.deliver(handle, {'revision': 2, 'text': 'CHANGED_NATIVE_R2'})
                stopped = engine.stop(handle)
                assert stopped['stopped'], stopped
                server.hold = False; server.release.set()
                resumed = engine.start({**run, 'id': uuid.uuid4().hex, 'revision': 3, 'generation': 3,
                                         'text': 'RESUMED_NATIVE_R3', 'resume_session': handle})
                done = wait(lambda: engine.observe(resumed) if engine.observe(resumed)['stopped'] else None)
                assert done['status'] == 'completed', done
                assert done['session_id'] == first['session_id'], done
                inputs = server.requests[start_count:]
                assert any('RESUMED_NATIVE_R3' in json.dumps(row) for row in inputs)
                result = {'kind': kind, 'first': first, 'change': receipt, 'stopped': stopped,
                          'resumed': done, 'fake_requests': len(inputs), 'model_calls': 0}
                results.append(result)
                (out / 'native-smoke.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
                print(json.dumps({'stage': 'verified', 'kind': kind, 'same_native_session': True, 'fake_requests': len(inputs), 'model_calls': 0}), flush=True)
            finally:
                engine.close()
    finally:
        server.release.set(); server.shutdown(); server.server_close(); thread.join()


if __name__ == '__main__':
    main()
