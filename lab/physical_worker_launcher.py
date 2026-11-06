"""Standalone fixture worker for the two-physical-host test; no model code."""
import argparse
import json
import os
import platform
from pathlib import Path
import signal
import threading

from coordinator.adapters import DirectAdapter
from coordinator.worker_host import WorkerHost, make_worker_server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    path = Path(args.config).resolve()
    config = json.loads(path.read_text(encoding='utf-8'))
    root = path.parent
    engine = DirectAdapter(root / 'fake-workspaces')
    host = WorkerHost(root / 'host-state', config['node_id'], config['token'],
                      {'fixture': engine}, config['owners'])
    server = make_worker_server(host, ('127.0.0.1', 0))
    ready = {'pid': os.getpid(), 'port': server.server_address[1],
             'node_id': config['node_id'], 'nonce': config['nonce'],
             'platform': platform.system(), 'python': platform.python_version()}
    temporary = root / 'ready.tmp'
    temporary.write_text(json.dumps(ready), encoding='utf-8')
    temporary.replace(root / 'ready.json')
    stopping = threading.Event()

    def request_stop(signum, frame):
        if not stopping.is_set():
            stopping.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        server.serve_forever(poll_interval=.1)
    finally:
        server.server_close()
        host.close()


if __name__ == '__main__':
    main()
