#!/usr/bin/env python3
"""Stream-json CLI fixture consumed by the real RT binary. No network/model code."""
import json
import os
from pathlib import Path
import signal
import sys
import uuid

if '--version' in sys.argv:
    print('2.1.0 (deterministic coordinator fixture; no AI)')
    raise SystemExit(0)

session = 'fixture-' + uuid.uuid4().hex
directory = Path('/out/rt/cli-observations')
directory.mkdir(parents=True, exist_ok=True)
path = directory / (str(os.getpid()) + '.jsonl')


def emit(value):
    print(json.dumps(value), flush=True)


def record(event, **fields):
    with path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps({'event': event, 'pid': os.getpid(), 'model_calls': 0, **fields}) + '\n')


def terminate(signum, frame):
    record('stopped', signal=signum)
    raise SystemExit(0)


signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)
record('started')
emit({'type': 'system', 'subtype': 'init', 'session_id': session})
for line in sys.stdin:
    value = json.loads(line)
    record('input', payload=value)
    emit({**value, 'session_id': session})
    text = json.dumps(value)
    if 'LAB_FINISH' in text:
        answer = 'FIXTURE_COMPLETE: deterministic output; model calls 0'
        emit({'type': 'assistant', 'session_id': session,
              'message': {'role': 'assistant', 'content': [{'type': 'text', 'text': answer}]}})
        emit({'type': 'result', 'subtype': 'success', 'is_error': False,
              'session_id': session, 'result': answer,
              'usage': {'input_tokens': 0, 'output_tokens': 0}, 'total_cost_usd': 0})
        record('completed')
        break
record('exited')
