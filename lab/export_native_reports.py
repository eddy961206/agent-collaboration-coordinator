"""Publish only synthetic-test receipts, without native IDs or prompt transcripts."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--distributed', required=True)
    parser.add_argument('--smoke', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    distributed_path = Path(args.distributed) / 'native-distributed-summary.json'
    smoke_path = Path(args.smoke) / 'native-smoke.json'
    for path in (distributed_path, smoke_path):
        if not path.resolve().is_relative_to(root / '.runtime'):
            raise ValueError('only_local_fixture_artifacts')
    distributed = json.loads(distributed_path.read_text(encoding='utf-8'))
    if not distributed.get('total') or distributed['passed'] != distributed['total'] or distributed['model_calls'] != 0:
        raise ValueError('distributed_fixture_did_not_pass')
    smoke = json.loads(smoke_path.read_text(encoding='utf-8'))
    if {row['kind'] for row in smoke} != {'claude', 'codex'}:
        raise ValueError('both_native_engines_required')
    checks = []
    for row in smoke:
        passed = (row['model_calls'] == 0 and row['change']['delivered'] is True
                  and row['stopped']['stopped'] is True and row['resumed']['stopped'] is True
                  and row['resumed']['status'] == 'completed'
                  and row['first']['session_id'] == row['resumed']['session_id'])
        if not passed:
            raise ValueError('native_smoke_did_not_pass')
        checks.append({'engine': row['kind'], 'passed': True, 'same_owned_native_session_resumed': True,
                       'change_delivery_state': row['change'].get('delivery_state'),
                       'fake_provider_requests': row['fake_requests'], 'model_calls': 0})
    report = {'scope': 'Real native CLI start, transport update, stop, same-session resume and completion against local fake providers',
              'verified_at_utc': datetime.fromtimestamp(smoke_path.stat().st_mtime, timezone.utc).isoformat(),
              'network': 'Docker --network none, only loopback provider', 'model_calls': 0,
              'artifacts': {name: distributed['artifacts'][name] for name in ('claude', 'codex')},
              'checks': checks, 'limits': ['Not model reasoning quality', 'No existing Desktop session adoption', 'No real provider credential used']}
    reports = root / 'reports'; reports.mkdir(exist_ok=True)
    (reports / 'native-distributed-summary.json').write_text(json.dumps(distributed, indent=2), encoding='utf-8')
    (reports / 'native-smoke-summary.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({'distributed_checks': distributed['total'], 'native_engines': len(checks), 'model_calls': 0}))


if __name__ == '__main__':
    main()
