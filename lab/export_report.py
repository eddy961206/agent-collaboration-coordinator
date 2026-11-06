"""Export the final lab receipt without fixture credentials, tokens or transcripts."""
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
session = json.loads((root / '.runtime' / 'lab-session.json').read_text(encoding='utf-8'))
raw = json.loads((Path(session['out']) / 'integration-results.json').read_text(encoding='utf-8'))
if raw.get('passed') != raw.get('total') or not raw.get('total'):
    raise SystemExit('The latest integration run did not finish successfully.')
for name in ('unit-windows.json', 'unit-linux.json'):
    summary = json.loads((root / 'reports' / name).read_text(encoding='utf-8'))
    if not summary['passed']:
        raise SystemExit(name + ' did not pass')
summary = {key: raw[key] for key in ('scope', 'passed', 'total', 'model_calls', 'gitea_version', 'rt_binary_sha256', 'limits')}
summary.update({'lab_id': session['id'], 'python_image': session['python_image'],
                'gitea_image': session['gitea_image'],
                'rt_source_commit': '383e02d6dc2feed92865baeb0443c9bc37deea63',
                'checks': [{'name': row['name'], 'passed': row['passed']} for row in raw['checks']]})
(root / 'reports' / 'integration-summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(json.dumps({'passed': summary['passed'], 'total': summary['total'], 'lab': session['id']}))
