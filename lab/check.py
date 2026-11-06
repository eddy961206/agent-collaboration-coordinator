"""Run the repository's actual tests and save a compact, reproducible receipt."""
import argparse
import io
import json
import platform
from pathlib import Path
import sys
import unittest

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='.runtime/unit-summary.json')
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    log = io.StringIO()
    suite = unittest.defaultTestLoader.discover('tests')
    result = unittest.TextTestRunner(stream=log, verbosity=2).run(suite)
    output.with_suffix('.log').write_text(log.getvalue(), encoding='utf-8')
    summary = {'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors),
               'skipped': len(result.skipped), 'passed': result.wasSuccessful(),
               'platform': platform.system(), 'python': platform.python_version(),
               'scope': 'SQLite, threads, processes, local fixture HTTP; no production services or model calls',
               'model_calls': 0}
    output.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(summary))
    if not result.wasSuccessful():
        print(log.getvalue(), file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
