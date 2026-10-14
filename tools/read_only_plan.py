"""Run the current sync planner with GET-only transport and all writes replaced.

Run from the repository: python tools/read_only_plan.py
Only aggregate effects and source IDs are printed. No destination writes or Chat.
"""
import copy
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app


class ReadOnlyClient(app.ReadRetryClient):
    def request(self, method, url, **kwargs):
        if method.upper() != 'GET':
            raise RuntimeError('Read-only audit blocked an outbound mutation')
        return super().request(method, url, **kwargs)


def main():
    logging.disable(logging.CRITICAL)
    baseline = app._load_state()
    opportunity_ids = sorted({e['opportunityId'] for e in baseline['shifts'].values()})
    results = []
    with ReadOnlyClient(timeout=30) as client:
        for opportunity_id in opportunity_ids:
            state = copy.deepcopy(baseline)
            effects = Counter()
            update_ids = []
            def create(_client, payloads):
                effects['creates'] += len(payloads)
                return [{'id': f'audit-only-{effects["creates"]}-{i}', 'isPublished': False} for i, _ in enumerate(payloads)]
            def update(_client, payloads):
                effects['updates'] += len(payloads)
                update_ids.extend(str(p['shiftId']) for p in payloads)
                return [{'id': p['shiftId'], 'isPublished': False} for p in payloads]
            def delete(*_): effects['deletes'] += 1
            def notify(*_): effects['notifications'] += 1
            with patch.object(app, '_save_state'), patch.object(app, 'create_shifts', side_effect=create), patch.object(app, 'update_shifts', side_effect=update), patch.object(app, 'delete_shift', side_effect=delete), patch.object(app, 'notify_ops', side_effect=notify):
                try:
                    result = app.sync_opportunity(client, opportunity_id, state)
                    results.append({'opportunity': opportunity_id, 'result': result, 'effects': dict(effects), 'update_ids': update_ids, 'exception': state.get('exceptions', {}).get(str(opportunity_id))})
                except Exception as exc:
                    results.append({'opportunity': opportunity_id, 'error': type(exc).__name__, 'detail': str(exc) if isinstance(exc, app.RecoveryRequired) else None, 'effects': dict(effects)})
    print(json.dumps({'read_only': True, 'tracked_entries': len(baseline['shifts']), 'opportunities': results}))


if __name__ == '__main__':
    main()
