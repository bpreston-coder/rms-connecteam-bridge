"""Offline-reviewed 4071 repair. Leaves a durable hold; never invokes sync.

The ten deletion IDs MUST be filled from the approved report and code-reviewed.
An empty allowlist deliberately disables planning/apply. No date-based guessing.
Run a dry run on the production host, review its manifest, then pass that exact
JSON file and digest to apply. The hold-aware poller must already be deployed.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path

import app
import recovery_support
from bridge_safety import RecoveryRequired

OPPORTUNITY = 4071
ORDER = '100004056'
OPERATOR_KEY = '126320'
OPERATOR_JOB = 'c770947e-f933-4892-9df9-83b20cb7f5b2'
MISSING = '6acf25be828980cd742a5f45'
REPLACEMENT = '6ad0a53a12f088c2ef1811fa'
OTHER_OPERATOR = '6acf28b67e4764842fac60dc'
IDENTITY = 'Bridge identity: 32b6440a4326421ba0742b7427f27d52'
RETAIN = {'125886': ('6acf25b8828980cd742a5f3d', '2026-10-26T08:00:00+08:00'),
          '125887': ('6acf25bc828980cd742a5f41', '2026-10-28T14:00:00+08:00')}
DELETE_IDS: tuple[str, ...] = ()  # Pending exact ten-ID manifest from approved Mac report.
JOURNAL_KEY = 'sandalford_4071_v1'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise RecoveryRequired(message)


def check_allowlist():
    protected = {MISSING, REPLACEMENT, OTHER_OPERATOR, *(v[0] for v in RETAIN.values())}
    require(len(DELETE_IDS) == 10 and len(set(DELETE_IDS)) == 10
            and not protected.intersection(DELETE_IDS), 'Reviewed exact ten deletion IDs required')


def scope(state):
    """All opportunity mappings and intents; unrelated polling may progress."""
    return {name: {k: copy.deepcopy(v) for k, v in state.get(name, {}).items()
                   if v.get('opportunityId') == OPPORTUNITY}
            for name in ('shifts', 'pending_creates')}


def owners(state, sid):
    return sorted(k for k, e in state['shifts'].items() if sid in e.get('shiftIds', []))


def source_snapshot(client):
    opportunity = app.fetch_opportunity(client, OPPORTUNITY)
    require(opportunity.get('id') == OPPORTUNITY and str(opportunity.get('number')) == ORDER
            and opportunity.get('state_name') == 'Order'
            and opportunity.get('status_name') not in {'Cancelled', 'Completed', 'Dead', 'Lost', 'Postponed'},
            '4071 source identity/status changed')
    services = recovery_support.fetch_service_items(client, OPPORTUNITY)
    require(len({s['id'] for s in services}) == len(services), 'Duplicate source service IDs')
    return {'opportunity': opportunity, 'services': services}


def identity(row, key):
    require(isinstance(row, dict) and row.get('id'), 'Required destination missing')
    notes = app._note_html(row)
    require(f'(opportunity item #{key})' in notes, 'Destination item identity differs')
    fields = row.get('customFields')
    require(isinstance(fields, list) and all(isinstance(f, dict) and 'customFieldId' in f for f in fields),
            'Malformed destination custom fields')
    by_id = {str(f['customFieldId']): f.get('value') for f in fields}
    require(len(by_id) == len(fields), 'Duplicate destination custom fields')
    require(by_id.get(str(app.CONNECTEAM_JOBNO_CUSTOM_FIELD_ID)) == ORDER, 'Destination order differs')
    return by_id


def stable(row):
    """Compare every returned field except server modification timestamps.

    Unknown fields remain strict. Notes and every staffing/publication field
    remain byte-for-byte equivalent JSON values; no manual-content rebuilding.
    """
    if row is None:
        return None
    return {k: v for k, v in row.items() if k not in {'updatedAt', 'modifiedAt', 'updateTime'}}


class RepairClient(app.ReadRetryClient):
    """No creates, chat, source writes or mutations outside the frozen operation."""
    approved_operation = None

    def request(self, method, url, **kwargs):
        if method.upper() != 'GET':
            op = self.approved_operation
            endpoint = f'{app.CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{app.CONNECTEAM_SCHEDULER_ID}/shifts'
            valid = op is not None and (
                (op['kind'] == 'delete' and op['id'] in DELETE_IDS and method.upper() == 'DELETE'
                 and str(url) == f"{endpoint}/{op['id']}")
                or (op['kind'] == 'update' and op['id'] in {v[0] for v in RETAIN.values()}
                    and method.upper() == 'PUT' and str(url) == endpoint
                    and kwargs.get('json') == [op['payload']]
                    and kwargs.get('params') == {'notifyUsers': 'false'}))
            require(valid, 'Repair blocked outbound mutation outside approved operation')
        return super().request(method, url, **kwargs)


def build_plan(client, state):
    check_allowlist()
    app._opportunity_hold(state, OPPORTUNITY)
    app._assert_unique_ownership(state, OPPORTUNITY)
    before = scope(state)
    require(not before['pending_creates'], 'Resolve pending 4071 creates before repair')
    require(not any(e.get('recoveryValidationPending') for e in before['shifts'].values()),
            'Resolve other validation intents before repair')
    source = source_snapshot(client)
    services = {str(s['id']): s for s in source['services']}
    require(set(RETAIN).union({OPERATOR_KEY}) <= services.keys(), 'Required source service missing')
    require(all(services[key].get('status_name') not in
                {'Cancelled', 'Completed', 'Dead', 'Lost', 'Postponed'}
                for key in (*RETAIN, OPERATOR_KEY)), 'Target service inactive')
    op_entry = before['shifts'].get(OPERATOR_KEY)
    require(op_entry and op_entry.get('jobId') == OPERATOR_JOB and op_entry.get('quantity') == 2,
            'Operator tracked job/quantity differs')
    require(float(services[OPERATOR_KEY].get('quantity') or 0) == 2,
            'Operator source quantity differs')
    require(set(op_entry.get('shiftIds', [])) == {MISSING, OTHER_OPERATOR},
            'Operator tracked IDs differ')
    require(owners(state, MISSING) == [OPERATOR_KEY] and owners(state, OTHER_OPERATOR) == [OPERATOR_KEY]
            and not owners(state, REPLACEMENT), 'Operator ownership differs')
    require(app.get_shift(client, MISSING) is None, 'Previously missing operator exists again')
    expanded, skipped, _ = app._expand_service_segments([services[OPERATOR_KEY]])
    require(not skipped and len(expanded) == 1 and expanded[0][0] == OPERATOR_KEY,
            'Operator source layout differs')
    _, _, operator_start, operator_end = expanded[0]
    require((op_entry.get('startTime'), op_entry.get('endTime')) == (operator_start, operator_end),
            'Operator source times differ from mapping')
    operator_rows = {}
    for sid, user in ((REPLACEMENT, 9971217), (OTHER_OPERATOR, 18210921)):
        row = app.get_shift(client, sid)
        fields = identity(row, OPERATOR_KEY)
        require(row.get('assignedUserIds') == [user] and row.get('isPublished') is True
                and row.get('jobId') == OPERATOR_JOB
                and fields.get(str(app.CONNECTEAM_QTY_CUSTOM_FIELD_ID)) == '2'
                and (row.get('startTime'), row.get('endTime')) == (operator_start, operator_end),
                'Operator staffing/job/time/quantity differs')
        if sid == REPLACEMENT:
            require(IDENTITY in app._note_html(row), 'Replacement bridge identity differs')
        operator_rows[sid] = stable(row)
    final = copy.deepcopy(before['shifts'])
    final[OPERATOR_KEY]['shiftIds'] = [REPLACEMENT if sid == MISSING else sid
                                      for sid in op_entry['shiftIds']]
    operations = []
    found_deletions = set()
    for key, (sid, timestamp) in RETAIN.items():
        service = services[key]
        start = app._to_epoch_seconds(timestamp)
        require(app._transport_arrival_time(service) == start, 'Approved source arrival milestone differs')
        keys = [k for k in before['shifts'] if k.split(':')[0] == key]
        require(keys and len(owners(state, sid)) == 1 and owners(state, sid)[0] in keys,
                'Canonical transport ownership differs')
        entry = before['shifts'][owners(state, sid)[0]]
        row = app.get_shift(client, sid)
        identity(row, key)
        require(row.get('jobId') == entry.get('jobId'), 'Canonical transport job differs')
        require(row.get('isPublished') is (key == '125886')
                and isinstance(row.get('assignedUserIds'), list)
                and (bool(row['assignedUserIds']) if key == '125886' else row['assignedUserIds'] == []),
                'Canonical transport publication/assignment differs')
        for k in keys:
            for old_id in before['shifts'][k].get('shiftIds', []):
                require(old_id == sid or old_id in DELETE_IDS, 'Unexpected transport destination in mapping')
                if old_id == sid:
                    continue
                require(owners(state, old_id) == [k], 'Deletion ownership differs')
                old = app.get_shift(client, old_id)
                identity(old, key)
                require(old.get('isPublished') is False and old.get('assignedUserIds') == []
                        and old.get('jobId') == before['shifts'][k].get('jobId')
                        and (old.get('startTime'), old.get('endTime')) ==
                            (before['shifts'][k].get('startTime'), before['shifts'][k].get('endTime')),
                        'Deletion draft identity/time/staffing differs')
                operations.append({'kind': 'delete', 'id': old_id, 'sourceKey': k,
                                   'before': stable(old), 'after': None})
                found_deletions.add(old_id)
        local = app.datetime.fromtimestamp(start, tz=app.PERTH_TZ)
        clock = f"{local.hour % 12 or 12}:{local.minute:02d}{'am' if local.hour < 12 else 'pm'}"
        arrival = f'On site at {clock} sharp (AWST). The one-minute slot represents arrival only, not work duration.'
        notes = copy.deepcopy(row.get('notes'))
        require(isinstance(notes, list) and all(isinstance(n, dict) and isinstance(n.get('html'), str) for n in notes),
                'Canonical notes cannot be safely preserved')
        require('On site at ' not in app._note_html(row) or arrival in app._note_html(row),
                'Conflicting existing arrival note requires review')
        if arrival not in app._note_html(row):
            notes.append({'type': 'html', 'html': f'<p>{arrival}</p>'})
        payload = {'shiftId': sid, 'startTime': start, 'endTime': start + 60, 'notes': notes}
        after = {**stable(row), 'startTime': start, 'endTime': start + 60, 'notes': notes}
        operations.insert(0, {'kind': 'update', 'id': sid, 'sourceKey': owners(state, sid)[0],
                              'before': stable(row), 'after': after, 'payload': payload})
        # Preserve notification/identity metadata from the retained mapping.
        # Only schedule/arrival change; fail if any other source-driven diff exists.
        require((entry.get('description') or '') == (service.get('description') or '').strip()
                and entry.get('quantity') == max(1, int(float(service.get('quantity') or 1))),
                'Canonical source metadata needs separate review')
        replacement = {**copy.deepcopy(entry), 'shiftIds': [sid], 'startTime': start,
                       'endTime': start + 60, 'arrivalNote': arrival}
        for k in keys:
            del final[k]
        final[key] = replacement
    require(found_deletions == set(DELETE_IDS), 'Deletion allowlist does not exactly match obsolete mappings')
    return {'version': 1, 'opportunityId': OPPORTUNITY, 'source': source,
            'beforeScope': before, 'afterScope': {'shifts': final, 'pending_creates': {}},
            'operators': operator_rows, 'operations': operations}


def check_ownership(state, plan, *, final=False):
    expected = plan['afterScope' if final else 'beforeScope']['shifts']
    affected = {MISSING, REPLACEMENT, OTHER_OPERATOR, *DELETE_IDS, *(v[0] for v in RETAIN.values())}
    for sid in affected:
        require(owners(state, sid) == sorted(k for k, e in expected.items() if sid in e.get('shiftIds', [])),
                'Destination ownership changed since review')


def verify_operators(client, plan):
    require(app.get_shift(client, MISSING) is None, 'Missing operator reappeared')
    for sid, expected in plan['operators'].items():
        require(stable(app.get_shift(client, sid)) == expected, 'Operator changed since review')


def apply_plan(client, state, plan, review_digest):
    """Caller holds production lock. Journal persists before each outbound write."""
    check_allowlist()
    require(digest(plan) == review_digest, 'Review digest differs')
    journals = state.setdefault('repair_journals', {})
    journal = journals.get(JOURNAL_KEY)
    if journal is None:
        require(scope(state) == plan['beforeScope'], 'State changed since review')
        # Rebuild every source/state/destination precondition before installing intent.
        require(build_plan(client, state) == plan, 'Live preflight changed since review')
        require(not app._opportunity_hold(state, OPPORTUNITY), 'Existing hold needs separate review')
        state.setdefault('opportunity_holds', {})[str(OPPORTUNITY)] = {
            'reason': 'Coordinated Sandalford repair; release requires convergence review',
            'journal': JOURNAL_KEY, 'digest': review_digest}
        journal = {'digest': review_digest, 'plan': copy.deepcopy(plan), 'confirmed': [],
                   'intent': None, 'status': 'in_progress'}
        journals[JOURNAL_KEY] = journal
        app._save_state(state)
    require(journal.get('digest') == review_digest and journal.get('plan') == plan,
            'Journal does not match reviewed plan')
    hold = state.get('opportunity_holds', {}).get(str(OPPORTUNITY), {})
    require(hold.get('journal') == JOURNAL_KEY and hold.get('digest') == review_digest,
            'Repair hold missing or changed')
    final = journal.get('status') == 'verified_held'
    require(scope(state) == plan['afterScope' if final else 'beforeScope'], 'Repair state changed')
    check_ownership(state, plan, final=final)
    require(source_snapshot(client) == plan['source'], 'Source changed since review')
    verify_operators(client, plan)
    # Check ALL rows before resuming, not merely the next operation.
    for op in plan['operations']:
        live = stable(app.get_shift(client, op['id']))
        allowed = [op['after']] if op['id'] in journal['confirmed'] else [op['before']]
        if journal['intent'] == op['id']:
            allowed.append(op['after'])
        require(live in allowed, 'Destination drift outside journaled operation')
    for op in plan['operations']:
        if op['id'] in journal['confirmed']:
            continue
        live = stable(app.get_shift(client, op['id']))
        require(live == op['before'] or (journal['intent'] == op['id'] and live == op['after']),
                'Destination changed immediately before operation')
        if live != op['after']:
            journal['intent'] = op['id']
            app._save_state(state)
            client.approved_operation = op
            try:
                if op['kind'] == 'update':
                    app.update_shifts(client, [op['payload']])
                else:
                    app.delete_shift(client, op['id'])
            finally:
                client.approved_operation = None
            # Failure/timeout leaves intent and hold intact; rerun GET reconciles.
        require(stable(app.get_shift(client, op['id'])) == op['after'], 'Operation read-back differs')
        journal['confirmed'].append(op['id'])
        journal['intent'] = None
        app._save_state(state)
    require(source_snapshot(client) == plan['source'], 'Source changed during repair')
    verify_operators(client, plan)
    for op in plan['operations']:
        require(stable(app.get_shift(client, op['id'])) == op['after'], 'Final destination read-back differs')
    # Atomic state migration only after every remote operation is confirmed.
    require(scope(state) == plan['afterScope' if final else 'beforeScope'], 'State changed before migration')
    check_ownership(state, plan, final=final)
    for key in list(state['shifts']):
        if state['shifts'][key].get('opportunityId') == OPPORTUNITY:
            del state['shifts'][key]
    state['shifts'].update(copy.deepcopy(plan['afterScope']['shifts']))
    journal['status'] = 'verified_held'
    app._save_state(state)
    return {'status': 'verified_held', 'reviewDigest': review_digest,
            'confirmed': list(journal['confirmed']), 'holdReleased': False}


def run(*, plan=None, review_digest=None):
    require(getattr(app, 'OPPORTUNITY_HOLD_POLICY_VERSION', None) == 1, 'Hold-aware poller code required')
    with app._sync_guard():
        require(app.STATE_FILE.is_file(), 'Existing production state required')
        state = app._load_state()
        with RepairClient(timeout=30) as client:
            # Staged source files alone cannot prove the old live process stopped.
            # Render runs this helper beside the single poller on its local port.
            port = int(os.environ.get('PORT', '10000'))
            readiness = client.get(f'http://127.0.0.1:{port}/readyz').json()
            require(readiness.get('opportunity_hold_policy_version') == 1,
                    'Running poller has not confirmed durable hold support')
            if review_digest is None:
                manifest = build_plan(client, state)
                return {'reviewDigest': digest(manifest), 'plan': manifest}
            require(isinstance(plan, dict), 'Exact reviewed plan required')
            return apply_plan(client, state, plan, review_digest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan-file', type=Path)
    parser.add_argument('--apply-digest')
    args = parser.parse_args()
    require(bool(args.plan_file) == bool(args.apply_digest), 'Apply requires both plan file and digest')
    manifest = json.loads(args.plan_file.read_text())['plan'] if args.plan_file else None
    print(json.dumps(run(plan=manifest, review_digest=args.apply_digest), indent=2))


if __name__ == '__main__':
    main()
