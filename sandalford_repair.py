"""Offline-reviewed 4071 repair. Leaves a durable hold; never invokes sync.

The ten deletion IDs and their source items are frozen from the approved report.
Publication and production execution remain separately gated. No date-based guessing.
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
from repair_notes import arrival_variants, visible_arrival_html

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
DELETE_BY_ITEM = {
    '125886': (
        '6acf25b627228e4c71521dcb', '6acf25b678aae02b9ecde568',
        '6acf25b778aae02b9ecde56a', '6acf25b82ecda2de8bd6b0ac',
        '6acf25b90991f240c218d535',
    ),
    '125887': (
        '6acf25ba828980cd742a5f3f', '6acf25ba78aae02b9ecde56c',
        '6acf25bb0991f240c218d537', '6acf25bb78aae02b9ecde56e',
        '6acf25bc0991f240c218d539',
    ),
}
DELETE_IDS = tuple(sid for ids in DELETE_BY_ITEM.values() for sid in ids)
JOURNAL_KEY = 'sandalford_4071_v1'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise RecoveryRequired(message)


def check_allowlist():
    protected = {MISSING, REPLACEMENT, OTHER_OPERATOR, *(v[0] for v in RETAIN.values())}
    require(len(DELETE_IDS) == 10 and len(set(DELETE_IDS)) == 10
            and not protected.intersection(DELETE_IDS)
            and set(DELETE_BY_ITEM) == set(RETAIN)
            and all(len(ids) == 5 for ids in DELETE_BY_ITEM.values())
            and tuple(sid for ids in DELETE_BY_ITEM.values() for sid in ids) == DELETE_IDS,
            'Reviewed exact ten deletion IDs and source items required')


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
                require(old_id == sid or old_id in DELETE_BY_ITEM[key],
                        'Unexpected transport destination/source item in mapping')
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
        owned_arrival = visible_arrival_html(key, arrival)
        require('On site at ' not in app._note_html(row) or
                (owned_arrival in app._note_html(row) and app._note_html(row).count('On site at ') == 1),
                'Existing unowned or conflicting arrival note requires review')
        if arrival not in app._note_html(row):
            notes.append({'type': 'html', 'html': owned_arrival})
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


def review_amendment(client, state, plan, review_digest):
    """GET-only evidence for the single known collection PUT stopped on sanitization."""
    check_allowlist()
    journal = state.get('repair_journals', {}).get(JOURNAL_KEY, {})
    collection_id = RETAIN['125887'][0]
    require(digest(plan) == review_digest and journal.get('digest') == review_digest
            and journal.get('plan') == plan, 'Original manifest/journal mismatch')
    require(journal.get('status') == 'in_progress' and journal.get('confirmed') == []
            and journal.get('intent') == collection_id and 'amendment' not in journal,
            'Amendment supports only the first unconfirmed collection intent')
    hold = state.get('opportunity_holds', {}).get(str(OPPORTUNITY), {})
    require(hold.get('journal') == JOURNAL_KEY and hold.get('digest') == review_digest,
            'Original durable hold changed')
    require(scope(state) == plan['beforeScope'], 'Original state mapping changed')
    check_ownership(state, plan)
    require(source_snapshot(client) == plan['source'], 'Source changed before amendment')
    verify_operators(client, plan)
    operations = copy.deepcopy(plan['operations'])
    require([op['id'] for op in operations[:2]] == [collection_id, RETAIN['125886'][0]]
            and all(op['kind'] == 'delete' for op in operations[2:]), 'Original operation order differs')
    for op in operations:
        live = stable(app.get_shift(client, op['id']))
        if op['id'] == collection_id:
            variants = arrival_variants(op)
            require(bool(variants), 'Original collection appended paragraph is not proven')
            sanitized = copy.deepcopy(op['after'])
            sanitized['notes'][-1]['html'] = variants[1]
            require(live == sanitized, 'Collection differs beyond the single stripped attribute')
            op['before'] = live
        else:
            require(live == op['before'], 'An untouched destination changed before amendment')
        if op['kind'] == 'update':
            variants = arrival_variants(next(original for original in plan['operations'] if original['id'] == op['id']))
            require(bool(variants), 'Original generated arrival paragraph is not proven')
            key = op['sourceKey'].split(':')[0]
            text = variants[1][3:-4]
            op['after']['notes'][-1]['html'] = visible_arrival_html(key, text)
            op['payload']['notes'] = copy.deepcopy(op['after']['notes'])
            if op['id'] == collection_id:
                # Its times already changed successfully. Only replace the proven
                # generated paragraph; preserve every original/manual note exactly.
                op['payload'] = {'shiftId': collection_id, 'notes': copy.deepcopy(op['after']['notes'])}
    amendment = {'version': 1, 'originalDigest': review_digest,
                 'originalJournal': copy.deepcopy(journal),
                 'reconciledIntent': collection_id, 'operations': operations}
    return {'amendmentDigest': digest(amendment), 'amendment': amendment}


def effective_operations(journal, plan):
    amendment = journal.get('amendment')
    if amendment is None:
        require(not any(arrival_variants(op) for op in plan['operations']),
                'Legacy attribute-based plan requires reviewed amendment')
        return plan['operations']
    require(digest(amendment) == journal.get('amendmentDigest')
            and amendment.get('originalDigest') == journal.get('digest')
            and amendment['originalJournal']['plan'] == plan,
            'Amendment integrity check failed')
    return amendment['operations']


def apply_plan(client, state, plan, review_digest, *, amendment_digest=None):
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
    if amendment_digest is not None and 'amendment' not in journal:
        report = review_amendment(client, state, plan, review_digest)
        require(report['amendmentDigest'] == amendment_digest, 'Amendment review digest differs')
        # Preserve original journal verbatim inside amendment and original plan /
        # digest in place. Reconciled old PUT is never resent as a time update.
        journal['amendment'] = report['amendment']
        journal['amendmentDigest'] = amendment_digest
        journal['intent'] = None
        app._save_state(state)
    if 'amendment' in journal:
        require(amendment_digest == journal['amendmentDigest'], 'Resume requires reviewed amendment digest')
    operations = effective_operations(journal, plan)
    hold = state.get('opportunity_holds', {}).get(str(OPPORTUNITY), {})
    require(hold.get('journal') == JOURNAL_KEY and hold.get('digest') == review_digest,
            'Repair hold missing or changed')
    final = journal.get('status') == 'verified_held'
    require(scope(state) == plan['afterScope' if final else 'beforeScope'], 'Repair state changed')
    check_ownership(state, plan, final=final)
    require(source_snapshot(client) == plan['source'], 'Source changed since review')
    verify_operators(client, plan)
    # Check ALL rows before resuming, not merely the next operation.
    for op in operations:
        live = stable(app.get_shift(client, op['id']))
        after_allowed = op['id'] in journal['confirmed'] or journal['intent'] == op['id']
        before_allowed = op['id'] not in journal['confirmed']
        require((before_allowed and live == op['before']) or
                (after_allowed and live == op['after']), 'Destination drift outside journaled operation')
    # Additive verifier-policy checkpoint; preserve the original plan, digest,
    # intent and confirmations. A crash before this save performs no writes.
    require(journal.get('arrivalReadbackPolicy', 1) in (1, 2), 'Unknown arrival read-back policy')
    if journal.get('arrivalReadbackPolicy') != 2:
        journal['arrivalReadbackPolicy'] = 2
        app._save_state(state)
    for op in operations:
        if op['id'] in journal['confirmed']:
            continue
        if op['kind'] == 'delete':
            for canonical in operations[:2]:
                require(canonical['kind'] == 'update' and canonical['id'] in journal['confirmed']
                        and stable(app.get_shift(client, canonical['id'])) == canonical['after'],
                        'Both canonical read-backs must remain verified before deletion')
        check_ownership(state, plan)
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
    for op in operations:
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


class PreviewClient:
    """No outgoing writes, even if wrapped repair client has an approved intent."""
    def __init__(self, client):
        self.client = client

    def request(self, method, url, **kwargs):
        require(method.upper() == 'GET', 'Normal sync would mutate; hold cannot be released')
        return self.client.get(url, **kwargs)

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)


def normal_sync_converges(client, state):
    """Execute real sync logic on a private copy, with disk/network writes blocked."""
    preview = copy.deepcopy(state)
    preview['opportunity_holds'].pop(str(OPPORTUNITY), None)
    token = app._preview_state_writes.set(True)
    try:
        result = app.sync_opportunity(PreviewClient(client), OPPORTUNITY, preview)
    finally:
        app._preview_state_writes.reset(token)
    require(result.get('status') == 'ok' and result.get('created_count') == 0
            and result.get('updated_count') == 0 and result.get('skipped_count') == 0
            and not result.get('newly_revealed_item_ids')
            and not result.get('orphaned_items_cleaned_up'), 'Normal sync does not converge without actions')
    def mapping(value):
        result = scope(value)
        # Ordinary sync records this default even when there is no cancellation.
        for entry in result['shifts'].values():
            if entry.get('notificationCancelled') is False:
                entry.pop('notificationCancelled')
        return result
    require(mapping(preview) == mapping(state), 'Normal sync would change source ownership or tracked content')


def release_hold(client, state, plan, review_digest, *, release=False):
    """Caller holds shared lock. Review-only default; release changes local state only."""
    check_allowlist()
    journal = state.get('repair_journals', {}).get(JOURNAL_KEY, {})
    require(digest(plan) == review_digest and journal.get('digest') == review_digest
            and journal.get('plan') == plan, 'Release requires exact journaled review digest')
    operations = effective_operations(journal, plan)
    require(journal.get('status') in {'verified_held', 'released'} and journal.get('intent') is None
            and journal.get('arrivalReadbackPolicy') == 2
            and len(journal.get('confirmed', [])) == len(plan['operations'])
            and set(journal['confirmed']) == {op['id'] for op in plan['operations']},
            'Repair actions are not fully confirmed')
    already_released = journal['status'] == 'released'
    hold = state.get('opportunity_holds', {}).get(str(OPPORTUNITY))
    require((hold is None if already_released else
             isinstance(hold, dict) and hold.get('journal') == JOURNAL_KEY
             and hold.get('digest') == review_digest), 'Release hold ownership differs')
    require(scope(state) == plan['afterScope'] and not scope(state)['pending_creates'],
            'Final mapping or pending actions changed')
    check_ownership(state, plan, final=True)
    require(source_snapshot(client) == plan['source'], 'Source changed since repair')
    verify_operators(client, plan)
    for op in operations:
        require(stable(app.get_shift(client, op['id'])) == op['after'], 'Release destination verification failed')
    normal_sync_converges(client, state)
    # The preview can be long; recheck source and exact destinations afterwards.
    require(source_snapshot(client) == plan['source'], 'Source changed during convergence check')
    verify_operators(client, plan)
    for op in operations:
        require(stable(app.get_shift(client, op['id'])) == op['after'], 'Destination changed during convergence check')
    if release and not already_released:
        # One durable atomic save: a crash leaves either verified_held or released.
        next_state = copy.deepcopy(state)
        del next_state['opportunity_holds'][str(OPPORTUNITY)]
        next_state['repair_journals'][JOURNAL_KEY]['status'] = 'released'
        app._save_state(next_state)
        state.clear()
        state.update(next_state)
    return {'status': 'released' if release or already_released else 'release_ready',
            'reviewDigest': review_digest, 'holdReleased': bool(release or already_released)}


def run(*, plan=None, review_digest=None, check_release=False, release=False,
        check_amendment=False, amendment_digest=None):
    require(getattr(app, 'OPPORTUNITY_HOLD_POLICY_VERSION', None) == 1, 'Hold-aware poller code required')
    require(getattr(app, 'SANDALFORD_REPAIR_POLICY_VERSION', None) == 2, 'Sanitizer-safe owned-note policy required')
    with app._sync_guard():
        require(app.STATE_FILE.is_file(), 'Existing production state required')
        state = app._load_state()
        with RepairClient(timeout=30) as client:
            # Staged source files alone cannot prove the old live process stopped.
            # Render runs this helper beside the single poller on its local port.
            port = int(os.environ.get('PORT', '10000'))
            readiness = client.get(f'http://127.0.0.1:{port}/readyz').json()
            require(readiness.get('opportunity_hold_policy_version') == 1
                    and readiness.get('sandalford_repair_policy_version') == 2,
                    'Running poller has not confirmed hold and owned-note convergence support')
            if review_digest is None:
                manifest = build_plan(client, state)
                return {'reviewDigest': digest(manifest), 'plan': manifest}
            require(isinstance(plan, dict), 'Exact reviewed plan required')
            if check_amendment:
                return review_amendment(client, state, plan, review_digest)
            if check_release or release:
                return release_hold(client, state, plan, review_digest, release=release)
            return apply_plan(client, state, plan, review_digest, amendment_digest=amendment_digest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan-file', type=Path)
    parser.add_argument('--apply-digest')
    parser.add_argument('--check-release', action='store_true', help='GET-only convergence review; retain hold')
    parser.add_argument('--release-hold', action='store_true', help='Reverify convergence and atomically release hold')
    parser.add_argument('--review-amendment', action='store_true', help='GET-only review of the known sanitized collection intent')
    parser.add_argument('--amendment-digest', help='Apply/resume only the exact reviewed journal amendment')
    args = parser.parse_args()
    require(bool(args.plan_file) == bool(args.apply_digest), 'Apply requires both plan file and digest')
    require(not (args.check_release and args.release_hold), 'Choose check-release or release-hold')
    require(not args.review_amendment or not (args.check_release or args.release_hold or args.amendment_digest),
            'Amendment review cannot be combined with apply/release')
    require(not (args.review_amendment or args.amendment_digest) or bool(args.apply_digest), 'Amendment needs original plan/digest')
    require(not (args.check_release or args.release_hold) or bool(args.apply_digest), 'Release needs reviewed plan and digest')
    manifest = json.loads(args.plan_file.read_text())['plan'] if args.plan_file else None
    print(json.dumps(run(plan=manifest, review_digest=args.apply_digest,
                         check_release=args.check_release, release=args.release_hold,
                         check_amendment=args.review_amendment, amendment_digest=args.amendment_digest), indent=2))


if __name__ == '__main__':
    main()
