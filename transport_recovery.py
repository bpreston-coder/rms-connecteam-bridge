"""Reviewed four-item recovery; never invokes opportunity sync or notifications."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json

import app
import recovery_support
from bridge_safety import RecoveryRequired

# Explicit approval boundary. Additional items require code review, not CLI input.
TARGETS = {4104: (126172, 126173), 4095: (125805, 125847)}
INACTIVE = {'Completed', 'Cancelled', 'Postponed', 'Dead', 'Lost'}


class RecoveryClient(app.ReadRetryClient):
    """GET-only by default; apply permits single approved draft creates only."""
    allow_create = False

    def request(self, method, url, **kwargs):
        if method.upper() != 'GET':
            rows = kwargs.get('json')
            allowed_ids = {str(i) for ids in TARGETS.values() for i in ids}
            endpoint = f'{app.CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{app.CONNECTEAM_SCHEDULER_ID}/shifts'
            if not (self.allow_create and method.upper() == 'POST' and str(url) == endpoint
                    and isinstance(rows, list) and len(rows) == 1
                    and rows[0].get('isPublished') is False
                    and not rows[0].get('assignedUserIds')
                    and kwargs.get('params', {}).get('notifyUsers') == 'false'
                    and any(f'(opportunity item #{key})' in app._note_html(rows[0]) for key in allowed_ids)):
                raise RecoveryRequired('Item recovery blocked an unapproved outbound mutation')
        return super().request(method, url, **kwargs)


def _verify_live(row, desired, key, *, fresh_payload=None, identity=None):
    if not row or not row.get('id'):
        raise RecoveryRequired(f'Target {key}: tracked destination missing')
    for field in ('startTime', 'endTime', 'title', 'jobId'):
        if row.get(field) != desired[field]:
            raise RecoveryRequired(f'Target {key}: destination {field} differs; no update permitted')
    notes = app._note_html(row)
    if f'(opportunity item #{key})' not in notes or desired['arrivalNote'] not in notes:
        raise RecoveryRequired(f'Target {key}: destination identity/arrival note differs')

    if fresh_payload is not None:
        if row.get('isPublished') is not False or row.get('assignedUserIds') != []:
            raise RecoveryRequired(f'Target {key}: fresh create is not an unassigned draft')
        def fields(value):
            if not isinstance(value, list) or any(
                not isinstance(f, dict) or 'customFieldId' not in f or 'value' not in f
                for f in value
            ):
                raise RecoveryRequired(f'Target {key}: missing custom-field read-back')
            result = {str(f['customFieldId']): f.get('value') for f in value}
            if len(result) != len(value):
                raise RecoveryRequired(f'Target {key}: duplicate custom-field read-back')
            return result
        actual_fields = fields(row.get('customFields'))
        expected_fields = fields(fresh_payload['customFields'])
        # Connecteam may omit this one explicitly empty text field on read-back.
        # This does not establish account-specific UI requiredness or omit it on write.
        # Never normalize null, whitespace, nonempty values, or any other field.
        if expected_fields.get('1353115') == '' and '1353115' not in actual_fields:
            actual_fields['1353115'] = ''
        if actual_fields != expected_fields:
            raise RecoveryRequired(f'Target {key}: fresh custom fields differ')
        expected_location = fresh_payload.get('locationData')
        actual_location = row.get('locationData') or {}
        if actual_location and actual_location.get('isReferencedToJob') is not False:
            raise RecoveryRequired(f'Target {key}: fresh location reference differs')
        if expected_location is None or expected_location['gps'] is None:
            location_matches = not actual_location.get('gps')
        else:
            location_matches = (actual_location.get('gps') or {}).get('address') == expected_location['gps']['address']
        if not location_matches:
            raise RecoveryRequired(f'Target {key}: fresh location differs')
        if 'color' in fresh_payload and row.get('color') != fresh_payload['color']:
            raise RecoveryRequired(f'Target {key}: fresh color differs')
        if any(n['html'] not in notes for n in fresh_payload['notes']) or not identity or identity not in notes:
            raise RecoveryRequired(f'Target {key}: fresh generated notes/identity differ')


def _scan_destinations(client, start, end):
    """Exhaust the reviewed date range, including short pages, before any create."""
    rows, seen = [], set()
    for _ in range(200):
        response = client.get(
            f'{app.CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{app.CONNECTEAM_SCHEDULER_ID}/shifts',
            headers={'X-API-KEY': app.CONNECTEAM_API_KEY},
            params={'startTime': start, 'endTime': end, 'limit': 500,
                    'offset': len(rows), 'sort': 'created_at', 'order': 'asc'})
        response.raise_for_status()
        batch = response.json()['data']['shifts']
        if not isinstance(batch, list):
            raise RecoveryRequired('Invalid destination scan response')
        if not batch:
            return rows
        ids = [str(row['id']) for row in batch]
        if len(set(ids)) != len(ids) or seen.intersection(ids):
            raise RecoveryRequired('Destination scan overlapped or did not advance')
        rows.extend(batch)
        seen.update(ids)
    raise RecoveryRequired('Destination scan reached page cap; no writes allowed')


def plan_recovery(client, state, scan_start, scan_end):
    """GET-only preflight of all four targets before any apply writes."""
    destinations = _scan_destinations(client, scan_start, scan_end)
    by_title, colors = app._load_job_title_cache(client)
    cache = {'by_title': dict(by_title), 'by_jobid_color': dict(colors)}
    plans = []
    for opportunity_id, item_ids in TARGETS.items():
        app._assert_unique_ownership(state, opportunity_id)
        opportunity = app.fetch_opportunity(client, opportunity_id)
        if opportunity.get('id') != opportunity_id:
            raise RecoveryRequired('Unexpected opportunity identity')
        eligible, _ = app._is_eligible(opportunity)
        if not eligible or opportunity.get('status_name') in INACTIVE:
            raise RecoveryRequired(f'Opportunity {opportunity_id}: not active/eligible')
        services = recovery_support.fetch_service_items(client, opportunity_id)
        address = app.fetch_venue_address(client, opportunity)
        contact = ((opportunity.get('custom_fields') or {}).get(
            'on-site_contact_if_different_from_project_contact') or '').strip()
        contact = contact or app.fetch_member_name(client, opportunity.get('owned_by')) or ''
        subject = opportunity.get('subject') or f"Order {opportunity.get('number', opportunity_id)}"
        title = subject.split('#', 1)[1].strip() if '#' in subject else subject
        for item_id in item_ids:
            key = str(item_id)
            matches = [s for s in services if s.get('id') == item_id]
            if len(matches) != 1:
                raise RecoveryRequired(f'Target {key}: source absent or ambiguous')
            service = matches[0]
            start = app._transport_arrival_time(service)
            if start is None or service.get('status_name') in INACTIVE:
                raise RecoveryRequired(f'Target {key}: not an active equal-time transport service')
            job_id = app.find_job_by_service_name(client, service['name'], cache)
            if not job_id:
                raise RecoveryRequired(f'Target {key}: destination job not found')
            _, _, _, _, desired, fields, notes = recovery_support._service_shift_content(
                opportunity, service, start, start + 60, job_id,
                app.get_job_color(job_id, cache), address, contact, title)
            payload = dict(startTime=start, endTime=start+60, title=title,
                           isPublished=False, jobId=job_id, customFields=fields, notes=notes)
            if desired['color']:
                payload['color'] = desired['color']
            # locationData is optional in the create API. Do not send a null
            # gps override while investigating the reported literal "None" address.
            # Read-back still rejects inherited/nonempty GPS for an absent source.
            if address:
                payload['locationData'] = {'isReferencedToJob': False,
                                           'gps': {'address': address}}
            entries = {k: e for k, e in state['shifts'].items() if k.split(':')[0] == key}
            pending = state.get('pending_creates', {}).get(key)
            if any(k.split(':')[0] == key and k != key for k in state.get('pending_creates', {})):
                raise RecoveryRequired(f'Target {key}: pending split layout requires review')
            if entries and (set(entries) != {key} or entries[key].get('opportunityId') != opportunity_id or pending):
                raise RecoveryRequired(f'Target {key}: conflicting tracking/layout')
            if not scan_start <= start < start + 60 <= scan_end:
                raise RecoveryRequired(f'Target {key}: outside reviewed destination scan')
            # Also hold unmarked lookalikes; never guess an existing shift's identity.
            candidates = [r for r in destinations if f'(opportunity item #{key})' in app._note_html(r)
                          or (r.get('startTime') == start and r.get('jobId') == job_id
                              and r.get('title') == title)]
            action = 'create'
            if entries:
                entry = entries[key]
                ids = entry.get('shiftIds', [])
                if not ids or any(entry.get(k) != v for k, v in desired.items()):
                    raise RecoveryRequired(f'Target {key}: existing mapping differs; no update permitted')
                if any(str(r['id']) not in set(map(str, ids)) for r in candidates):
                    raise RecoveryRequired(f'Target {key}: possible untracked duplicate')
                for sid in ids:
                    _verify_live(app.get_shift(client, sid), desired, key,
                        fresh_payload=payload if entry.get('recoveryValidationPending') else None,
                        identity=entry.get('bridgeIdentity'))
                action = 'unchanged'
            elif pending:
                if any(pending['desired'].get(k) != v for k, v in desired.items()):
                    raise RecoveryRequired(f'Target {key}: source differs from pending create')
                recovered = [r for r in destinations if pending['marker'] in app._note_html(r)]
                if len(recovered) != 1 or any(r['id'] != recovered[0]['id'] for r in candidates):
                    raise RecoveryRequired(f'Target {key}: uncertain create outcome; never replay')
                _verify_live(recovered[0], desired, key,
                    fresh_payload=payload if pending['desired'].get('recoveryValidationPending') else None,
                    identity=pending['marker'])
                if any(str(recovered[0]['id']) in set(map(str, e.get('shiftIds', []))) for e in state['shifts'].values()):
                    raise RecoveryRequired(f'Target {key}: pending destination already owned')
                action = 'recover'
            elif candidates:
                raise RecoveryRequired(f'Target {key}: existing destination candidate requires review')
            plans.append({'opportunityId': opportunity_id, 'itemId': item_id,
                          'sourceSubject': subject, 'serviceName': service['name'],
                          'sourceStart': service['starts_at'], 'sourceEnd': service['ends_at'],
                          'scanRange': {'startTime': scan_start, 'endTime': scan_end},
                          'action': action, 'desired': desired, 'payload': payload})
    return plans


def plan_digest(plans):
    # Actions can become unchanged after a partial successful apply; the reviewed
    # source and destination specification must still match exactly on restart.
    specification = [{k: v for k, v in p.items() if k != 'action'} for p in plans]
    return hashlib.sha256(json.dumps(specification, sort_keys=True).encode()).hexdigest()


def run_recovery(*, apply_digest=None, scan_start=None, scan_end=None):
    if getattr(app, 'ITEM_RECOVERY_POLICY_VERSION', None) != 1:
        raise RecoveryRequired('Compatible item-pagination and validation-hold poller required')
    if (type(scan_start) is not int or type(scan_end) is not int or scan_start >= scan_end):
        raise RecoveryRequired('Explicit reviewed destination scan bounds are required')
    with app._sync_guard():
        if not app.STATE_FILE.is_file():
            raise RecoveryRequired('Existing bridge state is required for item recovery')
        state = app._load_state()
        with RecoveryClient(timeout=30) as client:
            plans = plan_recovery(client, state, scan_start, scan_end)
            digest = plan_digest(plans)
            if apply_digest is not None and apply_digest != digest:
                raise RecoveryRequired('Review digest differs; obtain a fresh dry-run before applying')
            results = []
            if apply_digest is not None:
                client.allow_create = True
                for plan in plans:
                    key = str(plan['itemId'])
                    if plan['action'] != 'unchanged':
                        app._durable_create(client, key, copy.deepcopy(plan['payload']),
                                            {**plan['desired'], 'recoveryValidationPending': True}, state)
                    # Exact mapped destination read-back; a failure stops further writes.
                    for sid in state['shifts'][key]['shiftIds']:
                        row = app.get_shift(client, sid)
                        entry = state['shifts'][key]
                        _verify_live(row, plan['desired'], key,
                            fresh_payload=plan['payload'] if entry.get('recoveryValidationPending') else None,
                            identity=entry.get('bridgeIdentity'))
                        results.append({'itemId': plan['itemId'], 'shiftId': sid,
                                        'startTime': row['startTime'], 'endTime': row['endTime'],
                                        'arrivalNote': plan['desired']['arrivalNote']})
                    if state['shifts'][key].pop('recoveryValidationPending', None):
                        app._save_state(state)
            return {'mode': 'apply' if apply_digest is not None else 'dry_run',
                    'reviewDigest': digest, 'plans': plans, 'verifiedDestinations': results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply-digest', help='Apply only the exact reviewed dry-run digest')
    parser.add_argument('--scan-start', required=True, type=int, help='Reviewed scheduler history start (epoch seconds)')
    parser.add_argument('--scan-end', required=True, type=int, help='Reviewed scheduler future horizon (epoch seconds)')
    args = parser.parse_args()
    print(json.dumps(run_recovery(apply_digest=args.apply_digest,
        scan_start=args.scan_start, scan_end=args.scan_end), indent=2))


if __name__ == '__main__':
    main()
