"""Reviewed four-item recovery; never invokes opportunity sync or notifications."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json

import app
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


def _verify_live(row, desired, key):
    if not row or not row.get('id'):
        raise RecoveryRequired(f'Target {key}: tracked destination missing')
    for field in ('startTime', 'endTime', 'title', 'jobId'):
        if row.get(field) != desired[field]:
            raise RecoveryRequired(f'Target {key}: destination {field} differs; no update permitted')
    notes = app._note_html(row)
    if f'(opportunity item #{key})' not in notes or desired['arrivalNote'] not in notes:
        raise RecoveryRequired(f'Target {key}: destination identity/arrival note differs')


def plan_recovery(client, state):
    """GET-only preflight of all four targets before any apply writes."""
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
        services = app.fetch_service_items(client, opportunity_id)
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
            _, _, _, _, desired, fields, notes = app._service_shift_content(
                opportunity, service, start, start + 60, job_id,
                app.get_job_color(job_id, cache), address, contact, title)
            payload = dict(startTime=start, endTime=start+60, title=title,
                           isPublished=False, jobId=job_id, customFields=fields, notes=notes)
            if desired['color']:
                payload['color'] = desired['color']
            if address:
                payload['locationData'] = {'isReferencedToJob': False, 'gps': {'address': address}}
            entries = {k: e for k, e in state['shifts'].items() if k.split(':')[0] == key}
            pending = state.get('pending_creates', {}).get(key)
            if any(k.split(':')[0] == key and k != key for k in state.get('pending_creates', {})):
                raise RecoveryRequired(f'Target {key}: pending split layout requires review')
            if entries and (set(entries) != {key} or entries[key].get('opportunityId') != opportunity_id or pending):
                raise RecoveryRequired(f'Target {key}: conflicting tracking/layout')
            nearby = app.list_shifts(client, start - 86400, start + 86400 + 60)
            # Also hold unmarked lookalikes; never guess an existing shift's identity.
            candidates = [r for r in nearby if f'(opportunity item #{key})' in app._note_html(r)
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
                    _verify_live(app.get_shift(client, sid), desired, key)
                action = 'unchanged'
            elif pending:
                if any(pending['desired'].get(k) != v for k, v in desired.items()):
                    raise RecoveryRequired(f'Target {key}: source differs from pending create')
                recovered = [r for r in nearby if pending['marker'] in app._note_html(r)]
                if len(recovered) != 1 or any(r['id'] != recovered[0]['id'] for r in candidates):
                    raise RecoveryRequired(f'Target {key}: uncertain create outcome; never replay')
                _verify_live(recovered[0], desired, key)
                if any(str(recovered[0]['id']) in set(map(str, e.get('shiftIds', []))) for e in state['shifts'].values()):
                    raise RecoveryRequired(f'Target {key}: pending destination already owned')
                action = 'recover'
            elif candidates:
                raise RecoveryRequired(f'Target {key}: existing destination candidate requires review')
            plans.append({'opportunityId': opportunity_id, 'itemId': item_id,
                          'sourceSubject': subject, 'serviceName': service['name'],
                          'sourceStart': service['starts_at'], 'sourceEnd': service['ends_at'],
                          'action': action, 'desired': desired, 'payload': payload})
    return plans


def plan_digest(plans):
    # Actions can become unchanged after a partial successful apply; the reviewed
    # source and destination specification must still match exactly on restart.
    specification = [{k: v for k, v in p.items() if k != 'action'} for p in plans]
    return hashlib.sha256(json.dumps(specification, sort_keys=True).encode()).hexdigest()


def run_recovery(*, apply_digest=None):
    with app._sync_guard():
        if not app.STATE_FILE.is_file():
            raise RecoveryRequired('Existing bridge state is required for item recovery')
        state = app._load_state()
        with RecoveryClient(timeout=30) as client:
            plans = plan_recovery(client, state)
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
                                            plan['desired'], state)
                    # Exact mapped destination read-back; a failure stops further writes.
                    for sid in state['shifts'][key]['shiftIds']:
                        row = app.get_shift(client, sid)
                        _verify_live(row, plan['desired'], key)
                        results.append({'itemId': plan['itemId'], 'shiftId': sid,
                                        'startTime': row['startTime'], 'endTime': row['endTime'],
                                        'arrivalNote': plan['desired']['arrivalNote']})
            return {'mode': 'apply' if apply_digest is not None else 'dry_run',
                    'reviewDigest': digest, 'plans': plans, 'verifiedDestinations': results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply-digest', help='Apply only the exact reviewed dry-run digest')
    args = parser.parse_args()
    print(json.dumps(run_recovery(apply_digest=args.apply_digest), indent=2))


if __name__ == '__main__':
    main()
