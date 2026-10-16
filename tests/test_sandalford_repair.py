"""Offline repair lifecycle and preservation tests; no live destinations."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from contextlib import ExitStack

import test_notifications
import app
import sandalford_repair as repair


class HoldTest(unittest.TestCase):
    def test_hold_precedes_sync_checkpoint_and_all_cleanup(self):
        state = {'shifts': {'125886:0': {'opportunityId': 4071, 'shiftIds': ['kept'], 'quantity': 2}},
                 'opportunity_holds': {'4071': {'reason': 'repair'}},
                 'pending_creates': {'126320': {'opportunityId': 4071}},
                 'exceptions': {'4071': {'message': 'preserve'}}, 'job_title_cache': {}}
        before = copy.deepcopy(state)
        client = Mock()
        for call in (lambda: app.sync_opportunity(client, 4071, state),
                     lambda: app._sync_and_checkpoint(client, 4071, state),
                     lambda: app.cleanup_ineligible_opportunity(client, 4071, 'source404', state),
                     lambda: app._cleanup_shift_keys(client, ['125886:0'], state['shifts'], 4071,
                                                     'source404', {}, state)):
            self.assertEqual(call()['status'], 'held')
            self.assertEqual(state, before)
        self.assertFalse(client.mock_calls)
        with self.assertRaises(app.RecoveryRequired):
            app._cleanup_shift_keys(client, ['125886:0'], state['shifts'], 4071, 'source404', {})

    def test_malformed_holds_fail_closed(self):
        for holds in (None, [], {'04071': {'reason': 'x'}}, {'4071': {}}, {'4071': False}):
            with self.subTest(holds=holds), self.assertRaises(app.RecoveryRequired):
                app.sync_opportunity(Mock(), 4071, {'shifts': {}, 'opportunity_holds': holds})


class RepairTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.temp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(patch.object(app, 'STATE_FILE', Path(self.temp) / 'state.json'))
        self.deletes = repair.DELETE_IDS
        self.state = {'shifts': {}, 'exceptions': {'4071': {'message': 'prior'}},
                      'retry_opportunities': {'4071': {'error': 'prior'}}, 'pending_creates': {}}
        self.rows = {}
        self.services = []
        self.writes = []
        self.opportunity = {'id': 4071, 'number': repair.ORDER, 'state_name': 'Order', 'status_name': 'Active'}
        def entry(sid, job, start, end, quantity=1):
            return {'opportunityId': 4071, 'shiftIds': [sid], 'jobId': job, 'startTime': start,
                    'endTime': end, 'quantity': quantity, 'description': '', 'title': 'Sandalford',
                    'notificationCancelled': False, 'orderNumber': repair.ORDER}
        def row(sid, key, e, published=False, users=None):
            return {'id': sid, 'jobId': e['jobId'], 'startTime': e['startTime'], 'endTime': e['endTime'],
                    'title': 'Sandalford', 'isPublished': published, 'assignedUserIds': users or [],
                    'notes': [{'html': f'<p>Auto-created from Current RMS order {repair.ORDER} (opportunity item #{key}).</p>'},
                              {'html': '<p>Manual crew instruction: keep exactly</p>'}],
                    'customFields': [{'customFieldId': app.CONNECTEAM_JOBNO_CUSTOM_FIELD_ID, 'value': repair.ORDER},
                                     {'customFieldId': app.CONNECTEAM_QTY_CUSTOM_FIELD_ID, 'value': str(e['quantity'])}]}
        for n, (key, (sid, timestamp)) in enumerate(repair.RETAIN.items()):
            self.services.append({'id': int(key), 'item_type': 'Service', 'name': 'Truck 8 Ton - Perth ' + ('Delivery' if n == 0 else 'Collection'),
                                  'starts_at': timestamp, 'ends_at': timestamp, 'quantity': '1'})
            start = app._to_epoch_seconds('2026-10-23T08:00:00+08:00')
            for i, old_id in enumerate([sid, *self.deletes[n*5:n*5+5]]):
                e = entry(old_id, 'transport-job', start+i*86400, start+i*86400+3600)
                self.state['shifts'][f'{key}:{i}'] = e
                self.rows[old_id] = row(old_id, key, e, n == 0 and i == 0, [77] if n == 0 and i == 0 else [])
        start, end = '2026-10-26T08:00:00+08:00', '2026-10-26T18:00:00+08:00'
        self.services.append({'id': 126320, 'item_type': 'Service', 'name': 'Operator', 'starts_at': start, 'ends_at': end, 'quantity': '2'})
        e = entry(repair.MISSING, repair.OPERATOR_JOB, app._to_epoch_seconds(start), app._to_epoch_seconds(end), 2)
        e['shiftIds'].append(repair.OTHER_OPERATOR)
        self.state['shifts']['126320'] = e
        for sid, user in ((repair.REPLACEMENT, 9971217), (repair.OTHER_OPERATOR, 18210921)):
            self.rows[sid] = row(sid, '126320', e, True, [user])
        self.rows[repair.REPLACEMENT]['notes'].append({'html': f'<p>{repair.IDENTITY}</p>'})
        self.state['shifts']['unrelated'] = {'opportunityId': 999, 'shiftIds': ['untouched']}
        self.stack.enter_context(patch.object(app, 'fetch_opportunity', side_effect=lambda *_: copy.deepcopy(self.opportunity)))
        self.stack.enter_context(patch.object(repair.recovery_support, 'fetch_service_items', side_effect=lambda *_: copy.deepcopy(self.services)))
        self.stack.enter_context(patch.object(app, 'get_shift', side_effect=lambda _, sid: copy.deepcopy(self.rows.get(sid))))
        self.stack.enter_context(patch.object(app, 'update_shifts', side_effect=self.update))
        self.stack.enter_context(patch.object(app, 'delete_shift', side_effect=self.delete))
        app._save_state(self.state)
        self.client = Mock()

    def update(self, _, payloads):
        sid = payloads[0]['shiftId']
        self.assert_journal(sid)
        self.writes.append(('update', sid))
        self.rows[sid].update(copy.deepcopy({k: v for k, v in payloads[0].items() if k != 'shiftId'}))
        return [copy.deepcopy(self.rows[sid])]

    def delete(self, _, sid):
        self.assert_journal(sid)
        self.writes.append(('delete', sid))
        del self.rows[sid]

    def assert_journal(self, sid):
        disk = app._load_state()
        self.assertEqual(disk['repair_journals'][repair.JOURNAL_KEY]['intent'], sid)
        self.assertIn('4071', disk['opportunity_holds'])
        self.assertIn('125886:0', disk['shifts'])

    def plan(self):
        return repair.build_plan(self.client, self.state)

    def apply(self, plan):
        return repair.apply_plan(self.client, self.state, plan, repair.digest(plan))

    def test_no_deletion_allowlist_fails_closed(self):
        with patch.object(repair, 'DELETE_IDS', ()), self.assertRaises(app.RecoveryRequired):
            self.plan()
        self.assertEqual(self.writes, [])

    def test_approved_ids_are_bound_to_exact_source_items(self):
        self.assertEqual(repair.DELETE_BY_ITEM, {
            '125886': ('6acf25b627228e4c71521dcb', '6acf25b678aae02b9ecde568',
                       '6acf25b778aae02b9ecde56a', '6acf25b82ecda2de8bd6b0ac',
                       '6acf25b90991f240c218d535'),
            '125887': ('6acf25ba828980cd742a5f3f', '6acf25ba78aae02b9ecde56c',
                       '6acf25bb0991f240c218d537', '6acf25bb78aae02b9ecde56e',
                       '6acf25bc0991f240c218d539'),
        })
        self.assertEqual(len(set(self.deletes)), 10)
        protected = {repair.MISSING, repair.REPLACEMENT, repair.OTHER_OPERATOR,
                     *(pair[0] for pair in repair.RETAIN.values())}
        self.assertFalse(protected.intersection(self.deletes))
        plan = self.plan()
        for op in plan['operations']:
            if op['kind'] == 'delete':
                self.assertIn(op['id'], repair.DELETE_BY_ITEM[op['sourceKey'].split(':')[0]])

    def test_cross_item_rebinding_of_approved_id_is_rejected(self):
        delivery, collection = self.deletes[0], self.deletes[5]
        self.state['shifts']['125886:1']['shiftIds'] = [collection]
        self.state['shifts']['125887:1']['shiftIds'] = [delivery]
        # Even coherent rewritten identity and ownership cannot move approval.
        for sid, key in ((delivery, '125887'), (collection, '125886')):
            self.rows[sid]['notes'][0]['html'] = f'<p>(opportunity item #{key})</p>'
        with self.assertRaises(app.RecoveryRequired): self.plan()
        self.assertEqual(self.writes, [])

    def test_fresh_checks_stop_deletion_on_drift_after_initial_preflight(self):
        plan = self.plan()
        for drift in ({'isPublished': True}, {'assignedUserIds': [91]},
                      {'notes': [{'html': 'different source identity'}]}):
            with self.subTest(drift=drift):
                before_state, before_rows = copy.deepcopy(self.state), copy.deepcopy(self.rows)
                original = self.update
                def update_and_drift(client, payloads):
                    result = original(client, payloads)
                    self.rows[self.deletes[0]].update(copy.deepcopy(drift))
                    return result
                with patch.object(app, 'update_shifts', side_effect=update_and_drift):
                    with self.assertRaises(app.RecoveryRequired): self.apply(plan)
                self.assertFalse(any(kind == 'delete' for kind, _ in self.writes))
                self.assertIn('4071', self.state['opportunity_holds'])
                self.assertIn('125886:0', self.state['shifts'])
                self.state, self.rows, self.writes = before_state, before_rows, []
                app._save_state(self.state)

    def test_apply_preserves_staffing_manual_notes_and_repeats_without_writes(self):
        before_rows, before_state = copy.deepcopy(self.rows), copy.deepcopy(self.state)
        plan = self.plan()
        self.assertEqual(self.writes, [])
        result = self.apply(plan)
        self.assertEqual(result['status'], 'verified_held')
        self.assertEqual(len(self.writes), 12)
        self.assertEqual(self.state['shifts']['126320']['shiftIds'], [repair.REPLACEMENT, repair.OTHER_OPERATOR])
        for key, (sid, timestamp) in repair.RETAIN.items():
            actual, prior = self.rows[sid], before_rows[sid]
            self.assertEqual(actual['assignedUserIds'], prior['assignedUserIds'])
            self.assertEqual(actual['isPublished'], prior['isPublished'])
            self.assertEqual(actual['notes'][:-1], prior['notes'])
            self.assertEqual(actual['endTime']-actual['startTime'], 60)
            self.assertEqual(self.state['shifts'][key]['shiftIds'], [sid])
        self.assertEqual(self.rows[repair.OTHER_OPERATOR], before_rows[repair.OTHER_OPERATOR])
        self.assertEqual(self.rows[repair.REPLACEMENT], before_rows[repair.REPLACEMENT])
        for key in ('exceptions', 'retry_opportunities'):
            self.assertEqual(self.state[key], before_state[key])
        self.assertEqual(self.state['shifts']['unrelated'], before_state['shifts']['unrelated'])
        self.apply(plan)
        self.assertEqual(len(self.writes), 12)
        self.assertEqual(app.sync_opportunity(self.client, 4071, self.state)['status'], 'held')

    def test_drift_prevents_all_writes(self):
        for mutate in (lambda: self.rows[self.deletes[-1]].update(isPublished=True),
                       lambda: self.rows[repair.REPLACEMENT].update(assignedUserIds=[1]),
                       lambda: self.state['shifts'].update(conflict={'opportunityId': 999, 'shiftIds': [repair.REPLACEMENT]}),
                       lambda: self.services[0].update(ends_at='2026-10-26T09:00:00+08:00')):
            with self.subTest(mutate=mutate):
                rows, state, services = copy.deepcopy(self.rows), copy.deepcopy(self.state), copy.deepcopy(self.services)
                plan = self.plan()
                mutate()
                with self.assertRaises(app.RecoveryRequired): self.apply(plan)
                self.assertEqual(self.writes, [])
                self.rows, self.state, self.services = rows, state, services

    def test_timeout_after_delete_reconciles_without_second_delete(self):
        plan = self.plan()
        original = self.delete
        def uncertain(client, sid):
            original(client, sid)
            raise app.httpx.ReadTimeout('offline timeout')
        with patch.object(app, 'delete_shift', side_effect=uncertain):
            with self.assertRaises(app.httpx.ReadTimeout): self.apply(plan)
        self.assertIn('125886:0', self.state['shifts'])
        self.state = app._load_state()
        self.apply(plan)
        self.assertEqual(len(self.writes), 12)

    def test_timeout_before_write_rechecks_and_can_retry_exact_same_row(self):
        plan = self.plan()
        with patch.object(app, 'update_shifts', side_effect=app.httpx.ReadTimeout('offline timeout')):
            with self.assertRaises(app.httpx.ReadTimeout): self.apply(plan)
        self.state = app._load_state()
        sid = self.state['repair_journals'][repair.JOURNAL_KEY]['intent']
        self.rows[sid]['notes'].append({'html': 'Changed manually'})
        with self.assertRaises(app.RecoveryRequired): self.apply(plan)
        self.assertEqual(self.writes, [])
        self.rows[sid]['notes'].pop()
        self.apply(plan)
        self.assertEqual(len(self.writes), 12)

    def test_wrong_digest_cannot_install_hold_or_write(self):
        plan = self.plan()
        with self.assertRaises(app.RecoveryRequired):
            repair.apply_plan(self.client, self.state, plan, 'wrong')
        self.assertNotIn('opportunity_holds', self.state)
        self.assertEqual(self.writes, [])

    def test_inactive_source_targets_cannot_be_repaired(self):
        for service in self.services:
            for status in ('Cancelled', 'Completed', 'Dead', 'Lost', 'Postponed'):
                service['status_name'] = status
                with self.subTest(item=service['id'], status=status), self.assertRaises(app.RecoveryRequired):
                    self.plan()
            service.pop('status_name')
        self.assertEqual(self.writes, [])

    def test_operator_source_quantity_must_still_be_two(self):
        self.services[-1]['quantity'] = '3'
        with self.assertRaises(app.RecoveryRequired): self.plan()
        self.assertEqual(self.writes, [])

    def test_staged_code_cannot_run_against_old_poller(self):
        baseline = app.STATE_FILE.read_bytes()
        client = app.httpx.Client(transport=app.httpx.MockTransport(
            lambda _: app.httpx.Response(200, json={'item_recovery_policy_version': 1})))
        with patch.object(repair, 'RepairClient', return_value=client):
            with self.assertRaises(app.RecoveryRequired): repair.run()
        self.assertEqual(app.STATE_FILE.read_bytes(), baseline)
        self.assertEqual(self.writes, [])

    def test_hold_aware_degraded_readiness_allows_read_only_plan(self):
        baseline = app.STATE_FILE.read_bytes()
        client = app.httpx.Client(transport=app.httpx.MockTransport(
            lambda _: app.httpx.Response(503, json={'opportunity_hold_policy_version': 1})))
        with patch.object(repair, 'RepairClient', return_value=client):
            report = repair.run()
        self.assertEqual(report['reviewDigest'], repair.digest(report['plan']))
        self.assertEqual(app.STATE_FILE.read_bytes(), baseline)
        self.assertEqual(self.writes, [])


class NetworkGuardTest(unittest.TestCase):
    def test_only_exact_operation_can_mutate(self):
        calls = []
        transport = app.httpx.MockTransport(lambda request: (calls.append(request) or app.httpx.Response(200, json={})))
        endpoint = f'{app.CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{app.CONNECTEAM_SCHEDULER_ID}/shifts'
        with repair.RepairClient(transport=transport) as client:
            for method, url in (('POST', endpoint), ('PUT', endpoint), ('DELETE', endpoint+'/wrong')):
                with self.assertRaises(app.RecoveryRequired): client.request(method, url)
            sid = repair.RETAIN['125886'][0]
            payload = {'shiftId': sid, 'startTime': 1, 'endTime': 61, 'notes': []}
            client.approved_operation = {'kind': 'update', 'id': sid, 'payload': payload}
            with self.assertRaises(app.RecoveryRequired): client.put(endpoint, json=[payload], params={'notifyUsers': 'true'})
            app.update_shifts(client, [payload])
        self.assertEqual(len(calls), 1)
        import json
        self.assertEqual(json.loads(calls[0].content), [payload])


if __name__ == '__main__':
    unittest.main()
