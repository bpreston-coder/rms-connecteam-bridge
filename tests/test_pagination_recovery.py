"""Offline pagination and strictly scoped recovery tests."""
import copy
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

import test_notifications  # installs dummy credentials and disables scheduler
import app
import transport_recovery as recovery


class PaginationTest(unittest.TestCase):
    def client(self, pages, collection):
        calls = []
        def respond(request):
            calls.append(dict(request.url.params))
            index = int(request.url.params['page']) - 1
            return app.httpx.Response(200, json=pages[index])
        return app.httpx.Client(transport=app.httpx.MockTransport(respond)), calls

    def test_twenty_five_rows_does_not_end_opportunity_scan(self):
        pages = [{'opportunities': [{'id': i} for i in range(start, min(start+25, 62))],
                  'meta': {'total_row_count': 62, 'per_page': 100}} for start in (0, 25, 50)]
        client, calls = self.client(pages, 'opportunities')
        with client:
            self.assertEqual(app.fetch_opportunities_updated_since(client, '2026-10-01'), list(range(62)))
        self.assertEqual([c['page'] for c in calls], ['1', '2', '3', '1', '2', '3'])

    def test_services_filtered_only_after_all_pages(self):
        pages = [{'opportunity_items': [{'id': i, 'item_type': 'Product', 'name': 'equipment'} for i in range(25)],
                  'meta': {'total_row_count': 26, 'per_page': 100}},
                 {'opportunity_items': [{'id': 126172, 'item_type': 'Service', 'name': 'Truck 8 Ton - Perth Delivery'}],
                  'meta': {'total_row_count': 26, 'per_page': 100}}]
        client, calls = self.client(pages, 'opportunity_items')
        with client:
            self.assertEqual([r['id'] for r in app.fetch_service_items(client, 4104)], [126172])
        self.assertEqual(len(calls), 2)

    def test_no_total_requires_empty_page(self):
        pages = [{'rows': [{'id': 1}]}, {'rows': [{'id': 2}]}, {'rows': []}]
        client, calls = self.client(pages, 'rows')
        with client:
            self.assertEqual(len(app._fetch_rms_pages(client, '/offline', 'rows')), 2)
        self.assertEqual(len(calls), 3)

    def test_incomplete_overlap_and_changing_total_hold(self):
        for second in ({'rows': [], 'meta': {'total_row_count': 2}},
                       {'rows': [{'id': 1}], 'meta': {'total_row_count': 2}},
                       {'rows': [{'id': 2}], 'meta': {'total_row_count': 3}}):
            with self.subTest(second=second):
                client, _ = self.client([{'rows': [{'id': 1}], 'meta': {'total_row_count': 2}}, second], 'rows')
                with client, self.assertRaises(app.RecoveryRequired):
                    app._fetch_rms_pages(client, '/offline', 'rows')

    def test_duplicate_page_and_bound_hold(self):
        client, _ = self.client([{'rows': [{'id': 1}, {'id': 1}]}], 'rows')
        with client, self.assertRaises(app.RecoveryRequired):
            app._fetch_rms_pages(client, '/offline', 'rows')
        client, calls = self.client([{'rows': [{'id': i}]} for i in range(200)], 'rows')
        with client, self.assertRaises(app.RecoveryRequired):
            app._fetch_rms_pages(client, '/offline', 'rows')
        self.assertEqual(len(calls), 200)

    def test_incomplete_item_listing_cannot_trigger_orphan_cleanup(self):
        opportunity = {'id': 4104, 'state_name': 'Order', 'status_name': 'Active'}
        state = {'shifts': {'126172': {'opportunityId': 4104, 'shiftIds': ['keep']}}}
        pages = [{'opportunity_items': [], 'meta': {'total_row_count': 26}}]
        client, _ = self.client(pages, 'opportunity_items')
        with client, patch.object(app, 'fetch_opportunity', return_value=opportunity), \
                patch.object(app, '_cleanup_shift_keys') as cleanup, \
                patch.object(app, '_save_state') as save:
            with self.assertRaises(app.RecoveryRequired):
                app.sync_opportunity(client, 4104, state)
            cleanup.assert_not_called()
            save.assert_not_called()
        self.assertEqual(state['shifts']['126172']['shiftIds'], ['keep'])


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        def mock(name, **kwargs):
            return self.stack.enter_context(patch.object(app, name, **kwargs))
        self.temp = self.stack.enter_context(tempfile.TemporaryDirectory())
        mock('STATE_FILE', new=Path(self.temp) / 'state.json')
        self.state = {'shifts': {'999': {'opportunityId': 4104, 'shiftIds': ['staff'],
                                        'title': 'Production Management', 'assignedUserIds': [71]}},
                      'retry_opportunities': {'3949': {'error': 'RecoveryRequired'}},
                      'exceptions': {'3949': {'message': 'existing hold'}}, 'poll_cursor': 'unchanged'}
        app._save_state(self.state)
        self.baseline = app.STATE_FILE.read_bytes()
        self.sources = {}
        self.opps = {}
        for oid, ids in recovery.TARGETS.items():
            self.opps[oid] = {'id': oid, 'number': str(oid), 'subject': f'Test event {oid}',
                              'state_name': 'Order', 'status_name': 'Active'}
            self.sources[oid] = [dict(id=i, item_type='Service',
                name='Truck 8 Ton - Perth ' + ('Delivery' if j == 0 else 'Collection'),
                starts_at=f'2026-10-15T{14+j}:45:00+08:00',
                ends_at=f'2026-10-15T{14+j}:45:00+08:00', quantity='1', description='Arrival')
                for j, i in enumerate(ids)]
            self.sources[oid].append(dict(id=999, name='Production Management', item_type='Service'))
        mock('fetch_opportunity', side_effect=lambda c, oid: self.opps[oid])
        mock('fetch_service_items', side_effect=lambda c, oid: copy.deepcopy(self.sources[oid]))
        mock('fetch_venue_address', return_value=None)
        mock('fetch_member_name', return_value='Contact')
        mock('_load_job_title_cache', return_value=({'Truck 8 Ton - Delivery': 'delivery',
                                                   'Truck 8 Ton - Collection': 'collection'}, {}))
        self.live = {}
        mock('list_shifts', side_effect=lambda *a: list(copy.deepcopy(self.live).values()))
        mock('get_shift', side_effect=lambda c, sid: copy.deepcopy(self.live.get(sid)))
        self.sync = mock('sync_opportunity', side_effect=AssertionError('global sync forbidden'))
        self.notify = mock('notify_ops', side_effect=AssertionError('notifications forbidden'))
        self.delete = mock('delete_shift', side_effect=AssertionError('deletes forbidden'))
        self.update = mock('update_shifts', side_effect=AssertionError('updates forbidden'))
        self.posts = []
        self.timeout = False
        def handle(request):
            self.assertEqual(request.method, 'POST')
            self.assertEqual(request.url.params['notifyUsers'], 'false')
            rows = json.loads(request.content)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertFalse(row['isPublished'])
            self.assertNotIn('assignedUserIds', row)
            on_disk = json.loads(app.STATE_FILE.read_text())
            self.assertTrue(any(intent['marker'] in app._note_html(row)
                                for intent in on_disk['pending_creates'].values()))
            sid = 'new' + str(len(self.posts))
            self.posts.append(copy.deepcopy(row))
            self.live[sid] = {**row, 'id': sid}
            if self.timeout:
                self.timeout = False
                raise app.httpx.ReadTimeout('injected ambiguous create')
            return app.httpx.Response(200, json={'data': {'shifts': [self.live[sid]]}})
        real_client = recovery.RecoveryClient
        self.stack.enter_context(patch.object(recovery, 'RecoveryClient',
            side_effect=lambda **kw: real_client(transport=app.httpx.MockTransport(handle), **kw)))

    def dry(self):
        return recovery.run_recovery()

    def test_dry_run_four_only_no_state_or_remote_mutations(self):
        result = self.dry()
        self.assertEqual({p['itemId'] for p in result['plans']}, {126172,126173,125805,125847})
        self.assertEqual(app.STATE_FILE.read_bytes(), self.baseline)
        self.assertEqual(self.posts, [])
        for plan in result['plans']:
            self.assertEqual(plan['desired']['endTime'] - plan['desired']['startTime'], 60)
            self.assertIn('sharp (AWST)', plan['desired']['arrivalNote'])
        self.sync.assert_not_called()

    def test_apply_repeat_and_unrelated_state_preserved(self):
        digest = self.dry()['reviewDigest']
        result = recovery.run_recovery(apply_digest=digest)
        self.assertEqual(len(result['verifiedDestinations']), 4)
        self.assertEqual(len(self.posts), 4)
        state = app._load_state()
        for key in ('retry_opportunities','exceptions','poll_cursor'):
            self.assertEqual(state[key], self.state[key])
        self.assertEqual(state['shifts']['999'], self.state['shifts']['999'])
        # A human assignment/publication is retained on a repeated apply.
        for row in self.live.values():
            row.update(isPublished=True, assignedUserIds=[71])
        recovery.run_recovery(apply_digest=digest)
        self.assertEqual(len(self.posts), 4)
        self.assertTrue(all(r['assignedUserIds'] == [71] for r in self.live.values()))
        self.sync.assert_not_called(); self.update.assert_not_called()
        self.notify.assert_not_called(); self.delete.assert_not_called()

    def test_changed_source_digest_and_invalid_last_item_block_all_writes(self):
        digest = self.dry()['reviewDigest']
        self.sources[4095][1]['description'] = 'Changed after review'
        with self.assertRaises(app.RecoveryRequired):
            recovery.run_recovery(apply_digest=digest)
        self.sources[4095][1]['name'] = 'Production Management'
        with self.assertRaises(app.RecoveryRequired):
            self.dry()
        self.assertFalse(self.posts)
        self.assertEqual(app.STATE_FILE.read_bytes(), self.baseline)

    def test_missing_or_inactive_source_holds_without_cleanup(self):
        for status in ('Completed','Cancelled','Postponed','Dead','Lost'):
            self.opps[4095]['status_name'] = status
            with self.assertRaises(app.RecoveryRequired): self.dry()
        self.opps[4095]['status_name'] = 'Active'
        self.sources[4095].pop(1)
        with self.assertRaises(app.RecoveryRequired): self.dry()
        self.assertFalse(self.posts)
        self.assertEqual(app.STATE_FILE.read_bytes(), self.baseline)

    def test_untracked_marker_or_unmarked_lookalike_holds(self):
        payload = self.dry()['plans'][0]['payload']
        for notes in (payload['notes'], []):
            self.live['manual'] = {**payload, 'id': 'manual', 'notes': notes}
            with self.assertRaises(app.RecoveryRequired): self.dry()
        self.assertFalse(self.posts)

    def test_ambiguous_create_reconciles_without_duplicate_post(self):
        digest = self.dry()['reviewDigest']
        self.timeout = True
        with self.assertRaises(app.httpx.ReadTimeout):
            recovery.run_recovery(apply_digest=digest)
        self.assertEqual(len(self.posts), 1)
        self.assertIn('126172', app._load_state()['pending_creates'])
        before = app.STATE_FILE.read_bytes()
        self.assertEqual(self.dry()['plans'][0]['action'], 'recover')
        self.assertEqual(app.STATE_FILE.read_bytes(), before)
        recovery.run_recovery(apply_digest=digest)
        self.assertEqual(len(self.posts), 4)
        self.assertFalse(app._load_state()['pending_creates'])

    def test_unresolved_timeout_never_replays(self):
        digest = self.dry()['reviewDigest']
        self.timeout = True
        with self.assertRaises(app.httpx.ReadTimeout): recovery.run_recovery(apply_digest=digest)
        self.live.clear()
        with self.assertRaises(app.RecoveryRequired): recovery.run_recovery(apply_digest=digest)
        self.assertEqual(len(self.posts), 1)

    def test_missing_existing_destination_stays_held(self):
        digest = self.dry()['reviewDigest']
        recovery.run_recovery(apply_digest=digest)
        del self.live['new0']
        with self.assertRaises(app.RecoveryRequired): recovery.run_recovery(apply_digest=digest)
        self.assertEqual(len(self.posts), 4)

    def test_missing_state_does_not_start_fresh(self):
        app.STATE_FILE.unlink()
        with self.assertRaises(app.RecoveryRequired): self.dry()
        self.assertFalse(self.posts)

    def test_split_layout_and_pending_source_drift_hold(self):
        state = app._load_state()
        state['shifts']['126172:0'] = {'opportunityId': 4104, 'shiftIds': ['old']}
        app._save_state(state)
        with self.assertRaises(app.RecoveryRequired): self.dry()
        del state['shifts']['126172:0']
        app._save_state(state)
        digest = self.dry()['reviewDigest']
        self.timeout = True
        with self.assertRaises(app.httpx.ReadTimeout): recovery.run_recovery(apply_digest=digest)
        self.sources[4104][0]['starts_at'] = self.sources[4104][0]['ends_at'] = '2026-10-16T14:45:00+08:00'
        with self.assertRaises(app.RecoveryRequired): self.dry()
        self.assertEqual(len(self.posts), 1)

    def test_network_guard_blocks_sync_notification_and_unapproved_create(self):
        # Access the real class through the patched constructor.
        with recovery.RecoveryClient() as client:
            for method, url, body in (
                ('POST', 'https://chat.invalid', {}),
                ('PUT', 'https://api.connecteam.com/shifts', []),
                ('DELETE', 'https://api.current-rms.com/opportunity', None)):
                with self.assertRaises(app.RecoveryRequired): client.request(method, url, json=body)
            client.allow_create = True
            url=f'{app.CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{app.CONNECTEAM_SCHEDULER_ID}/shifts'
            with self.assertRaises(app.RecoveryRequired):
                client.post(url, json=[{'isPublished':False,'notes':[{'html':'(opportunity item #999)'}]}], params={'notifyUsers':'false'})
