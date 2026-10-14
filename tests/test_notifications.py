"""Offline sync regression tests: every upstream and Chat call is mocked."""
import copy
import os
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

for name in ('CURRENT_RMS_SUBDOMAIN', 'CURRENT_RMS_API_KEY', 'CONNECTEAM_API_KEY', 'CONNECTEAM_SCHEDULER_ID'):
    os.environ[name] = 'offline-test'
os.environ['ENABLE_SCHEDULER'] = 'false'
os.environ['GOOGLE_CHAT_WEBHOOK_URL'] = ''
import app

NOW = app._to_epoch_seconds('2026-10-14T12:00:00+08:00')


class NotificationsTest(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        def mock(name, **kwargs):
            return self.stack.enter_context(patch.object(app, name, **kwargs))
        self.opportunity = {'id': 1, 'number': '1001', 'subject': 'Concert', 'state_name': 'Order', 'status_name': 'Active'}
        self.service = {'id': 10, 'name': 'Crew', 'starts_at': '2026-10-15T12:00:00+08:00', 'ends_at': '2026-10-15T16:00:00+08:00', 'quantity': '1.0', 'description': 'Setup'}
        self.entry = dict(opportunityId=1, orderNumber='1001', startTime=NOW+86400, endTime=NOW+100800,
                          title='Concert', jobId='job', color='blue', address=None, quantity=1, description='Setup', siteContact='', shiftIds=['s1'])
        self.state = {'shifts': {'10': copy.deepcopy(self.entry)}, 'job_title_cache': {'by_title': {'Crew': 'job'}}}
        mock('fetch_opportunity', side_effect=lambda *_: self.opportunity)
        mock('fetch_service_items', side_effect=lambda *_: [self.service])
        mock('fetch_venue_address', return_value=None)
        mock('fetch_member_name', return_value=None)
        mock('_load_job_title_cache', return_value=({'Crew': 'job'}, {'job': 'blue'}))
        self.live = {'id': 's1', 'isPublished': True, 'startTime': self.entry['startTime'], 'endTime': self.entry['endTime']}
        self.get = mock('get_shift', side_effect=lambda *_: self.live.copy())
        mock('_find_sibling_shift_ids', return_value=[])
        self.create = mock('create_shifts', side_effect=lambda client, payloads: [{'id': f'new{i}', 'isPublished': False} for i, p in enumerate(payloads)])
        self.update = mock('update_shifts', side_effect=lambda client, payloads: [{'id': p['shiftId'], 'isPublished': self.live['isPublished']} for p in reversed(payloads)])
        self.delete = mock('delete_shift')
        self.notify = mock('notify_ops')
        self.stack.enter_context(patch.object(app.time, 'time', return_value=NOW))
        self.client = Mock(spec=app.httpx.Client)

    def sync(self):
        result = app.sync_opportunity(self.client, 1, self.state)
        self.client.request.assert_not_called()
        self.client.post.assert_not_called()
        self.client.get.assert_not_called()
        return result

    def test_future_timing_change_alerts_and_syncs(self):
        self.service['starts_at'] = '2026-10-15T13:00:00+08:00'
        self.assertEqual(self.sync()['updated_count'], 1)
        self.assertIn('Time — Original:', self.notify.call_args.args[1])
        self.assertIn('AWST', self.notify.call_args.args[1])

    def test_future_dead_lost_or_not_required_still_alerts(self):
        for changes in ({'status_name': 'Dead'}, {'status_name': 'Lost'}, {'state_name': 'Quotation', 'custom_fields': {'draft_shifts_in_connecteams': 'No'}}):
            with self.subTest(changes=changes):
                self.state['shifts'] = {'10': copy.deepcopy(self.entry)}
                self.opportunity = {'id': 1, 'state_name': 'Order', **changes}
                self.notify.reset_mock()
                result = self.sync()
                self.assertEqual(result['deleted_count'], 1)
                self.notify.assert_called_once()
                self.assertEqual(self.state['shifts'], {})

    def test_removed_future_service_alerts(self):
        with patch.object(app, 'fetch_service_items', return_value=[]):
            self.sync()
        self.notify.assert_called_once()
        self.delete.assert_called_once()

    def test_finalisation_only_is_silent_and_preserves_shift(self):
        self.service['status_name'] = 'Completed'
        self.sync()
        self.notify.assert_not_called()
        self.delete.assert_not_called()
        self.assertEqual(self.update.call_args.args[1], [])
        self.assertTrue(self.state['shifts']['10']['notificationFinalised'])
        self.opportunity['status_name'] = 'Dead'
        result = self.sync()
        self.assertEqual(result['deleted_published_count'], 1)
        self.notify.assert_not_called()

    def test_past_edit_syncs_without_alert(self):
        self.state['shifts']['10'].update(startTime=NOW-7200, endTime=NOW-3600)
        self.service.update(starts_at='2026-10-14T10:00:00+08:00', ends_at='2026-10-14T11:00:00+08:00', description='Corrected notes')
        self.assertEqual(self.sync()['updated_count'], 1)
        self.notify.assert_not_called()

    def test_past_cancellation_syncs_without_alert_and_retains_counts(self):
        self.state['shifts']['10'].update(startTime=NOW-7200, endTime=NOW-3600)
        self.live.update(startTime=NOW-7200, endTime=NOW-3600)
        self.opportunity['status_name'] = 'Dead'
        self.assertEqual(self.sync()['deleted_published_count'], 1)
        self.delete.assert_called_once()
        self.notify.assert_not_called()

    def test_unchanged_silent(self):
        self.assertEqual(self.sync()['unchanged_count'], 1)
        self.notify.assert_not_called()

    def test_internal_color_refresh_silent(self):
        self.state['shifts']['10']['color'] = 'old'
        self.assertEqual(self.sync()['updated_count'], 1)
        self.notify.assert_not_called()

    def test_future_moved_into_past_still_alerts(self):
        self.service.update(starts_at='2026-10-13T12:00:00+08:00', ends_at='2026-10-13T16:00:00+08:00')
        self.sync()
        self.notify.assert_called_once()

    def test_past_moved_into_future_still_alerts(self):
        self.state['shifts']['10'].update(startTime=NOW-7200, endTime=NOW-3600)
        self.sync()
        self.notify.assert_called_once()

    def test_draft_edit_stays_silent(self):
        self.live['isPublished'] = False
        self.service['description'] = 'Changed'
        self.sync()
        self.notify.assert_not_called()

    def test_mixed_past_future_cleanup_only_lists_unfinished_shift(self):
        self.state['shifts']['9'] = {**self.entry, 'shiftIds': ['past'], 'title': 'Past title', 'endTime': NOW-1}
        self.get.side_effect = lambda client, shift_id: {**self.live, 'endTime': NOW-1 if shift_id == 'past' else NOW+86400}
        self.opportunity['status_name'] = 'Dead'
        self.assertEqual(self.sync()['deleted_count'], 2)
        self.assertNotIn('Past title', self.notify.call_args.args[1])
        self.assertIn('Concert', self.notify.call_args.args[1])

    def test_in_progress_and_unknown_legacy_dates_retain_alerts(self):
        self.assertTrue(app._notification_work_remaining({'startTime': NOW-60, 'endTime': NOW+60}, now=NOW))
        self.assertTrue(app._notification_work_remaining({}, now=NOW))
        self.assertFalse(app._notification_work_remaining({'endTime': NOW}, now=NOW))

    def test_internal_sibling_discovery_silent(self):
        self.service['quantity'] = '2.0'
        self.state['shifts']['10']['quantity'] = 2
        with patch.object(app, '_find_sibling_shift_ids', return_value=['s1', 's2']):
            self.assertEqual(self.sync()['updated_count'], 2)
        self.notify.assert_not_called()

    def test_failed_update_keeps_failure_and_sends_no_change_alert(self):
        self.service['description'] = 'New notes'
        request = app.httpx.Request('PUT', 'https://offline.invalid/shifts')
        response = app.httpx.Response(500, request=request)
        self.update.side_effect = app.httpx.HTTPStatusError('offline failure', request=request, response=response)
        with self.assertRaises(app.httpx.HTTPStatusError):
            self.sync()
        self.notify.assert_not_called()
        self.assertEqual(self.state['shifts']['10']['description'], 'Setup')

    def test_live_future_date_retains_cancellation_alert(self):
        self.state['shifts']['10']['endTime'] = NOW-1
        self.opportunity['status_name'] = 'Dead'
        self.sync()
        self.notify.assert_called_once()

    def test_cancelled_status_only_alerts_once_without_changing_sync(self):
        for target in (self.opportunity, self.service):
            with self.subTest(target='opportunity' if target is self.opportunity else 'service'):
                self.state['shifts']['10'].pop('notificationCancelled', None)
                target['status_name'] = 'Cancelled'
                self.notify.reset_mock()
                self.assertEqual(self.sync()['unchanged_count'], 1)
                self.notify.assert_called_once()
                self.assertIn('OnRent work cancelled', self.notify.call_args.args[1])
                self.assertNotIn('been deleted', self.notify.call_args.args[1])
                self.delete.assert_not_called()
                self.sync()
                self.notify.assert_called_once()
                target['status_name'] = 'Active'

    def test_completed_opportunity_silences_operational_diff(self):
        self.opportunity['status_name'] = 'Completed'
        self.service['description'] = 'Finalisation changes'
        self.assertEqual(self.sync()['updated_count'], 1)
        self.notify.assert_not_called()

    def test_past_status_cancellation_silent(self):
        self.state['shifts']['10'].update(startTime=NOW-7200, endTime=NOW-3600)
        self.service.update(starts_at='2026-10-14T10:00:00+08:00', ends_at='2026-10-14T11:00:00+08:00', status_name='Cancelled')
        self.sync()
        self.notify.assert_not_called()

    def test_timezone_equivalence(self):
        self.service.update(starts_at='2026-10-15T04:00:00Z', ends_at='2026-10-15T08:00:00Z')
        self.assertEqual(self.sync()['unchanged_count'], 1)
        self.notify.assert_not_called()


if __name__ == '__main__':
    unittest.main()
