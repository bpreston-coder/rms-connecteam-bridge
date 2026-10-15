"""Offline transport arrival tests; no upstream writes or notifications."""
import copy
import unittest
from unittest.mock import patch

import test_notifications as notifications
import app


class TransportSegmentsTest(unittest.TestCase):
    def service(self, **changes):
        return dict(id=10, name='Truck 8 Ton - Perth Delivery',
                    starts_at='2026-10-15T14:45:00+08:00',
                    ends_at='2026-10-15T14:45:00+08:00', **changes)

    def test_every_explicit_transport_name_extends_only_end(self):
        for name in app.TRANSPORT_ARRIVAL_SERVICE_NAMES:
            for rate in ('Hour', 'Day'):
                with self.subTest(name=name, rate=rate):
                    service = self.service(service_rate_type_name=rate)
                    service['name'] = name
                    original = copy.deepcopy(service)
                    rows, skipped, protected = app._expand_service_segments([service])
                    start = app._to_epoch_seconds(service['starts_at'])
                    self.assertEqual(rows, [('10', service, start, start + 60)])
                    self.assertFalse(skipped or protected)
                    self.assertEqual(service, original)
                    self.assertIn(name, app.SERVICE_JOB_OVERRIDES)

    def test_normal_durations_keep_existing_segmentation(self):
        for end in ('2026-10-15T15:45:00+08:00', '2026-10-17T16:00:00+08:00'):
            for rate in ('Hour', 'Day'):
                service = self.service(service_rate_type_name=rate)
                service['ends_at'] = end
                rows, skipped, protected = app._expand_service_segments([service])
                start = app._to_epoch_seconds(service['starts_at'])
                finish = app._to_epoch_seconds(end)
                self.assertEqual([(r[2], r[3]) for r in rows],
                                 app._split_into_daily_segments(start, finish, rate == 'Day'))
                self.assertIsNone(app._transport_arrival_time(service))
                self.assertFalse(skipped or protected)

    def test_non_transport_and_unrecognized_names_stay_protected(self):
        for name in ('Crew', 'Delivery', 'Truck 8 Ton - Perth Delivery assistant',
                     'Unreviewed Transport Collection', 'Truck 8 Ton - Delivery'):
            service = self.service(description='Truck 8 Ton - Perth Delivery')
            service['name'] = name
            rows, skipped, protected = app._expand_service_segments([service])
            self.assertEqual(rows, [])
            self.assertEqual(protected, {'10'})
            self.assertIn('equal times', skipped[0][1])

    def test_reversed_transport_stays_protected(self):
        service = self.service()
        service['ends_at'] = '2026-10-14T14:45:00+08:00'
        rows, skipped, protected = app._expand_service_segments([service])
        self.assertEqual(rows, [])
        self.assertEqual(protected, {'10'})
        self.assertIn('end before start', skipped[0][1])

    def test_timezone_equivalence_and_midnight_year_rollover(self):
        service = self.service()
        service.update(starts_at='2026-12-31T23:59:00+08:00',
                       ends_at='2026-12-31T15:59:00Z')
        rows, skipped, protected = app._expand_service_segments([service])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], '10')
        self.assertEqual(rows[0][2], app._to_epoch_seconds(service['starts_at']))
        self.assertEqual(rows[0][3], app._to_epoch_seconds('2027-01-01T00:00:00+08:00'))
        self.assertFalse(skipped or protected)

    def test_subsecond_reversal_is_not_mistaken_for_equality(self):
        service = self.service()
        service.update(starts_at='2026-10-15T14:45:00.500+08:00',
                       ends_at='2026-10-15T14:45:00.100+08:00')
        self.assertIsNone(app._transport_arrival_time(service))
        rows, _, protected = app._expand_service_segments([service])
        self.assertEqual(rows, [])
        self.assertEqual(protected, {'10'})


class TransportSyncTest(unittest.TestCase):
    # Reuse the fully offline fixture without inheriting duplicate test methods.
    setUp = notifications.NotificationsTest.setUp
    sync = notifications.NotificationsTest.sync

    def arrival(self, start='2026-10-15T14:45:00+08:00'):
        self.service.update(name='Truck 8 Ton - Perth Delivery', starts_at=start, ends_at=start)
        self.stack.enter_context(patch.object(app, 'find_job_by_service_name', return_value='job'))

    def test_create_and_repeated_polling_are_idempotent(self):
        self.arrival()
        original = copy.deepcopy(self.service)
        self.state['shifts'].clear()
        self.sync()
        payload = self.create.call_args.args[1][0]
        self.assertEqual(payload['endTime'] - payload['startTime'], 60)
        self.assertEqual(payload['startTime'], app._to_epoch_seconds(original['starts_at']))
        self.assertIn('On site at 2:45pm sharp (AWST).', app._note_html(payload))
        self.assertIn('not work duration', app._note_html(payload))
        self.live.update(payload, id='new0', isPublished=False)
        app._save_state(self.state)
        for _ in range(3):
            # Real polling orchestration, all reads and writes mocked locally.
            with patch.object(app, 'ReadRetryClient') as client, patch.object(
                app, 'fetch_opportunities_updated_since', return_value=[1]
            ), patch.object(app, 'reconcile_report', return_value={'issues': []}):
                client.return_value.__enter__.return_value = self.client
                app.poll_all_open_orders()
        self.create.assert_called_once()
        self.update.assert_not_called()
        self.delete.assert_not_called()
        self.notify.assert_not_called()
        self.assertEqual(app._load_state()['shifts']['10']['shiftIds'], ['new0'])
        self.assertEqual(self.service, original)

    def test_existing_id_assignments_and_manual_notes_preserved(self):
        self.arrival()
        self.live.update(assignedUserIds=[71], notes=[{'html': '<p>Manual instructions</p>'}])
        self.sync()
        payload = self.update.call_args.args[1][0]
        self.assertEqual(payload['shiftId'], 's1')
        self.assertNotIn('assignedUserIds', payload)
        self.assertNotIn('isPublished', payload)
        self.assertIn('Manual instructions', app._note_html(payload))
        self.assertIn('On site at 2:45pm sharp', app._note_html(payload))
        self.assertEqual(self.state['shifts']['10']['shiftIds'], ['s1'])
        self.create.assert_not_called()
        self.delete.assert_not_called()
        self.notify.assert_called_once()
        self.live.update(payload)
        self.sync()
        self.update.assert_called_once()
        self.notify.assert_called_once()

    def test_note_updates_and_clears_when_only_milestone_semantics_change(self):
        self.arrival()
        self.sync()
        self.live.update(self.update.call_args.args[1][0])
        # Same destination times, but source now specifies a real minute of work.
        self.service['ends_at'] = '2026-10-15T14:46:00+08:00'
        self.sync()
        payload = self.update.call_args.args[1][0]
        self.assertNotIn('On site at', app._note_html(payload))
        self.assertIsNone(self.state['shifts']['10']['arrivalNote'])
        self.live.update(payload)
        self.service['ends_at'] = self.service['starts_at']
        self.sync()
        self.assertEqual(app._note_html(self.update.call_args.args[1][0]).count('On site at'), 1)

    def test_notes_use_original_perth_arrival_at_noon_and_midnight(self):
        self.arrival()
        for value, label in [('2026-10-15T04:00:00Z', '12:00pm'),
                             ('2026-12-31T15:59:00Z', '11:59pm'),
                             ('2026-12-31T16:00:00Z', '12:00am')]:
            self.service.update(starts_at=value, ends_at=value)
            self.sync()
            payload = self.update.call_args.args[1][0]
            self.assertIn(f'On site at {label} sharp', app._note_html(payload))
            self.assertEqual(app._note_html(payload).count('On site at'), 1)
            self.live.update(payload)

    def test_invalid_and_completed_transport_preserve_existing_shift(self):
        self.arrival()
        for changes in ({'ends_at': '2026-10-14T14:45:00+08:00'},
                        {'ends_at': None},
                        {'ends_at': self.service['starts_at'], 'status_name': 'Completed'}):
            self.service.update(changes)
            self.sync()
        self.assertEqual(self.state['shifts']['10']['shiftIds'], ['s1'])
        self.update.assert_not_called()
        self.create.assert_not_called()
        self.delete.assert_not_called()
        self.notify.assert_not_called()

    def test_past_arrival_update_remains_silent(self):
        self.arrival('2026-10-13T14:45:00+08:00')
        self.state['shifts']['10'].update(startTime=notifications.NOW-7200,
                                         endTime=notifications.NOW-3600)
        self.sync()
        self.update.assert_called_once()
        self.notify.assert_not_called()
