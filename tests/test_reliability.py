"""Offline failures only; no live upstream calls or notifications."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import test_notifications as notifications
import app
import bridge_safety as safety


class DurabilityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'state.json'
        self.state = {'shifts': {'10': {'shiftIds': ['s1']}}, 'poll_cursor': 'old'}

    def test_atomic_failure_keeps_previous_and_backups(self):
        safety.save_state(self.path, self.state)
        changed = {**self.state, 'poll_cursor': 'new'}
        real_replace = safety.os.replace
        def fail_main(src, dst):
            if dst == self.path:
                raise OSError('injected crash before replace')
            real_replace(src, dst)
        with patch.object(safety.os, 'replace', side_effect=fail_main):
            with self.assertRaises(OSError):
                safety.save_state(self.path, changed)
        self.assertEqual(safety.load_state(self.path), self.state)
        self.assertEqual(json.loads(Path(str(self.path)+'.bak').read_text()), self.state)
        self.assertEqual(json.loads(Path(str(self.path)+'.pre-hardening').read_text()), self.state)
        self.assertFalse(list(self.path.parent.glob('*.tmp')))

    def test_corrupt_or_missing_state_never_silently_rebuilds(self):
        self.path.write_text('{broken')
        with self.assertRaises(safety.RecoveryRequired):
            safety.load_state(self.path)
        self.path.unlink()
        Path(str(self.path)+'.bak').write_text('{}')
        with self.assertRaises(safety.RecoveryRequired):
            safety.load_state(self.path)

    def test_read_retry_and_write_no_retry(self):
        calls = []
        def request(req):
            calls.append(req.method)
            return app.httpx.Response(503 if len(calls) < 3 else 200, json={})
        with patch.object(safety.time, 'sleep'), safety.ReadRetryClient(transport=app.httpx.MockTransport(request)) as c:
            self.assertEqual(c.get('https://offline.invalid/data').status_code, 200)
            calls.clear()
            self.assertEqual(c.post('https://offline.invalid/data').status_code, 503)
        self.assertEqual(calls, ['POST'])

    def test_api_cache_scoped_to_client_and_invalidatable(self):
        handler = Mock(return_value=app.httpx.Response(200, json={}))
        with safety.ReadRetryClient(transport=app.httpx.MockTransport(handler)) as c:
            for _ in range(4):
                c.get('https://offline.invalid/jobs/v1/jobs', params={'offset':0})
            self.assertEqual(handler.call_count, 1)
            c.invalidate_jobs()
            c.get('https://offline.invalid/jobs/v1/jobs', params={'offset':0})
            self.assertEqual(handler.call_count, 2)


class ReliabilitySyncTest(notifications.NotificationsTest):
    # Inherit the fixture, but keep the original suite in its own class only.
    def test_create_intent_written_before_post_and_ambiguous_result_not_replayed(self):
        self.state['shifts'].clear()
        def timeout(*args):
            saved=json.loads(app.STATE_FILE.read_text())
            self.assertIn('10',saved['pending_creates'])
            raise app.httpx.ReadTimeout('after POST')
        self.create.side_effect=timeout
        with self.assertRaises(app.httpx.ReadTimeout):
            self.sync()
        with patch.object(app,'list_shifts',return_value=[]):
            with self.assertRaises(safety.RecoveryRequired):
                self.sync()
        self.assertEqual(self.create.call_count,1)

    def test_create_timeout_reconciles_exact_marker_before_source_delete(self):
        self.state['shifts'].clear()
        self.create.side_effect=app.httpx.ReadTimeout('uncertain')
        with self.assertRaises(app.httpx.ReadTimeout): self.sync()
        marker=self.state['pending_creates']['10']['marker']
        self.opportunity['status_name']='Dead'
        with patch.object(app,'list_shifts',return_value=[{'id':'accepted','notes':[{'html':marker}]}]):
            self.sync()
        self.assertEqual(self.create.call_count,1)
        self.delete.assert_called_once_with(self.client,'accepted')
        self.assertFalse(self.state['pending_creates'])

    def test_partial_create_commits_identity_before_later_failure(self):
        self.state['shifts'].clear()
        second={**self.service,'id':11}
        self.create.side_effect=[[{'id':'first'}],app.httpx.ReadTimeout('second uncertain')]
        with patch.object(app,'fetch_service_items',return_value=[self.service,second]):
            with self.assertRaises(app.httpx.ReadTimeout): self.sync()
        disk=json.loads(app.STATE_FILE.read_text())
        self.assertEqual(disk['shifts']['10']['shiftIds'],['first'])
        self.assertIn('11',disk['pending_creates'])
        self.assertNotIn('10',disk['pending_creates'])

    def test_partial_update_response_keeps_old_state(self):
        self.service['description']='New'
        self.update.return_value=[]
        self.update.side_effect=None
        with self.assertRaises(safety.RecoveryRequired): self.sync()
        self.assertEqual(self.state['shifts']['10']['description'],'Setup')
        self.notify.assert_not_called()

    def test_completed_item_without_dates_protects_all_old_segments(self):
        self.state['shifts']={'10:0':copy.deepcopy(self.entry),'10:1':{**self.entry,'shiftIds':['s2']}}
        self.service.update(status_name='Completed',starts_at=None,ends_at=None)
        self.sync()
        self.assertEqual(set(self.state['shifts']),{'10:0','10:1'})
        self.delete.assert_not_called(); self.create.assert_not_called(); self.update.assert_not_called()

    def test_changed_split_layout_is_held_without_losing_assignments(self):
        self.service['ends_at']='2026-10-18T12:00:00+08:00'
        self.sync()
        self.assertEqual(self.state['shifts']['10']['shiftIds'],['s1'])
        self.delete.assert_not_called(); self.create.assert_not_called(); self.update.assert_not_called()
        self.assertIn('layout',self.state['exceptions']['1']['message'])

    def test_milestone_and_missing_dates_preserve_existing_shift(self):
        for end in (self.service['starts_at'],None):
            self.service['ends_at']=end
            self.sync()
            self.assertIn('10',self.state['shifts'])
        self.delete.assert_not_called(); self.create.assert_not_called(); self.update.assert_not_called()

    def test_cancelled_missing_dates_alerts_using_old_schedule(self):
        self.service.update(status_name='Cancelled',starts_at=None,ends_at=None)
        self.sync()
        self.notify.assert_called_once()
        self.delete.assert_not_called(); self.update.assert_not_called()

    def test_cancelled_opportunity_without_services_is_non_destructive(self):
        self.opportunity['status_name']='Cancelled'
        with patch.object(app,'fetch_service_items',return_value=[]):
            self.sync()
        self.notify.assert_called_once()
        self.delete.assert_not_called()
        self.assertIn('10',self.state['shifts'])

    def test_postponed_service_is_not_rewritten(self):
        self.service.update(status_name='Postponed',description='Changed')
        self.sync()
        self.delete.assert_not_called(); self.update.assert_not_called()

    def test_manual_notes_and_assignments_preserved_on_update(self):
        self.live.update(assignedUserIds=[71],notes=[{'type':'html','html':'<p>Auto-created from Current RMS order 1001 (opportunity item #10).</p><p>Site contact: old</p><p>Manual instruction</p>'},{'type':'html','html':'<p>Manual note two</p>'}],customFields=[{'customFieldId':'manual','value':'keep'}])
        self.service['description']='New'
        self.sync()
        payload=self.update.call_args.args[1][0]
        self.assertNotIn('assignedUserIds',payload)
        self.assertNotIn('isPublished',payload)
        html=' '.join(n['html'] for n in payload['notes'])
        self.assertIn('Manual instruction',html); self.assertIn('Manual note two',html)
        self.assertNotIn('Site contact: old',html)
        self.assertNotIn('manual',[f['customFieldId'] for f in payload['customFields']])

    def test_non_html_manual_note_holds_update(self):
        self.live['notes']=[{'type':'file','files':['keep']}]
        self.service['description']='New'
        with self.assertRaises(safety.RecoveryRequired): self.sync()
        self.update.assert_not_called()

    def test_owned_address_clears_but_manual_override_is_held(self):
        self.state['shifts']['10']['address']='old'
        self.live['locationData']={'gps':{'address':'manual'}}
        with self.assertRaises(safety.RecoveryRequired): self.sync()
        self.update.assert_not_called()
        self.live['locationData']['gps']['address']='old'
        self.sync()
        self.assertEqual(self.update.call_args.args[1][0]['locationData'],{'isReferencedToJob':False,'gps':None})

    def test_missing_job_mapping_holds_instead_of_silently_retaining_wrong_job(self):
        with patch.object(app,'find_job_by_service_name',return_value=None):
            with self.assertRaises(safety.RecoveryRequired): self.sync()
        self.update.assert_not_called()

    def test_source_404_uses_cleanup(self):
        response=app.httpx.Response(404,request=app.httpx.Request('GET','https://offline.invalid/opportunity'))
        with patch.object(app,'fetch_opportunity',side_effect=app.httpx.HTTPStatusError('deleted',request=response.request,response=response)):
            self.sync()
        self.delete.assert_called_once()
        self.notify.assert_called_once()

    def test_failed_opportunity_survives_cursor_advance_and_retries(self):
        app._save_state(self.state)
        client=MagicMock()
        with patch.object(app,'ReadRetryClient',return_value=client), patch.object(app,'fetch_opportunities_updated_since',return_value=[1]), patch.object(app,'sync_opportunity',side_effect=app.httpx.ReadTimeout('offline')):
            result=app.poll_all_open_orders()
        self.assertEqual(result['results']['1']['status'],'error')
        disk=app._load_state(); self.assertIn('1',disk['retry_opportunities']); self.assertTrue(disk['poll_cursor'])
        with patch.object(app,'ReadRetryClient',return_value=client), patch.object(app,'fetch_opportunities_updated_since',return_value=[]), patch.object(app,'sync_opportunity',return_value={'status':'ok'}) as sync:
            app.poll_all_open_orders()
        sync.assert_called_once()
        self.assertFalse(app._load_state()['retry_opportunities'])


class ContractTest(unittest.TestCase):
    def test_exclusive_midnight_day_rate_and_cap(self):
        start=app._to_epoch_seconds('2026-10-14T08:00:00+08:00')
        end=app._to_epoch_seconds('2026-10-16T00:00:00+08:00')
        segments=app._split_into_daily_segments(start,end,True)
        self.assertEqual(len(segments),2); self.assertEqual(segments[-1][1],end)
        start=app._to_epoch_seconds('2026-10-01T00:00:00+08:00')
        self.assertEqual(len(app._split_into_daily_segments(start,start+14*86400,True)),14)
        self.assertIsNone(app._split_into_daily_segments(start,start+14*86400+1,True))

    def test_exact_item_and_old_time_for_sibling_identity(self):
        rows=[{'id':'good','startTime':1,'endTime':2,'notes':[{'html':'(opportunity item #10)'}]}, {'id':'other','startTime':1,'endTime':2,'notes':[{'html':'(opportunity item #100)'}]}, {'id':'wrong-time','startTime':3,'endTime':4,'notes':[{'html':'(opportunity item #10)'}]}]
        with patch.object(app,'list_shifts',return_value=rows):
            self.assertEqual(app._find_sibling_shift_ids(Mock(),'1001','changed','changed',1,2,'10'),['good'])
            self.assertEqual(app._find_sibling_shift_ids(Mock(),'1001','changed','changed',1,2),[])

    def test_shift_pagination_uses_next_offset_and_rejects_stuck_pages(self):
        client=Mock()
        first=[{'id':str(i)} for i in range(500)]
        response=lambda rows: Mock(json=Mock(return_value={'data':{'shifts':rows}}))
        client.get.side_effect=[response(first),response([{'id':'last'}])]
        self.assertEqual(len(app.list_shifts(client,1,2)),501)
        self.assertEqual(client.get.call_args.kwargs['params']['offset'],500)
        client.get.side_effect=[response(first),response(first)]
        with self.assertRaises(safety.RecoveryRequired): app.list_shifts(client,1,2)

    def test_bridge_identity_note_not_duplicated(self):
        marker='<p>Bridge identity: '+'a'*32+'</p>'
        self.assertEqual(app._merge_owned_notes({'notes':[{'html':marker}]},[{'html':marker}]),[{'html':marker}])


# Avoid counting inherited notification tests twice in discovery.
for _name in list(notifications.NotificationsTest.__dict__):
    if _name.startswith('test_') and _name not in ReliabilitySyncTest.__dict__:
        setattr(ReliabilitySyncTest,_name,None)
