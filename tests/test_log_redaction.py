"""Dummy credentials only; no live requests, credentials or notifications."""
import io
import logging
import sys
import os
import subprocess
import unittest

import httpx
from uvicorn.logging import AccessFormatter
from log_redaction import RedactingFormatter, install_log_redaction, redact_query_credentials


class LogRedactionTest(unittest.TestCase):
    def test_encoded_repeated_names_values_and_google_chat_keys(self):
        source = ('/webhooks/current-rms/opportunity-converted?token=dummy-one'
                  '&%74oken=dummy%26two;tail&ToKeN=dummy-three&%2574oken=dummy-four'
                  '&api_key=dummy-five&job=3830')
        result = redact_query_credentials(source)
        self.assertNotIn('dummy', result)
        self.assertEqual(result.count('[REDACTED]'), 5)
        self.assertIn('job=3830', result)
        chat = 'https://chat.googleapis.com/v1/spaces/example/messages?key=dummy-key&token=dummy-token'
        self.assertEqual(redact_query_credentials(chat),
                         chat.replace('dummy-key', '[REDACTED]').replace('dummy-token', '[REDACTED]'))

    def test_success_and_error_access_logs_keep_request_metadata(self):
        formatter = RedactingFormatter(AccessFormatter(
            '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            use_colors=False))
        for status in (200, 400, 403, 404, 422, 500):
            with self.subTest(status=status):
                args = ('127.0.0.1:1234', 'POST', '/webhook?token=dummy-secret&event=change', '1.1', status)
                record = logging.LogRecord('uvicorn.access', logging.INFO, '', 0,
                                           '%s - "%s %s HTTP/%s" %d', args, None)
                output = formatter.format(record)
                self.assertNotIn('dummy-secret', output)
                self.assertIn('127.0.0.1:1234', output)
                self.assertIn('POST /webhook?token=[REDACTED]&event=change HTTP/1.1', output)
                self.assertIn(str(status), output)
                self.assertEqual(record.args, args)

    def test_httpx_info_and_exception_traceback(self):
        sink = io.StringIO()
        handler = logging.StreamHandler(sink)
        handler.setFormatter(RedactingFormatter(logging.Formatter('%(levelname)s %(message)s')))
        logger = logging.Logger('offline')
        logger.addHandler(handler)
        url = 'https://chat.googleapis.com/v1/spaces/example/messages?key=dummy-key&token=dummy-token'
        logger.info('HTTP Request: %s %s "%s"', 'POST', url, 'HTTP/1.1 403 Forbidden')
        try:
            httpx.Response(403, request=httpx.Request('POST', url)).raise_for_status()
        except httpx.HTTPStatusError:
            logger.exception('Failed to post ops notification')
        output = sink.getvalue()
        self.assertNotIn('dummy-', output)
        self.assertIn('Traceback', output)
        self.assertIn('HTTPStatusError', output)
        self.assertIn('403', output)
        self.assertIn('Failed to post ops notification', output)
        self.assertIn('key=[REDACTED]&token=[REDACTED]', output)

    def test_ordinary_logs_unchanged(self):
        for text in ('Poll checked 1 opportunit(y/ies)', '/healthz',
                     'https://offline.invalid/items?page=2&limit=100',
                     'invalid or missing token'):
            self.assertEqual(redact_query_credentials(text), text)

    def test_install_wraps_all_output_handlers_and_is_idempotent(self):
        loggers = [logging.getLogger(name) for name in (None, 'uvicorn', 'uvicorn.access', 'uvicorn.error')]
        originals = [logger.handlers for logger in loggers]
        try:
            for logger in loggers:
                logger.handlers = [logging.StreamHandler(io.StringIO())]
            install_log_redaction()
            formatters = [logger.handlers[0].formatter for logger in loggers]
            self.assertTrue(all(isinstance(f, RedactingFormatter) for f in formatters))
            install_log_redaction()
            self.assertEqual(formatters, [logger.handlers[0].formatter for logger in loggers])
        finally:
            for logger, handlers in zip(loggers, originals):
                logger.handlers = handlers

    def test_actual_uvicorn_configuration_installs_redaction_on_app_import(self):
        env = os.environ.copy()
        for key in ('CURRENT_RMS_SUBDOMAIN', 'CURRENT_RMS_API_KEY', 'CONNECTEAM_API_KEY', 'CONNECTEAM_SCHEDULER_ID'):
            env[key] = 'offline-test'
        env.update(ENABLE_SCHEDULER='false', GOOGLE_CHAT_WEBHOOK_URL='', WEBHOOK_TOKEN='dummy-only')
        code = """
import logging
import uvicorn
uvicorn.Config('app:app', use_colors=False).load()
logging.getLogger('uvicorn.access').info('%s - \"%s %s HTTP/%s\" %d',
    '127.0.0.1:1234', 'POST', '/webhooks/current-rms/opportunity-converted?%74oken=dummy-only', '1.1', 403)
logging.getLogger('httpx').info('POST https://chat.googleapis.com/example?key=dummy-key&token=dummy-token')
try:
    raise RuntimeError('https://offline.invalid/?token=dummy-error')
except RuntimeError:
    logging.getLogger('uvicorn.error').exception('Offline error test')
"""
        result = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = result.stdout + result.stderr
        self.assertNotIn('dummy-', output)
        self.assertIn('POST /webhooks/current-rms/opportunity-converted?%74oken=[REDACTED] HTTP/1.1', output)
        self.assertIn('403', output)
        self.assertIn('key=[REDACTED]&token=[REDACTED]', output)
        self.assertIn('RuntimeError', output)
