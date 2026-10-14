"""Durability and bounded read retries, independent of service credentials."""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import ssl
import tempfile
import time
from pathlib import Path

import certifi
import httpx


class RecoveryRequired(RuntimeError):
    """Stop writes when an upstream outcome or local identity is uncertain."""


def validate_state(state):
    if not isinstance(state, dict) or not isinstance(state.get('shifts'), dict):
        raise RecoveryRequired('Invalid tracking state; restore a verified snapshot before syncing')
    for key, entry in state['shifts'].items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            raise RecoveryRequired('Invalid tracking entry')
        ids = entry.get('shiftIds', [entry['shiftId']] if 'shiftId' in entry else [])
        if not isinstance(ids, list) or any(not isinstance(i, (str, int)) for i in ids):
            raise RecoveryRequired('Invalid tracked shift identity')
    for field in ('pending_creates', 'retry_opportunities', 'exceptions'):
        if field in state and not isinstance(state[field], dict):
            raise RecoveryRequired(f'Invalid state field: {field}')
    return state


def atomic_write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name+'.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def load_state(path: Path):
    if not path.exists():
        if any(path.with_suffix(path.suffix+suffix).exists() for suffix in ('.bak', '.pre-hardening')):
            raise RecoveryRequired('Tracking state missing but backup exists; refusing fresh sync')
        return {'shifts': {}, 'jobs': {}, 'job_title_cache': {}, 'poll_cursor': None}
    try:
        return validate_state(json.loads(path.read_text()))
    except (OSError, ValueError) as exc:
        raise RecoveryRequired('Tracking state unreadable; refusing to start with empty mappings') from exc


def save_state(path: Path, state):
    validate_state(state)
    data = json.dumps(state, allow_nan=False).encode()
    if path.exists():
        old = path.read_bytes()
        validate_state(json.loads(old))
        baseline = path.with_suffix(path.suffix+'.pre-hardening')
        if not baseline.exists():
            atomic_write(baseline, old)
        atomic_write(path.with_suffix(path.suffix+'.bak'), old)
    atomic_write(path, data)


@contextlib.contextmanager
def state_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_suffix(path.suffix+'.lock'), 'a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


# A shared immutable TLS context avoids loading/parsing the CA bundle every poll.
# Clients still close sockets at the end of each poll/webhook.
_TLS_CONTEXT = ssl.create_default_context(cafile=certifi.where())


class ReadRetryClient(httpx.Client):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('verify', _TLS_CONTEXT)
        super().__init__(*args, **kwargs)
        self.bridge_cache = {}

    def request(self, method, url, **kwargs):
        method = method.upper()
        # Only reads retry automatically. An ambiguous POST must reconcile.
        attempts = 3 if method == 'GET' else 1
        cacheable = method == 'GET' and ('/jobs/v1/jobs' in str(url) or '/api/v1/members/' in str(url))
        key = (str(url), repr(kwargs.get('params')))
        if cacheable and key in self.bridge_cache:
            return self.bridge_cache[key]
        for attempt in range(attempts):
            try:
                response = super().request(method, url, **kwargs)
            except httpx.TransportError:
                if attempt + 1 == attempts:
                    raise
                time.sleep(2 ** attempt)
                continue
            if response.status_code not in {429, 500, 502, 503, 504} or attempt + 1 == attempts:
                if cacheable and response.is_success and len(self.bridge_cache) < 256:
                    self.bridge_cache[key] = response
                return response
            try:
                delay = float(response.headers.get('Retry-After', 2 ** attempt))
            except ValueError:
                delay = 2 ** attempt
            response.close()
            time.sleep(max(0, min(delay, 30)))

    def invalidate_jobs(self):
        self.bridge_cache = {k: v for k, v in self.bridge_cache.items() if '/jobs/v1/jobs' not in k[0]}
