"""
Current RMS -> Connecteam draft shift bridge.

An opportunity is eligible for draft shifts when it's in "Order" state
(always), or in "Quotation" state with the "Draft shifts in Connecteam"
Yes/No custom field ticked. Two paths keep Connecteam in sync:

  1. Webhooks fire instantly on opportunity_convert_to_order, opportunity_
     update, opportunity_convert_to_quotation, opportunity_revert_to_
     quotation, opportunity_mark_as_dead, and opportunity_mark_as_lost.
  2. A background poll (every POLL_INTERVAL_SECONDS, default 15 minutes)
     scans every Order- or Quotation-state opportunity updated since the
     last poll, so Service-item edits and eligibility changes are always
     eventually picked up even if a webhook is missed.

Both paths funnel into the same idempotent sync routine, keyed off each
Current RMS opportunity_item's ID:
  - First time we see an item on an eligible opportunity -> CREATE a draft
    Connecteam shift for it.
  - If we've already created a shift for that item -> UPDATE that same
    shift in place if the title/time/job/quantity changed, otherwise do
    nothing.
  - If the opportunity is no longer eligible (flag unticked, went dead/
    lost/reverted, or was deleted outright — including a deletion the poll
    discovers as a 404, not just one caught by webhook) -> DELETE any
    shifts we created that are still draft; leave already-published shifts
    alone for manual review.
  - Same teardown applies at the individual item level: if one service line
    item is removed from an opportunity that's still otherwise eligible,
    only that item's shift is cleaned up (draft deleted / published left).
  - Whenever a published shift is left in place instead of deleted, or gets
    edited in place because Current RMS changed under it, ops is notified
    in Google Chat (see notify_ops / GOOGLE_CHAT_WEBHOOK_URL) so a human
    knows to check on a shift crew may already have been told about.
This guarantees we never create duplicate shifts, no matter how many times
an opportunity is processed (webhook retries, overlapping polls, re-
conversions), and never silently blow away a shift someone has published.

It also finds a Connecteam Job per Service line item (matched by service
name, never created here), writes the Current RMS order number into the
shift's "Job No." custom field, and the required headcount into "Qty Rqrd".

See README.md for setup instructions.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rms-connecteam-bridge")

# ---------------------------------------------------------------------------
# Configuration (environment variables — see .env.example)
# ---------------------------------------------------------------------------

CURRENT_RMS_SUBDOMAIN = os.environ["CURRENT_RMS_SUBDOMAIN"]
CURRENT_RMS_API_KEY = os.environ["CURRENT_RMS_API_KEY"]
CURRENT_RMS_BASE_URL = os.environ.get("CURRENT_RMS_BASE_URL", "https://api.current-rms.com")

CONNECTEAM_API_KEY = os.environ["CONNECTEAM_API_KEY"]
CONNECTEAM_SCHEDULER_ID = os.environ["CONNECTEAM_SCHEDULER_ID"]
CONNECTEAM_BASE_URL = os.environ.get("CONNECTEAM_BASE_URL", "https://api.connecteam.com")

# Jobs in Connecteam represent TASK TYPES (e.g. "Lighting Technician"), one
# per Current RMS Service — never one per order. This service only ever
# *looks up* an existing Job by title (prefix + service name); it never
# creates Jobs. While testing in "Elite Test Schedule" the Jobs are all
# prefixed "TEST " (see /debug/create-test-jobs) so they're easy to tell
# apart from anything real. Clear this env var when pointed at production.
CONNECTEAM_JOB_PREFIX = os.environ.get("CONNECTEAM_JOB_PREFIX", "")

# Some Current RMS Service names don't map 1:1 onto a Connecteam Job — either
# because several distinct RMS services (e.g. daytime/overnight variants, or
# per-city/per-distance-type variants) are meant to share ONE task-type Job,
# or because the Job title in Connecteam has just diverged from the RMS
# service name over time. Reviewed and confirmed with the user 2026-08-17
# against the live Current RMS Service catalog and Connecteam Jobs list.
# Keys are the exact Current RMS service["name"]; values are the exact
# Connecteam Job title to look up instead. Services not listed here use
# their own name unchanged (the historical 1:1 behavior).
SERVICE_JOB_OVERRIDES: dict[str, str] = {
    "Audio Engineer - FOH - Overnight 0000 - 0700 and Sunday": "Audio Engineer - FOH",
    "Audio Engineer - MONS - Overnight 0000 - 0700 and Sunday": "Audio Engineer - MONS",
    "Audio Engineer - Systems - Overnight 0000 - 0700 and Sunday": "Audio Engineer - Systems",
    "Followspot operator - Overnight 0000 - 0700 and Sunday": "Follow spot operator",
    "Lighting operator - Overnight 0000 - 0700 and Sunday": "Lighting Operator",
    "Lighting system's technician - Overnight 0000 - 0700 and Sunday": "Lighting system's technician",
    "Lighting technician - Overnight 0000 - 0700 and Sunday": "Lighting technician",
    "Production manager - Overnight 0000 - 0700 and Sunday": "Production manager",
    "Rigger - Overnight 0000 - 0700 and Sunday": "Rigger",
    "Truck 8 Ton - Collection (Distance)": "Truck 8 Ton - Collection",
    "Truck 8 Ton - Delivery (Distance)": "Truck 8 Ton - Delivery",
    "Truck 8 Ton - Perth Collection": "Truck 8 Ton - Collection",
    "Truck 8 Ton - Perth Collection (After hours and Weekend)": "Truck 8 Ton - Collection",
    "Truck 8 Ton - Perth Delivery": "Truck 8 Ton - Delivery",
    "Truck 8 Ton - Perth Delivery (After hours and Weekend)": "Truck 8 Ton - Delivery",
    "Van 1 Ton Perth Collection": "Van 1 Ton Collection",
    "Van 1 Ton Perth Collection  (After hours and Weekend)": "Van 1 Ton Collection",
    "Van 1 Ton Perth Delivery": "Van 1 Ton Delivery",
    "Van 1 Ton Perth Delivery (After hours and Weekend)": "Van 1 Ton Delivery",
}

# Shift custom field id for the "Job No." box in the shift editor (renamed
# to "Opportunity No." by the user in the test schedule's UI). Captured live
# from Connecteam's own web app request when saving that field on a shift —
# it's genuinely per-shift and independent of whichever Job is selected, so
# it's safe to hold the Current RMS order number even though many shifts
# will share the same task-type Job. The public Shifts API takes this as
# customFields: [{"customFieldId": ..., "value": ...}].
CONNECTEAM_JOBNO_CUSTOM_FIELD_ID = int(os.environ.get("CONNECTEAM_JOBNO_CUSTOM_FIELD_ID", "1317802"))

# Shift custom field id for "Quantity Required" — created specifically so
# quantity > 1 line items (e.g. "4 x Lighting Technician") become ONE draft
# shift with this field holding the number needed, instead of N separate
# shifts. Connecteam's public API has no working multi-slot/open-shift
# field (confirmed by direct testing: isOpenShift+numOfUsers is accepted
# with HTTP 200 but never actually changes the stored openSpots when
# created or updated through the public API — only the internal,
# session-authenticated web app can do that). So admins read this field and
# manually assign that many people to the single shift.
CONNECTEAM_QTY_CUSTOM_FIELD_ID = int(os.environ.get("CONNECTEAM_QTY_CUSTOM_FIELD_ID", "1319220"))

# Shift custom field id for "Shift Type/Notes" — holds the Current RMS
# Service line item's free-text description verbatim (e.g. "Bump in Day
# 1"), confirmed live via GET /scheduler/v1/schedulers/{id}/custom-fields
# against Elite Primary Schedule. Field id, not name, is what the Shifts
# API takes.
CONNECTEAM_SHIFT_TYPE_CUSTOM_FIELD_ID = int(
    os.environ.get("CONNECTEAM_SHIFT_TYPE_CUSTOM_FIELD_ID", "1353115")
)

# Shared secret appended to protected URLs as ?token=... . Current RMS
# webhooks aren't signed, so this query-string token is the gate for the
# webhook endpoint — it's also required for /sync so randoms can't trigger
# an unscheduled full sync. Keep the URL itself private too.
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")

# Separate shared secret for the temporary /backfill endpoint below, kept
# distinct from WEBHOOK_TOKEN so a one-off admin action never risks the
# live webhook token. Unset by default — the route 403s until this is set.
BACKFILL_TOKEN = os.environ.get("BACKFILL_TOKEN", "")

# Incoming webhook URL for the ops Google Chat space. Posted to whenever a
# shift a human already published gets left in place during cleanup (would
# otherwise have been deleted) or gets edited by the sync — both cases where
# a real person may have already told crew about that shift. Unset by
# default: notify_ops() just logs and no-ops rather than failing sync, so
# this is safe to leave unconfigured in test.
GOOGLE_CHAT_WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")

# Where we remember: (a) which Connecteam shift belongs to which Current RMS
# opportunity_item (so we UPDATE instead of duplicating), and (b) how far
# back the last poll checked, so the next poll only looks at what changed.
STATE_FILE = Path(os.environ.get("STATE_FILE", "./processed_orders.json"))

MAX_SHIFT_SECONDS = 24 * 60 * 60  # Connecteam: a shift can't exceed 24h.

# Background poll: catches Service-item edits made *after* conversion (no
# Current RMS webhook fires for those). Runs every POLL_INTERVAL_SECONDS.
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", 15 * 60))
# On the very first poll (no cursor yet), look back this far to catch any
# orders converted before the service came online.
POLL_INITIAL_LOOKBACK_HOURS = int(os.environ.get("POLL_INITIAL_LOOKBACK_HOURS", 24))
# Overlap subtracted from "now" when saving the cursor, so a poll that took
# a while to run doesn't create a gap that skips an edit made mid-poll.
POLL_OVERLAP_MINUTES = 5
ENABLE_SCHEDULER = os.environ.get("ENABLE_SCHEDULER", "true").lower() == "true"

app = FastAPI(title="Current RMS -> Connecteam bridge")

# Serializes all sync work (webhook hits and the background poll can
# otherwise race on the same state file / same opportunity).
sync_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Small persistent state: shift tracking + poll cursor
# ---------------------------------------------------------------------------

def _load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text())
            data.setdefault("shifts", {})
            data.setdefault("jobs", {})  # legacy per-order job cache, unused by current code
            data.setdefault("job_title_cache", {})
            data.setdefault("poll_cursor", None)
            return data
        except (json.JSONDecodeError, OSError):
            log.warning("Could not read state file %s, starting fresh", STATE_FILE)
    return {"shifts": {}, "jobs": {}, "job_title_cache": {}, "poll_cursor": None}


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state))


# ---------------------------------------------------------------------------
# Current RMS client
# ---------------------------------------------------------------------------

def rms_headers() -> dict[str, str]:
    return {
        "X-SUBDOMAIN": CURRENT_RMS_SUBDOMAIN,
        "X-AUTH-TOKEN": CURRENT_RMS_API_KEY,
        "Content-Type": "application/json",
    }


def fetch_opportunity(client: httpx.Client, opportunity_id: int) -> dict[str, Any]:
    resp = client.get(
        f"{CURRENT_RMS_BASE_URL}/api/v1/opportunities/{opportunity_id}",
        headers=rms_headers(),
    )
    resp.raise_for_status()
    return resp.json()["opportunity"]


#  2 = Quotation, 3 = Order (confirmed live against this account's data).
CURRENT_RMS_STATE_ORDER = 3
CURRENT_RMS_STATE_QUOTATION = 2


def fetch_opportunities_updated_since(client: httpx.Client, since_iso: str) -> list[int]:
    """Return IDs of every opportunity in 'Order' OR 'Quotation' state that's
    been updated since since_iso. Used by the background poll to catch
    Service-item edits made after conversion, and Quotation-state
    opportunities that had "Draft shifts in Connecteam" ticked (or
    unticked/gone dead/lost — sync_opportunity() decides eligibility and
    handles cleanup either way)."""
    ids: set[int] = set()
    for state_id in (CURRENT_RMS_STATE_ORDER, CURRENT_RMS_STATE_QUOTATION):
        page = 1
        while True:
            resp = client.get(
                f"{CURRENT_RMS_BASE_URL}/api/v1/opportunities",
                headers=rms_headers(),
                params={
                    "q[state_eq]": state_id,
                    "q[updated_at_gteq]": since_iso,
                    "per_page": 100,
                    "page": page,
                    "sort": "-updated_at",
                },
            )
            resp.raise_for_status()
            body = resp.json()
            ids.update(o["id"] for o in body["opportunities"])
            meta = body.get("meta", {})
            if page * meta.get("per_page", 100) >= meta.get("total_row_count", 0):
                break
            page += 1
    return list(ids)


def _is_eligible(opportunity: dict[str, Any]) -> tuple[bool, str]:
    """Order state is always eligible (existing behavior). Quotation state is
    eligible only when the "Draft shifts in Connecteam" Yes/No custom field
    is ticked — Current RMS returns this as custom_fields.
    draft_shifts_in_connecteams == "Yes" (exact string, confirmed live).
    Everything else (Enquiry, dead, lost, etc.) is not eligible."""
    state_name = opportunity.get("state_name")
    if state_name == "Order":
        return True, "order"
    if state_name == "Quotation":
        flag = (opportunity.get("custom_fields") or {}).get("draft_shifts_in_connecteams")
        if (flag or "").strip().lower() == "yes":
            return True, "quotation_flagged"
        return False, "quotation_not_flagged"
    return False, f"state '{state_name}' not eligible"


def fetch_venue_address(client: httpx.Client, opportunity: dict[str, Any]) -> str | None:
    """Return a formatted address string for the opportunity's venue, or
    None if the opportunity has no venue set."""
    venue_id = opportunity.get("venue_id")
    if not venue_id:
        return None

    resp = client.get(
        f"{CURRENT_RMS_BASE_URL}/api/v1/members/{venue_id}",
        headers=rms_headers(),
    )
    resp.raise_for_status()
    member = resp.json()["member"]
    addr = member.get("primary_address")
    if not addr:
        return None

    parts = [
        addr.get("street"),
        addr.get("city"),
        addr.get("county"),
        addr.get("postcode"),
        addr.get("country_name"),
    ]
    return ", ".join(p for p in parts if p)


def fetch_service_items(client: httpx.Client, opportunity_id: int) -> list[dict[str, Any]]:
    """Return opportunity_items where item_type == 'Service' and both
    starts_at/ends_at are populated (i.e. an actual scheduled service, not a
    group/header row)."""
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        resp = client.get(
            f"{CURRENT_RMS_BASE_URL}/api/v1/opportunities/{opportunity_id}/opportunity_items",
            headers=rms_headers(),
            params={"page": page, "per_page": 100},
        )
        resp.raise_for_status()
        body = resp.json()
        items.extend(body["opportunity_items"])
        meta = body.get("meta", {})
        if page * meta.get("per_page", 100) >= meta.get("total_row_count", 0):
            break
        page += 1

    return [
        item
        for item in items
        if item.get("item_type") == "Service" and item.get("starts_at") and item.get("ends_at")
    ]


# ---------------------------------------------------------------------------
# Connecteam client
# ---------------------------------------------------------------------------

def _to_epoch_seconds(iso_ts: str) -> int:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    return int(dt.astimezone(timezone.utc).timestamp())


def _format_epoch(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%a %d %b %Y, %H:%M UTC")


def _load_job_title_cache(client: httpx.Client) -> tuple[dict[str, str], dict[str, str]]:
    """Fetch every Job in this scheduler and index by exact title, and
    separately by jobId -> color (so shift payloads can inherit the Job's
    current color). Jobs are curated task types (see
    /debug/create-test-jobs) — this never creates one, only looks them up.

    Connecteam's Jobs list can contain more than one Job with the exact same
    title — e.g. a soft-deleted duplicate left behind after a Job was
    recreated (confirmed live via /debug/inspect-job-mapping: two "Lighting
    Operator" entries, one isDeleted:true). Deleted/archived Jobs are
    excluded here so a title collision can never have the dead entry win
    just because it happened to sort later in the page — a shift payload
    that references a deleted jobId is rejected by Connecteam outright."""
    headers = {"X-API-KEY": CONNECTEAM_API_KEY}
    by_title: dict[str, str] = {}
    by_jobid_color: dict[str, str] = {}
    offset = 0
    while True:
        resp = client.get(
            f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
            headers=headers,
            params={"instanceIds": CONNECTEAM_SCHEDULER_ID, "limit": 500, "offset": offset},
        )
        resp.raise_for_status()
        body = resp.json()
        jobs = body.get("data", {}).get("jobs", [])
        for j in jobs:
            if j.get("isDeleted") or j.get("isArchived"):
                continue
            if j.get("title"):
                by_title[j["title"]] = j["jobId"]
            if j.get("color"):
                by_jobid_color[j["jobId"]] = j["color"]
        if len(jobs) < 500:
            break
        offset = body.get("paging", {}).get("offset", offset + 500)
    return by_title, by_jobid_color


def find_job_by_service_name(
    client: httpx.Client, service_name: str, jobs_state: dict[str, Any]
) -> str | None:
    """Look up the task-type Job whose title is
    f"{CONNECTEAM_JOB_PREFIX}{SERVICE_JOB_OVERRIDES.get(service_name, service_name)}"
    (e.g. "TEST Lighting Technician" while testing, or just "Lighting
    Technician" in production) — most services map onto a Job of the same
    name, but SERVICE_JOB_OVERRIDES redirects the handful that don't (see
    its comment). Returns None — and logs a warning — if no matching Job
    exists; it does NOT create one. jobs_state is a title->jobId cache
    persisted in the state file so repeated syncs don't refetch the whole
    Jobs list every time."""
    mapped_name = SERVICE_JOB_OVERRIDES.get(service_name, service_name)
    # Built with an explicit space rather than relying on a trailing space
    # surviving inside CONNECTEAM_JOB_PREFIX itself — env var UIs (Render
    # included) tend to silently trim trailing whitespace on save, which
    # would otherwise turn "TEST Lighting technician" into
    # "TESTLighting technician" and break every lookup.
    prefix = CONNECTEAM_JOB_PREFIX.strip()
    title = f"{prefix} {mapped_name}" if prefix else mapped_name

    cache: dict[str, str] = jobs_state.setdefault("by_title", {})
    color_cache: dict[str, str] = jobs_state.setdefault("by_jobid_color", {})

    if title in cache:
        job_id = cache[title]
        if job_id not in color_cache:
            # Color cache is missing/stale for this job (e.g. state file
            # predates the color-cache feature, or the Job's color changed
            # since the cache was built) — refresh once so callers get an
            # up-to-date color without waiting for an unrelated title miss.
            by_title, by_jobid_color = _load_job_title_cache(client)
            cache.clear()
            cache.update(by_title)
            color_cache.clear()
            color_cache.update(by_jobid_color)
        return job_id

    # Cache miss: refresh the whole title->jobId (and jobId->color) map once
    # (cheap — Jobs lists are small) and look again, in case a Job was added
    # since the cache was last built.
    by_title, by_jobid_color = _load_job_title_cache(client)
    cache.clear()
    cache.update(by_title)
    color_cache.clear()
    color_cache.update(by_jobid_color)

    job_id = cache.get(title)
    if job_id is None:
        log.warning(
            "No Connecteam Job titled '%s' found for scheduler %s — shift will be created without a Job",
            title, CONNECTEAM_SCHEDULER_ID,
        )
    return job_id


def _is_stale_job_id_error(exc: httpx.HTTPStatusError) -> bool:
    """True if Connecteam rejected a shift create/update because a cached
    Job ID no longer exists (e.g. the Job was deleted/archived in
    Connecteam after find_job_by_service_name cached it). The title->jobId
    cache only ever refreshes on a title *miss*, never revalidates a hit,
    so a deleted Job's ID can sit there stale indefinitely. Triggers a
    one-time forced cache refresh + retry rather than failing the whole
    sync — see sync_opportunity."""
    text = exc.response.text.lower()
    return "job_id" in text and "does not exist" in text


def get_job_color(job_id: str | None, jobs_state: dict[str, Any]) -> str | None:
    """Look up the current color of a Job from the cache populated by
    find_job_by_service_name. Returns None if unknown (job_id missing, or
    the color cache hasn't been built yet) — callers should omit the
    "color" key entirely in that case rather than send a bogus value."""
    if not job_id:
        return None
    return jobs_state.get("by_jobid_color", {}).get(job_id)


def create_shifts(client: httpx.Client, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not payloads:
        return []
    url = f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts"
    headers = {"X-API-KEY": CONNECTEAM_API_KEY, "Content-Type": "application/json"}
    created: list[dict[str, Any]] = []
    for i in range(0, len(payloads), 500):
        chunk = payloads[i : i + 500]
        resp = client.post(url, headers=headers, json=chunk, params={"notifyUsers": "false"})
        if resp.status_code >= 400:
            log.error("Connecteam rejected shift creation: %s %s", resp.status_code, resp.text)
            resp.raise_for_status()
        created.extend(resp.json().get("data", {}).get("shifts", []))
    return created


def update_shifts(client: httpx.Client, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not payloads:
        return []
    url = f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts"
    headers = {"X-API-KEY": CONNECTEAM_API_KEY, "Content-Type": "application/json"}
    updated: list[dict[str, Any]] = []
    for i in range(0, len(payloads), 500):
        chunk = payloads[i : i + 500]
        resp = client.put(url, headers=headers, json=chunk, params={"notifyUsers": "false"})
        if resp.status_code >= 400:
            log.error("Connecteam rejected shift update: %s %s", resp.status_code, resp.text)
            resp.raise_for_status()
        updated.extend(resp.json().get("data", {}).get("shifts", []))
    return updated


def notify_ops(client: httpx.Client, text: str) -> None:
    """Post a message to the ops Google Chat space. Best-effort: logs and
    swallows failures instead of raising, so a Chat outage never breaks a
    sync that otherwise succeeded."""
    if not GOOGLE_CHAT_WEBHOOK_URL:
        log.warning("GOOGLE_CHAT_WEBHOOK_URL not set — skipping ops notification: %s", text)
        return
    try:
        resp = client.post(GOOGLE_CHAT_WEBHOOK_URL, json={"text": text})
        resp.raise_for_status()
    except httpx.HTTPError:
        log.exception("Failed to post ops notification to Google Chat: %s", text)


def get_shift(client: httpx.Client, shift_id: str) -> dict[str, Any] | None:
    """Fetch a single shift; returns None if it no longer exists (e.g.
    already deleted by hand in Connecteam)."""
    headers = {"X-API-KEY": CONNECTEAM_API_KEY}
    resp = client.get(
        f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts/{shift_id}",
        headers=headers,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["data"]


def delete_shift(client: httpx.Client, shift_id: str) -> None:
    headers = {"X-API-KEY": CONNECTEAM_API_KEY}
    resp = client.request(
        "DELETE",
        f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts/{shift_id}",
        headers=headers,
    )
    if resp.status_code >= 400 and resp.status_code != 404:
        log.error("Connecteam rejected shift deletion %s: %s %s", shift_id, resp.status_code, resp.text)
        resp.raise_for_status()


def _cleanup_shift_keys(
    client: httpx.Client,
    keys: list[str],
    shifts_state: dict[str, Any],
    opportunity_id: int,
    reason: str,
) -> dict[str, Any]:
    """Shared teardown for a set of tracked shift keys that should no longer
    exist (whole opportunity ineligible, or an individual service item gone
    from an opportunity that's otherwise still eligible). Draft shifts are
    deleted outright. A shift a human already published is left in place —
    real scheduling work has gone into it — but ops gets a Google Chat ping
    since without this bridge would have deleted it."""
    deleted = 0
    left_published = 0
    already_gone = 0
    for key in keys:
        shift_id = shifts_state[key]["shiftId"]
        shift = get_shift(client, shift_id)
        if shift is None:
            already_gone += 1
            del shifts_state[key]
            continue
        if shift.get("isPublished"):
            left_published += 1
            log.warning(
                "Opportunity %s, shift %s should be removed (%s) but is already published — "
                "leaving it in place for manual review.",
                opportunity_id, shift_id, reason,
            )
            order_number = shifts_state[key].get("orderNumber")
            title = shifts_state[key].get("title")
            notify_ops(
                client,
                f"⚠️ Published Connecteam shift needs manual review.\n"
                f"Opportunity {opportunity_id} (order {order_number}) — {reason}.\n"
                f"Shift \"{title}\" ({shift_id}) is already published, so the bridge left it in place "
                f"instead of deleting it. Please review/cancel it in Connecteam.",
            )
            continue
        delete_shift(client, shift_id)
        deleted += 1
        del shifts_state[key]

    return {
        "tracked_shifts": len(keys),
        "deleted_draft_count": deleted,
        "left_published_count": left_published,
        "already_gone_count": already_gone,
    }


def cleanup_ineligible_opportunity(
    client: httpx.Client, opportunity_id: int, reason: str, state: dict[str, Any]
) -> dict[str, Any]:
    """An opportunity that used to be eligible (Order, or flagged Quotation)
    no longer is — flag was unticked, it went dead/lost/reverted, or it was
    deleted outright. All shifts tracked for it are torn down via
    _cleanup_shift_keys (draft deleted, published left + notified)."""
    shifts_state: dict[str, Any] = state["shifts"]
    keys = [k for k, v in shifts_state.items() if v.get("opportunityId") == opportunity_id]
    if not keys:
        return {"status": "skipped", "reason": reason, "tracked_shifts": 0}

    result = _cleanup_shift_keys(client, keys, shifts_state, opportunity_id, reason)
    return {"status": "cleaned_up", "reason": reason, **result}


# ---------------------------------------------------------------------------
# Core sync — idempotent, keyed off Current RMS opportunity_item IDs
# ---------------------------------------------------------------------------

def sync_opportunity(client: httpx.Client, opportunity_id: int, state: dict[str, Any]) -> dict[str, Any]:
    """Create/update draft Connecteam shifts for one order, and its linked
    Job. Mutates `state` in place; caller is responsible for persisting it.
    Safe to call repeatedly for the same opportunity — matches existing
    shifts by opportunity_item ID and only sends an update when something
    actually changed."""
    opportunity = fetch_opportunity(client, opportunity_id)

    eligible, reason = _is_eligible(opportunity)
    if not eligible:
        cleanup_result = cleanup_ineligible_opportunity(client, opportunity_id, reason, state)
        return {"status": "skipped", "reason": reason, **cleanup_result}

    services = fetch_service_items(client, opportunity_id)
    shifts_state: dict[str, Any] = state["shifts"]

    # A service line item that's tracked (we made a shift for it before) but
    # no longer shows up here — deleted from the opportunity, or had its
    # dates cleared — is torn down the same way a whole ineligible
    # opportunity is: draft deleted, published left + Chat notification.
    current_keys = {str(service["id"]) for service in services}
    orphaned_keys = [
        k
        for k, v in shifts_state.items()
        if v.get("opportunityId") == opportunity_id and k not in current_keys
    ]
    orphan_result = (
        _cleanup_shift_keys(
            client, orphaned_keys, shifts_state, opportunity_id, "service item removed from opportunity"
        )
        if orphaned_keys
        else None
    )

    if not services:
        result: dict[str, Any] = {"status": "skipped", "reason": "no dated Service line items found"}
        if orphan_result:
            result["orphaned_items_cleaned_up"] = orphan_result
        return result

    address = fetch_venue_address(client, opportunity)
    order_number = opportunity.get("number")
    job_title_cache: dict[str, Any] = state["job_title_cache"]

    subject = opportunity.get("subject") or f"Order {opportunity.get('number', opportunity['id'])}"
    if "#" in subject:
        subject = subject.split("#", 1)[1].strip()

    to_create: list[tuple[str, dict[str, Any], dict[str, Any]]] = []  # (key, payload, desired)
    to_update: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    unchanged = 0
    skipped: list[tuple[str, str]] = []
    # Service name per shift key, kept so a stale-Job-ID retry (see
    # _is_stale_job_id_error below) can redo the title lookup after a forced
    # cache refresh without re-walking the Current RMS service items.
    service_name_by_key: dict[str, str] = {}

    for service in services:
        start = _to_epoch_seconds(service["starts_at"])
        end = _to_epoch_seconds(service["ends_at"])

        if end <= start:
            skipped.append((service["name"], "end time is not after start time"))
            continue
        if end - start > MAX_SHIFT_SECONDS:
            skipped.append((service["name"], "duration exceeds Connecteam's 24h shift limit"))
            continue

        # Job = task type, matched by Service name (e.g. "Lighting
        # Technician") — never created per-order. The opportunity name only
        # ever goes into the shift title text, per the corrected design.
        job_id = find_job_by_service_name(client, service["name"], job_title_cache)
        job_color = get_job_color(job_id, job_title_cache)

        # Shift title is just the opportunity name — the service/task type
        # is already conveyed by the Job dropdown, so it doesn't need to be
        # repeated in the title text.
        title = subject

        # Current RMS line items with quantity > 1 (e.g. "4 x Lighting
        # Technician") become ONE draft shift, with that number written into
        # the "Quantity Required" shift custom field — Connecteam's public
        # API has no working way to make a single shift claimable by
        # multiple people (see CONNECTEAM_QTY_CUSTOM_FIELD_ID comment
        # above), so admins read this field and manually assign that many
        # people to the shift. Current RMS returns quantity as a decimal
        # string (e.g. "4.0"), so int() directly would raise — go through
        # float() first.
        quantity = service.get("quantity") or 1
        try:
            quantity = max(1, int(float(quantity)))
        except (TypeError, ValueError):
            quantity = 1

        # Current RMS's free-text description on the Service line item (e.g.
        # "Bump in Day 1") — confirmed live via /debug/inspect-service-items
        # against real service items. Written verbatim into the "Shift
        # Type/Notes" custom field (not the shift notes field) so crew see
        # what the line item is actually for.
        description = (service.get("description") or "").strip()

        key = str(service["id"])
        service_name_by_key[key] = service["name"]
        desired = {
            "opportunityId": opportunity_id,
            "startTime": start,
            "endTime": end,
            "title": title,
            "jobId": job_id,
            "color": job_color,
            "orderNumber": order_number,
            "address": address,
            "quantity": quantity,
            "description": description,
        }

        custom_fields = []
        if order_number:
            custom_fields.append(
                {"customFieldId": CONNECTEAM_JOBNO_CUSTOM_FIELD_ID, "value": str(order_number)}
            )
        custom_fields.append(
            {"customFieldId": CONNECTEAM_QTY_CUSTOM_FIELD_ID, "value": str(quantity)}
        )
        custom_fields.append(
            {"customFieldId": CONNECTEAM_SHIFT_TYPE_CUSTOM_FIELD_ID, "value": description}
        )

        notes = [
            {
                "html": (
                    f"<p>Auto-created from Current RMS order "
                    f"{opportunity.get('number', opportunity['id'])} "
                    f"(opportunity item #{service['id']}).</p>"
                )
            }
        ]

        existing = shifts_state.get(key)
        if existing is None:
            payload: dict[str, Any] = {
                "startTime": start,
                "endTime": end,
                "title": title,
                "isPublished": False,
                "notes": notes,
                "customFields": custom_fields,
            }
            if job_id:
                payload["jobId"] = job_id
            if job_color:
                payload["color"] = job_color
            if address:
                payload["locationData"] = {"isReferencedToJob": False, "gps": {"address": address}}
            to_create.append((key, payload, desired))
        elif (
            existing.get("startTime") != start
            or existing.get("endTime") != end
            or existing.get("title") != title
            or existing.get("jobId") != job_id
            or existing.get("color") != job_color
            or existing.get("orderNumber") != order_number
            or existing.get("address") != address
            or existing.get("quantity") != quantity
            or existing.get("description") != description
        ):
            update_payload: dict[str, Any] = {
                "shiftId": existing["shiftId"],
                "startTime": start,
                "endTime": end,
                "title": title,
                "notes": notes,
                "customFields": custom_fields,
            }
            if job_id:
                update_payload["jobId"] = job_id
            if job_color:
                update_payload["color"] = job_color
            if address:
                update_payload["locationData"] = {"isReferencedToJob": False, "gps": {"address": address}}
            to_update.append((key, update_payload, desired))
        else:
            unchanged += 1

    if skipped:
        for name, reason in skipped:
            log.warning("Skipped service '%s': %s", name, reason)

    def _force_refresh_job_cache() -> None:
        by_title, by_jobid_color = _load_job_title_cache(client)
        cache = job_title_cache.setdefault("by_title", {})
        color_cache = job_title_cache.setdefault("by_jobid_color", {})
        cache.clear()
        cache.update(by_title)
        color_cache.clear()
        color_cache.update(by_jobid_color)

    def _rebuild_job_fields(payload: dict[str, Any], desired: dict[str, Any], key: str) -> None:
        prefix = CONNECTEAM_JOB_PREFIX.strip()
        service_name = service_name_by_key[key]
        mapped_name = SERVICE_JOB_OVERRIDES.get(service_name, service_name)
        title = f"{prefix} {mapped_name}" if prefix else mapped_name
        job_id = job_title_cache.get("by_title", {}).get(title)
        job_color = job_id and job_title_cache.get("by_jobid_color", {}).get(job_id)
        payload.pop("jobId", None)
        payload.pop("color", None)
        if job_id:
            payload["jobId"] = job_id
        if job_color:
            payload["color"] = job_color
        desired["jobId"] = job_id
        desired["color"] = job_color or None

    try:
        created_shifts = create_shifts(client, [p for _, p, _ in to_create])
    except httpx.HTTPStatusError as exc:
        if not _is_stale_job_id_error(exc):
            raise
        log.warning(
            "Connecteam rejected shift creation for opportunity %s due to a stale cached Job ID — "
            "forcing a Job cache refresh and retrying once",
            opportunity_id,
        )
        _force_refresh_job_cache()
        for key, payload, desired in to_create:
            _rebuild_job_fields(payload, desired, key)
        created_shifts = create_shifts(client, [p for _, p, _ in to_create])

    if len(created_shifts) != len(to_create):
        log.warning(
            "Created %d shifts but requested %d for opportunity %s — response ordering assumption may be wrong",
            len(created_shifts), len(to_create), opportunity_id,
        )
    for (key, _, desired), shift_obj in zip(to_create, created_shifts):
        shifts_state[key] = {"shiftId": shift_obj["id"], **desired}

    try:
        updated_shifts = update_shifts(client, [p for _, p, _ in to_update])
    except httpx.HTTPStatusError as exc:
        if not _is_stale_job_id_error(exc):
            raise
        log.warning(
            "Connecteam rejected shift update for opportunity %s due to a stale cached Job ID — "
            "forcing a Job cache refresh and retrying once",
            opportunity_id,
        )
        _force_refresh_job_cache()
        for key, payload, desired in to_update:
            _rebuild_job_fields(payload, desired, key)
        updated_shifts = update_shifts(client, [p for _, p, _ in to_update])

    # Snapshot pre-update state before it's overwritten below, so the
    # published-edit notification can describe exactly what changed.
    previous_by_key = {key: dict(shifts_state.get(key, {})) for key, _, _ in to_update}

    for key, _, desired in to_update:
        existing = shifts_state.get(key, {})
        existing.update(desired)
        shifts_state[key] = existing

    # A shift someone already published can still be in to_update (nothing
    # here checks publish state before editing — Current RMS changes are
    # still pushed through). Ops gets a heads-up, with specifics, so crew
    # who were told about the original time/details aren't caught out by a
    # silent change.
    published_edits = 0
    for (key, _, desired), shift_obj in zip(to_update, updated_shifts):
        if shift_obj.get("isPublished"):
            published_edits += 1
            previous = previous_by_key.get(key, {})
            changes: list[str] = []
            if previous.get("startTime") != desired.get("startTime") or previous.get("endTime") != desired.get(
                "endTime"
            ):
                changes.append(
                    f"Time: {_format_epoch(previous['startTime'])} – {_format_epoch(previous['endTime'])} "
                    f"→ {_format_epoch(desired['startTime'])} – {_format_epoch(desired['endTime'])}"
                )
            if previous.get("title") != desired.get("title"):
                changes.append(f'Title: "{previous.get("title")}" → "{desired.get("title")}"')
            if previous.get("quantity") != desired.get("quantity"):
                changes.append(f"Quantity required: {previous.get('quantity')} → {desired.get('quantity')}")
            if previous.get("address") != desired.get("address"):
                changes.append(f"Address: {previous.get('address')} → {desired.get('address')}")
            if previous.get("description") != desired.get("description"):
                changes.append(f'Notes: "{previous.get("description")}" → "{desired.get("description")}"')
            change_text = "\n".join(f"  • {c}" for c in changes) if changes else "  (no tracked-field change detected)"
            notify_ops(
                client,
                f"✏️ Published Connecteam shift edited by sync.\n"
                f"Opportunity {opportunity_id} (order {order_number}) — shift \"{desired.get('title')}\" "
                f"({shift_obj['id']}):\n{change_text}\n"
                f"Please confirm crew are aware of the change.",
            )

    result = {
        "status": "ok",
        "created_count": len(created_shifts),
        "updated_count": len(updated_shifts),
        "unchanged_count": unchanged,
        "skipped_count": len(skipped),
        "published_edits_count": published_edits,
    }
    if orphan_result:
        result["orphaned_items_cleaned_up"] = orphan_result
    return result


def poll_all_open_orders() -> dict[str, Any]:
    """Background/manual poll: sync every Order- or Quotation-state
    opportunity updated since the last poll, so Service-item edits made
    after conversion, and flag-driven Quotation eligibility changes (ticked,
    unticked, gone dead/lost), are picked up even outside the instant
    webhook paths."""
    with sync_lock:
        state = _load_state()
        poll_start = datetime.now(timezone.utc)
        cursor = state.get("poll_cursor")
        since = cursor or (poll_start - timedelta(hours=POLL_INITIAL_LOOKBACK_HOURS)).isoformat()

        results: dict[str, Any] = {}
        with httpx.Client(timeout=30) as client:
            opportunity_ids = fetch_opportunities_updated_since(client, since)
            for opportunity_id in opportunity_ids:
                try:
                    results[str(opportunity_id)] = sync_opportunity(client, opportunity_id, state)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        # Opportunity was deleted and the poll (not the webhook) is
                        # what found out — clean up the same way the webhook's
                        # Action_type=="destroy" path does, instead of leaving
                        # orphaned shifts to 404 again every subsequent poll.
                        log.warning(
                            "Opportunity %s 404'd during poll — treating as deleted and cleaning up",
                            opportunity_id,
                        )
                        cleanup_result = cleanup_ineligible_opportunity(
                            client, opportunity_id, "deleted (404 on poll)", state
                        )
                        results[str(opportunity_id)] = {"status": "skipped", "reason": "deleted", **cleanup_result}
                    else:
                        log.exception("Poll failed for opportunity %s", opportunity_id)
                        results[str(opportunity_id)] = {
                            "status": "error",
                            "detail": f"{exc.response.status_code} {exc.response.text[:300]}",
                        }

        state["poll_cursor"] = (poll_start - timedelta(minutes=POLL_OVERLAP_MINUTES)).isoformat()
        _save_state(state)

    log.info("Poll checked %d opportunit(y/ies): %s", len(results), results)
    return {"checked": len(results), "since": since, "results": results}


def _scheduler_loop() -> None:
    # Give the app a moment to finish starting before the first poll.
    time.sleep(10)
    while True:
        try:
            poll_all_open_orders()
        except Exception:
            log.exception("Scheduled poll crashed")
        time.sleep(POLL_INTERVAL_SECONDS)


@app.on_event("startup")
def start_scheduler() -> None:
    if ENABLE_SCHEDULER:
        threading.Thread(target=_scheduler_loop, daemon=True).start()
        log.info("Background poll scheduler started (every %ds)", POLL_INTERVAL_SECONDS)
    else:
        log.info("Background poll scheduler disabled (ENABLE_SCHEDULER=false)")


# ---------------------------------------------------------------------------
# Webhook endpoint (instant trigger on conversion)
# ---------------------------------------------------------------------------

@app.post("/webhooks/current-rms/opportunity-converted")
async def opportunity_converted(request: Request, token: str | None = None):
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    payload = await request.json()
    action = payload.get("action", {})

    if action.get("subject_type") != "Opportunity":
        return JSONResponse({"status": "ignored", "reason": "not an Opportunity action"})

    opportunity_id = action.get("subject_id")
    if opportunity_id is None:
        raise HTTPException(status_code=400, detail="action.subject_id missing")

    def _run() -> dict[str, Any]:
        with sync_lock:
            state = _load_state()
            with httpx.Client(timeout=30) as client:
                if action.get("Action_type") == "destroy":                # Deleted opportunity: fetch_opportunity would 404, so clean up tracked shifts directly.
                    result = cleanup_ineligible_opportunity(client, opportunity_id, "deleted", state)
                    result = {"status": "skipped", "reason": "deleted", **result}
                else:
                    result = sync_opportunity(client, opportunity_id, state)
            _save_state(state)
            return result

    try:
        result = await asyncio.to_thread(_run)
    except httpx.HTTPStatusError as exc:
        log.exception("Upstream API error while processing opportunity %s", opportunity_id)
        raise HTTPException(
            status_code=502,
            detail=f"upstream error: {exc.response.status_code} {exc.response.text[:500]}",
        ) from exc

    log.info("Webhook processed opportunity %s: %s", opportunity_id, result)
    return JSONResponse({"opportunity_id": opportunity_id, **result})


# ---------------------------------------------------------------------------
# Manual/scheduled sync trigger + health check
# ---------------------------------------------------------------------------

@app.get("/sync")
async def manual_sync(token: str | None = None):
    """Trigger a poll on demand (also runs automatically every
    POLL_INTERVAL_SECONDS). Protected by WEBHOOK_TOKEN."""
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")
    result = await asyncio.to_thread(poll_all_open_orders)
    return JSONResponse(result)


@app.get("/debug/inspect-sync")
async def debug_inspect_sync(opportunity_id: int, token: str | None = None):
    """TEMPORARY, diagnostic only, no writes. User reports some shifts exist
    in Connecteam but the "Shift Type/Notes" custom field is blank even
    after a successful backfill. For each dated Service item on this
    opportunity, returns the raw Current RMS description, the tracked state
    entry (shiftId + last-synced description), and the LIVE Connecteam
    shift's actual customFields — so we can tell apart "source description
    is genuinely blank" from "we computed a value but never sent/received
    it". Protected by BACKFILL_TOKEN (reused — diagnostic, not a write).
    Remove this route once the mismatch is understood."""
    if not BACKFILL_TOKEN or not hmac.compare_digest(token or "", BACKFILL_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        state = _load_state()
        shifts_state: dict[str, Any] = state["shifts"]
        with httpx.Client(timeout=30) as client:
            services = fetch_service_items(client, opportunity_id)
            rows = []
            for service in services:
                key = str(service["id"])
                existing = shifts_state.get(key)
                row: dict[str, Any] = {
                    "service_item_id": service["id"],
                    "service_name": service["name"],
                    "raw_description": service.get("description"),
                    "tracked_state": existing,
                }
                if existing and existing.get("shiftId"):
                    shift = get_shift(client, existing["shiftId"])
                    row["live_shift_customFields"] = (
                        shift.get("customFields") if shift else "SHIFT NOT FOUND (404)"
                    )
                rows.append(row)
            return {"opportunity_id": opportunity_id, "services": rows}

    result = await asyncio.to_thread(_run)
    return JSONResponse(result)


@app.get("/backfill")
async def backfill(token: str | None = None, ids: str | None = None):
    """TEMPORARY, one-off. Resyncs every current Order/Quotation-state
    opportunity regardless of when it was last updated in Current RMS, so
    already-existing shifts pick up mapping changes (e.g. the new "Shift
    Type/Notes" custom field) without waiting for someone to edit that
    order. Unlike /sync, this does NOT read or advance the poll cursor, so
    the normal background poll's incremental window is unaffected either
    way. Protected by its own BACKFILL_TOKEN (separate from WEBHOOK_TOKEN).
    Pass ?ids=3108,3898 to scope the run to specific opportunity IDs (e.g.
    retrying only the ones that failed a prior full run) instead of
    scanning every open opportunity. Remove this route once the backfill
    has been run and confirmed."""
    if not BACKFILL_TOKEN or not hmac.compare_digest(token or "", BACKFILL_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        with sync_lock:
            state = _load_state()
            results: dict[str, Any] = {}
            with httpx.Client(timeout=30) as client:
                if ids:
                    opportunity_ids = [int(x.strip()) for x in ids.split(",") if x.strip()]
                else:
                    # Deliberately far back so every current Order/Quotation
                    # opportunity is included, not just ones changed recently.
                    opportunity_ids = fetch_opportunities_updated_since(client, "2000-01-01T00:00:00Z")
                for opportunity_id in opportunity_ids:
                    try:
                        results[str(opportunity_id)] = sync_opportunity(client, opportunity_id, state)
                    except httpx.HTTPStatusError as exc:
                        log.exception("Backfill failed for opportunity %s", opportunity_id)
                        results[str(opportunity_id)] = {
                            "status": "error",
                            "detail": f"{exc.response.status_code} {exc.response.text[:300]}",
                        }
            _save_state(state)
        return {"checked": len(results), "results": results}

    result = await asyncio.to_thread(_run)
    log.info("Backfill checked %d opportunit(y/ies)", result["checked"])
    return JSONResponse(result)


@app.post("/debug/create-jobs")
async def debug_create_jobs(titles: str, token: str | None = None):
    """TEMPORARY, one-off. Creates a Connecteam Job (task type) for each
    comma-separated title in `titles`, in whichever scheduler
    CONNECTEAM_SCHEDULER_ID currently points at. Skips any title that
    already has a matching Job (idempotent-ish safety net) — never
    duplicates, never renames/deletes an existing Job. Protected by
    BACKFILL_TOKEN (reused — one-off admin action, not part of normal
    sync). Remove this route once the service<->Job mapping cleanup is
    done."""
    if not BACKFILL_TOKEN or not hmac.compare_digest(token or "", BACKFILL_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    wanted = [t.strip() for t in titles.split(",") if t.strip()]

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY, "Content-Type": "application/json"}
        with httpx.Client(timeout=30) as client:
            existing_titles, _ = _load_job_title_cache(client)
            created: list[str] = []
            skipped: list[str] = []
            errors: list[dict[str, Any]] = []
            for title in wanted:
                if title in existing_titles:
                    skipped.append(title)
                    continue
                resp = client.post(
                    f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
                    headers=headers,
                    json=[
                        {
                            "instanceIds": [int(CONNECTEAM_SCHEDULER_ID)],
                            "title": title,
                            "assign": {"type": "both", "userIds": [], "groupIds": []},
                        }
                    ],
                )
                if resp.status_code >= 400:
                    errors.append({"title": title, "status": resp.status_code, "body": resp.text[:300]})
                    continue
                created.append(title)
        return {
            "scheduler_id": CONNECTEAM_SCHEDULER_ID,
            "created": created,
            "skipped_already_existed": skipped,
            "errors": errors,
        }

    result = await asyncio.to_thread(_run)
    return JSONResponse(result)


@app.get("/debug/sweep-mismatched-jobs")
async def debug_sweep_mismatched_jobs(token: str | None = None, apply: bool = False):
    """TEMPORARY, one-off. Finds every tracked shift whose Connecteam Job no
    longer matches what SERVICE_JOB_OVERRIDES says it should be — chiefly
    shifts created before that mapping existed, which got no Job at all, or
    the wrong one. For each: refetches the opportunity's current service
    items (shifts_state doesn't store the RMS service name, only the
    opportunity subject) to recompute the expected jobId, and compares it
    against the shift's LIVE jobId in Connecteam.

    Dry-run by default (apply=false): reports mismatches, deletes nothing.
    Pass apply=true to actually delete — ONLY drafts are ever deleted;
    published shifts are always left in place and reported separately (with
    a Google Chat notification), never touched. A deleted shift's state
    entry is also dropped so the next normal sync recreates it fresh with
    the correct Job. Protected by BACKFILL_TOKEN. Remove this route once
    the sweep has been run and confirmed."""
    if not BACKFILL_TOKEN or not hmac.compare_digest(token or "", BACKFILL_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        with sync_lock:
            state = _load_state()
            shifts_state: dict[str, Any] = state["shifts"]
            job_title_cache: dict[str, Any] = state["job_title_cache"]
            opportunity_ids = sorted({v["opportunityId"] for v in shifts_state.values()})

            mismatches: list[dict[str, Any]] = []
            deleted: list[dict[str, Any]] = []
            left_published: list[dict[str, Any]] = []
            already_gone = 0
            checked = 0

            with httpx.Client(timeout=30) as client:
                for opportunity_id in opportunity_ids:
                    try:
                        services = fetch_service_items(client, opportunity_id)
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            continue  # deleted opportunity — handled by the normal 404 cleanup paths, not this sweep
                        raise
                    service_name_by_key = {str(s["id"]): s["name"] for s in services}

                    keys = [k for k, v in shifts_state.items() if v.get("opportunityId") == opportunity_id]
                    for key in keys:
                        service_name = service_name_by_key.get(key)
                        if service_name is None:
                            continue  # item no longer on the opportunity — the orphan-cleanup path handles this, not this sweep
                        checked += 1

                        expected_job_id = find_job_by_service_name(client, service_name, job_title_cache)
                        entry = shifts_state[key]
                        shift_id = entry["shiftId"]
                        shift = get_shift(client, shift_id)
                        if shift is None:
                            already_gone += 1
                            del shifts_state[key]
                            continue

                        actual_job_id = shift.get("jobId")
                        if actual_job_id == expected_job_id:
                            continue

                        row = {
                            "opportunity_id": opportunity_id,
                            "shift_id": shift_id,
                            "service_name": service_name,
                            "shift_title": shift.get("title"),
                            "actual_job_id": actual_job_id,
                            "expected_job_id": expected_job_id,
                            "is_published": bool(shift.get("isPublished")),
                        }
                        mismatches.append(row)

                        if shift.get("isPublished"):
                            left_published.append(row)
                            if apply:
                                notify_ops(
                                    client,
                                    f"⚠️ Published Connecteam shift has the wrong Job and needs manual "
                                    f"review.\nOpportunity {opportunity_id} — shift \"{shift.get('title')}\" "
                                    f"({shift_id}), service \"{service_name}\": Job mismatch found during the "
                                    f"service-mapping sweep. Left in place (published) — please reassign its "
                                    f"Job manually in Connecteam.",
                                )
                            continue

                        if apply:
                            delete_shift(client, shift_id)
                            del shifts_state[key]
                            deleted.append(row)

            if apply:
                _save_state(state)

        return {
            "applied": apply,
            "opportunities_checked": len(opportunity_ids),
            "shifts_checked": checked,
            "mismatches_found": len(mismatches),
            "mismatches": mismatches,
            "deleted_draft_count": len(deleted),
            "left_published_count": len(left_published),
            "already_gone_count": already_gone,
        }

    result = await asyncio.to_thread(_run)
    return JSONResponse(result)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "time": int(time.time())}
