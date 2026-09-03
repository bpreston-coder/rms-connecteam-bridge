"""
Current RMS -> Connecteam draft shift bridge.

An opportunity is eligible for draft shifts when it's in "Order" state
(always), or in "Quotation" state with the "Draft shifts in Connecteam"
Yes/No custom field ticked — UNLESS it's been marked dead or lost, which
Current RMS represents as a status_name change ("Dead"/"Lost"), not a state
change or flag change, so it's checked first regardless of state/flag (see
DEAD_OR_LOST_STATUS_NAMES). Two paths keep Connecteam in sync:

  1. Webhooks fire instantly on opportunity_convert_to_order, opportunity_
     update, opportunity_convert_to_quotation, opportunity_revert_to_
     quotation, opportunity_mark_as_dead, and opportunity_mark_as_lost.
  2. A background poll (every POLL_INTERVAL_SECONDS, default 15 minutes)
     scans every Order- or Quotation-state opportunity updated since the
     last poll, so Service-item edits and eligibility changes are always
     eventually picked up even if a webhook is missed.

Both paths funnel into the same idempotent sync routine, keyed off each
Current RMS opportunity_item's ID (plus a day-index for a split multi-day
item — see below):
  - First time we see an item on an eligible opportunity -> CREATE a draft
    Connecteam shift for it.
  - If we've already created a shift for that item -> UPDATE that same
    shift in place if the title/time/job/quantity/site contact changed,
    otherwise do nothing.
  - If the opportunity is no longer eligible (flag unticked, went dead/
    lost/reverted, or was deleted outright and caught by the webhook's
    Action_type=="destroy" path) -> DELETE every shift we created for it,
    draft or already-published alike.
  - Same teardown applies at the individual item level: if one service
    line item is removed from an opportunity that's still otherwise
    eligible (or excluded — see EXCLUDED_SERVICE_NAMES), only that item's
    shift is deleted.
  - A Service line item spanning more than 24h becomes one shift PER
    Australia/Perth calendar day it touches (see
    _split_into_daily_segments), each independently created/updated/
    deleted by opportunity_item-id + day-index. Beyond MAX_SHIFT_SPAN_DAYS
    it's skipped instead. A "Day Rate" service gets full 00:00->24:00
    Perth segments regardless of its literal time-of-day.
  - A finalised/completed service line item (status_name == "Completed")
    is left completely untouched — no create, update, delete, or
    notification (see FINALISED_STATUS_NAME).
  - Whenever a published shift is deleted, or gets edited in place because
    Current RMS changed under it, ops is notified in Google Chat (see
    notify_ops / GOOGLE_CHAT_WEBHOOK_URL) so a human knows a shift crew
    may already have been told about was just removed or changed — after
    the fact, since by the time the message lands the action has already
    happened.
This guarantees we never create duplicate shifts, no matter how many times
an opportunity is processed (webhook retries, overlapping polls, re-
conversions).

It also finds a Connecteam Job per Service line item (matched by service
name via SERVICE_JOB_OVERRIDES when the names don't line up 1:1, never
created here), writes the Current RMS order number into the shift's
"Job No." custom field, and the required headcount into "Qty Rqrd".

Each shift's notes include a "Site contact" line — the opportunity's
"on-site contact if different from project contact" custom field when set,
otherwise the opportunity owner's name.

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

# Incoming webhook URL for the ops Google Chat space. Posted to whenever a
# shift a human already published gets left in place during cleanup or
# edited by the sync — both cases where a real person may have already told
# crew about that shift. Unset by default: notify_ops() just logs and
# no-ops rather than failing sync, so this is safe to leave unconfigured in
# test.
GOOGLE_CHAT_WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "")

# Perth (AWST) is fixed UTC+8 year-round — no DST to account for. Used only
# to render times in ops-facing Chat notifications in local time rather
# than UTC.
PERTH_TZ = timezone(timedelta(hours=8))

# Where we remember: (a) which Connecteam shift belongs to which Current RMS
# opportunity_item (so we UPDATE instead of duplicating), and (b) how far
# back the last poll checked, so the next poll only looks at what changed.
STATE_FILE = Path(os.environ.get("STATE_FILE", "./processed_orders.json"))

MAX_SHIFT_SECONDS = 24 * 60 * 60  # Connecteam: a shift can't exceed 24h.

# A Service line item spanning more than one calendar day becomes one shift
# PER Australia/Perth calendar day it touches (see _split_into_daily_segments)
# instead of one shift skipped outright for exceeding MAX_SHIFT_SECONDS.
# Beyond MAX_SHIFT_SPAN_DAYS calendar days, a service goes back to being
# skipped (with a warning) rather than split — guards against turning a
# genuinely long equipment-only booking (weeks or months) into that many
# daily shifts. Confirmed with the user 2026-08-17.
MAX_SHIFT_SPAN_DAYS = 14

# Some Current RMS Service names don't map 1:1 onto a Connecteam Job — either
# because several distinct RMS services (daytime/overnight variants, or
# per-city/per-distance-type variants) are meant to share ONE task-type Job,
# or because the Job title in Connecteam has just diverged from the RMS
# service name over time. Reviewed and confirmed with the user against the
# live Current RMS Service catalog and Connecteam Jobs list. Keys are the
# exact Current RMS service["name"]; values are the exact Connecteam Job
# title to look up instead. Services not listed here use their own name
# unchanged (the historical 1:1 behavior).
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

# These Service line items are cost/allowance entries, not an actual crew
# shift someone needs to turn up for — never synced to Connecteam. Matched
# on the Current RMS service["name"] exactly. An opportunity with only
# excluded services still gets its other (non-excluded) items synced
# normally; if one of these was already tracked from before this exclusion
# existed, it's cleaned up the same way any other item removed from the
# opportunity would be, since fetch_service_items simply stops returning it.
EXCLUDED_SERVICE_NAMES = {"Per diem", "Accomodation - Per night"}

# A Service line item's status_name once someone finalises/completes it in
# Current RMS (after which Current RMS itself won't let it be edited
# further). Once finalised, the bridge leaves its shift completely alone —
# no create, no update, no delete, no notification.
FINALISED_STATUS_NAME = "Completed"

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


#  Current RMS represents "marked as dead/lost" as a `status`/`status_name`
#  change (e.g. status_name == "Dead"), NOT a `state` change — an opportunity
#  marked dead stays in whatever state it was in (state_name still
#  "Quotation" or "Order"), and any "Draft shifts in Connecteam" flag is left
#  as-is too. Confirmed live against opportunity 4056 (marked dead 2026-09-
#  02): state_name stayed "Quotation", draft_shifts_in_connecteams stayed
#  "Yes", only status_name changed to "Dead" — so eligibility must check
#  status_name first or a dead/lost opportunity is wrongly treated as still
#  eligible and its shifts are never cleaned up.
DEAD_OR_LOST_STATUS_NAMES = {"Dead", "Lost"}


def _is_eligible(opportunity: dict[str, Any]) -> tuple[bool, str]:
    """An opportunity marked dead or lost (status_name, not state_name) is
    never eligible, regardless of state or flag. Otherwise: Order state is
    always eligible (existing behavior). Quotation state is eligible only
    when the "Draft shifts in Connecteam" Yes/No custom field is ticked —
    Current RMS returns this as custom_fields.draft_shifts_in_connecteams ==
    "Yes" (exact string, confirmed live). Everything else (Enquiry, Draft,
    etc.) is not eligible."""
    status_name = opportunity.get("status_name")
    if status_name in DEAD_OR_LOST_STATUS_NAMES:
        return False, f"status '{status_name}'"

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


def fetch_member_name(client: httpx.Client, member_id: int | None) -> str | None:
    """Return a Member's display name — used to resolve an opportunity's
    owner (opportunity["owned_by"] is a member_id) to a human name for the
    site-contact default. None if member_id is falsy or not found."""
    if not member_id:
        return None
    resp = client.get(
        f"{CURRENT_RMS_BASE_URL}/api/v1/members/{member_id}",
        headers=rms_headers(),
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["member"].get("name")


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
        if item.get("item_type") == "Service"
        and item.get("starts_at")
        and item.get("ends_at")
        and item.get("name") not in EXCLUDED_SERVICE_NAMES
    ]


# ---------------------------------------------------------------------------
# Connecteam client
# ---------------------------------------------------------------------------

def _to_epoch_seconds(iso_ts: str) -> int:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    return int(dt.astimezone(timezone.utc).timestamp())


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


def notify_ops(client: httpx.Client, text: str) -> None:
    """Post a message to the ops Google Chat space, as a Card TextParagraph
    rather than a plain text message — Chat's plain `text` field supports
    no color, only a card's TextParagraph does
    (<font color="#RRGGBB">...</font>, confirmed via Google's card
    text-formatting docs). `text` may use \\n for line breaks (converted to
    <br> here) and inline HTML tags — see _orange()/_green() for the
    colored Original/Updated diff wording. Best-effort: logs and swallows
    failures instead of raising, so a Chat outage never breaks a sync that
    otherwise succeeded."""
    if not GOOGLE_CHAT_WEBHOOK_URL:
        log.warning("GOOGLE_CHAT_WEBHOOK_URL not set — skipping ops notification: %s", text)
        return
    payload = {
        "cardsV2": [
            {
                "cardId": "bridge-notification",
                "card": {
                    "sections": [{"widgets": [{"textParagraph": {"text": text.replace("\n", "<br>")}}]}]
                },
            }
        ]
    }
    try:
        resp = client.post(GOOGLE_CHAT_WEBHOOK_URL, json=payload)
        resp.raise_for_status()
    except httpx.HTTPError:
        log.exception("Failed to post ops notification to Google Chat: %s", text)


def _html_escape(value: Any) -> str:
    """Escape a value for safe inclusion inside a Card TextParagraph's
    HTML — used only for the free-text values wrapped in a colored <font>
    span by _orange()/_green(), since those can contain arbitrary
    Current RMS-entered text (notes, addresses, etc)."""
    text = "" if value is None else str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# Colors for the "Original: X, Updated to: Y" wording in the
# published-shift-edited Chat notification — orange for what it was, green
# for what it's now. Confirmed with the user 2026-08-27.
_ORIGINAL_COLOR = "#E69138"
_UPDATED_COLOR = "#38761D"


def _orange(value: Any) -> str:
    return f'<font color="{_ORIGINAL_COLOR}">{_html_escape(value)}</font>'


def _green(value: Any) -> str:
    return f'<font color="{_UPDATED_COLOR}">{_html_escape(value)}</font>'


def _format_epoch(ts: int) -> str:
    """Used in ops-facing Chat notifications only — local (Perth) time, not
    UTC, since that's what the times actually mean to crew reading the
    message."""
    return datetime.fromtimestamp(ts, tz=PERTH_TZ).strftime("%a %d %b %Y, %H:%M AWST")


def _split_into_daily_segments(
    start: int, end: int, full_day: bool = False
) -> list[tuple[int, int]] | None:
    """Return the Connecteam shift segment(s) for [start, end) (unix epoch
    seconds, end already confirmed > start by the caller).

    Connecteam's actual constraint is a 24-HOUR DURATION limit, not a
    calendar-day one — a single shift is free to cross midnight (e.g. 8pm
    -> 2am, 6h) so that stays exactly ONE segment with its literal times,
    not two pieces cut at the midnight boundary. Only once the span
    genuinely exceeds MAX_SHIFT_SECONDS does it get split into one segment
    per Australia/Perth calendar day it touches — e.g. Fri 22:00 -> Sun
    06:00 AWST (32h) becomes [Fri 22:00->midnight, Sat 00:00->24:00, Sun
    00:00->06:00]. Returns None if that split would cover more than
    MAX_SHIFT_SPAN_DAYS calendar days — caller should skip instead of
    splitting into that many shifts.

    full_day=True (Current RMS "Day Rate" services) makes every returned
    segment the FULL Perth calendar day (00:00->24:00) — anchored to
    start's day when duration <=24h — ignoring the actual time-of-day in
    start/end. This is how a day-rate service is represented on the
    Connecteam side, since Connecteam's own "all day" shift flag is
    accepted by its public API but silently has no effect (confirmed via
    direct testing: create + read-back showed no trace of the field at
    all, same failure mode as the documented isOpenShift/numOfUsers
    multi-slot limitation)."""
    start_dt = datetime.fromtimestamp(start, tz=PERTH_TZ)
    start_day = start_dt.date()

    if end - start <= MAX_SHIFT_SECONDS:
        if full_day:
            day_start = datetime(start_day.year, start_day.month, start_day.day, tzinfo=PERTH_TZ)
            return [(int(day_start.timestamp()), int((day_start + timedelta(days=1)).timestamp()))]
        return [(start, end)]

    end_dt = datetime.fromtimestamp(end, tz=PERTH_TZ)
    end_day = end_dt.date()
    if (end_day - start_day).days + 1 > MAX_SHIFT_SPAN_DAYS:
        return None

    segments: list[tuple[int, int]] = []
    cursor = start
    day = start_day
    while day <= end_day:
        day_start = datetime(day.year, day.month, day.day, tzinfo=PERTH_TZ)
        next_midnight = day_start + timedelta(days=1)
        if full_day:
            segments.append((int(day_start.timestamp()), int(next_midnight.timestamp())))
        else:
            day_end = min(end, int(next_midnight.timestamp()))
            if day_end > cursor:
                segments.append((cursor, day_end))
            cursor = day_end
        day += timedelta(days=1)
    return segments


def _expand_service_segments(
    services: list[dict[str, Any]],
) -> tuple[list[tuple[str, dict[str, Any], int, int]], list[tuple[str, str]], set[str]]:
    """Turn each dated Service line item into one or more (key, service,
    start, end) rows — one per Australia/Perth calendar day it spans (see
    _split_into_daily_segments). A same-day item keeps its plain
    str(service["id"]) key, unchanged from before day-splitting existed, so
    already-tracked single-day shifts are never mistaken for orphans or
    recreated under a new key; a multi-day item's per-day rows get
    "{id}:{day_index}" keys instead. A "Day Rate" service (Current RMS
    service_rate_type_name == "Day") gets full 00:00->24:00 Perth segments
    regardless of its literal starts_at/ends_at time-of-day.

    A FINALISED_STATUS_NAME service (status_name == "Completed" — set once
    someone finalises/completes it, after which Current RMS itself won't
    let it be edited further) is deliberately left OUT of `expanded`, so
    nothing about its shift is ever created/updated. Its key(s) still go
    into `protected_keys` though, so the caller's orphan detection treats
    it as still present rather than removed — the whole point is that
    nothing happens to it either way, not that its existing shift gets torn
    down.

    Returns (expanded, skipped, protected_keys) — skipped covers items that
    can't be scheduled at all (bad times, or a span longer than
    MAX_SHIFT_SPAN_DAYS calendar days)."""
    expanded: list[tuple[str, dict[str, Any], int, int]] = []
    skipped: list[tuple[str, str]] = []
    protected_keys: set[str] = set()
    for service in services:
        start = _to_epoch_seconds(service["starts_at"])
        end = _to_epoch_seconds(service["ends_at"])
        if end <= start:
            skipped.append((service["name"], "end time is not after start time"))
            continue

        full_day = service.get("service_rate_type_name") == "Day"
        segments = _split_into_daily_segments(start, end, full_day=full_day)
        if segments is None:
            skipped.append((service["name"], f"span exceeds {MAX_SHIFT_SPAN_DAYS}-day split cap"))
            continue

        finalised = service.get("status_name") == FINALISED_STATUS_NAME
        single_day = len(segments) == 1
        for day_index, (seg_start, seg_end) in enumerate(segments):
            key = str(service["id"]) if single_day else f"{service['id']}:{day_index}"
            if finalised:
                protected_keys.add(key)
                continue
            expanded.append((key, service, seg_start, seg_end))
    return expanded, skipped, protected_keys


def _cleanup_shift_keys(
    client: httpx.Client,
    keys: list[str],
    shifts_state: dict[str, Any],
    opportunity_id: int,
    reason: str,
    job_title_cache: dict[str, Any],
) -> dict[str, Any]:
    """Shared teardown for a set of tracked shift keys that should no longer
    exist (whole opportunity ineligible, or an individual service item gone
    from an opportunity that's otherwise still eligible) — deleted outright
    from Connecteam, draft or already-published alike. Ops is notified
    afterwards rather than being asked to act, since the deletion has
    already happened by the time the message lands (a deliberate reversal
    of an earlier "never touch a published shift" safety net: a shift whose
    underlying Current RMS item is genuinely gone is no longer real work
    someone needs to show up for). All keys passed in belong to one
    opportunity (every caller scopes it that way), so the
    published-deletion notification is batched into ONE Chat message
    rather than one per shift."""
    job_id_to_title = {v: k for k, v in job_title_cache.get("by_title", {}).items()}

    deleted = 0
    already_gone = 0
    deleted_published_blocks: list[str] = []
    order_number = None
    for key in keys:
        shift_id = shifts_state[key]["shiftId"]
        shift = get_shift(client, shift_id)
        if shift is None:
            already_gone += 1
            del shifts_state[key]
            continue
        if shift.get("isPublished"):
            order_number = shifts_state[key].get("orderNumber") or order_number
            title = shifts_state[key].get("title")
            job_label = job_id_to_title.get(shifts_state[key].get("jobId"), "(no Job)")
            deleted_published_blocks.append(f'"{title}" — {job_label} ({shift_id})')
        delete_shift(client, shift_id)
        deleted += 1
        del shifts_state[key]

    if deleted_published_blocks:
        shift_word = "shift has" if len(deleted_published_blocks) == 1 else "shifts have"
        notify_ops(
            client,
            f"🗑️ Published Connecteam {shift_word} been deleted — no longer required.\n"
            f"Opportunity {opportunity_id} (order {order_number}) — {reason}.\n"
            + "\n".join(deleted_published_blocks),
        )

    return {
        "tracked_shifts": len(keys),
        "deleted_count": deleted,
        "deleted_published_count": len(deleted_published_blocks),
        "already_gone_count": already_gone,
    }


def cleanup_ineligible_opportunity(
    client: httpx.Client, opportunity_id: int, reason: str, state: dict[str, Any]
) -> dict[str, Any]:
    """An opportunity that used to be eligible (Order, or flagged Quotation)
    no longer is — flag was unticked, it went dead/lost/reverted, or it was
    deleted outright. All shifts tracked for it are torn down via
    _cleanup_shift_keys (deleted outright, draft or published; ops notified
    afterwards for anything that was published)."""
    shifts_state: dict[str, Any] = state["shifts"]
    keys = [k for k, v in shifts_state.items() if v.get("opportunityId") == opportunity_id]
    if not keys:
        return {"status": "skipped", "reason": reason, "tracked_shifts": 0}

    result = _cleanup_shift_keys(client, keys, shifts_state, opportunity_id, reason, state["job_title_cache"])
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
    # no longer shows up here — deleted from the opportunity, excluded, or
    # had its dates cleared — is torn down the same way a whole ineligible
    # opportunity is: deleted outright (draft or published), Chat notified
    # if it was published. Same applies to a single-day-turned-multi-day (or
    # vice versa) item's now-stale key, since _expand_service_segments
    # changes key format between the two cases — current_keys covers every
    # key that SHOULD exist right now (split or not) PLUS every finalised
    # item's key, so a finalised item's shift is protected from this
    # teardown rather than treated as removed.
    expanded_segments, segment_skips, protected_keys = _expand_service_segments(services)
    current_keys = {key for key, _, _, _ in expanded_segments} | protected_keys
    orphaned_keys = [
        k
        for k, v in shifts_state.items()
        if v.get("opportunityId") == opportunity_id and k not in current_keys
    ]
    orphan_result = (
        _cleanup_shift_keys(
            client,
            orphaned_keys,
            shifts_state,
            opportunity_id,
            "service item removed from opportunity",
            state["job_title_cache"],
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

    # Site contact: defaults to the opportunity owner's name, overridden by
    # the "on-site contact if different" custom field when it's filled in.
    # Computed once per opportunity (not per service item) — it's the same
    # for every shift on this order. Confirmed with the user 2026-08-19.
    site_contact_override = (
        (opportunity.get("custom_fields") or {})
        .get("on-site_contact_if_different_from_project_contact", "")
        or ""
    ).strip()
    site_contact = site_contact_override or fetch_member_name(client, opportunity.get("owned_by")) or ""

    job_title_cache: dict[str, Any] = state["job_title_cache"]

    # Refresh the whole Job title/color cache once per sync. The cache used
    # to only auto-refresh on a title *miss*, so once a jobId was cached its
    # color was trusted forever — if a user later changed that Job's color
    # in Connecteam, already-synced shifts would never pick it up (confirmed
    # live via /debug/inspect-sync: two shifts' live "color" no longer
    # matched their Job's actual current color). Jobs lists are small
    # (~60 in this account), so one extra API call per opportunity sync is
    # cheap insurance against silently stale colors.
    _fresh_by_title, _fresh_by_jobid_color = _load_job_title_cache(client)
    job_title_cache.setdefault("by_title", {}).clear()
    job_title_cache["by_title"].update(_fresh_by_title)
    job_title_cache.setdefault("by_jobid_color", {}).clear()
    job_title_cache["by_jobid_color"].update(_fresh_by_jobid_color)

    subject = opportunity.get("subject") or f"Order {opportunity.get('number', opportunity['id'])}"
    if "#" in subject:
        subject = subject.split("#", 1)[1].strip()

    to_create: list[tuple[str, dict[str, Any], dict[str, Any]]] = []  # (key, payload, desired)
    to_update: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    unchanged = 0
    skipped: list[tuple[str, str]] = list(segment_skips)
    # Service name per shift key, kept so a stale-Job-ID retry (see
    # _is_stale_job_id_error below) can redo the title lookup after a forced
    # cache refresh without re-walking the Current RMS service items.
    service_name_by_key: dict[str, str] = {}

    for key, service, start, end in expanded_segments:

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
            "siteContact": site_contact,
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

        notes_html = (
            f"<p>Auto-created from Current RMS order "
            f"{opportunity.get('number', opportunity['id'])} "
            f"(opportunity item #{service['id']}).</p>"
        )
        if site_contact:
            notes_html += f"<p>Site contact: {site_contact}</p>"
        notes = [{"html": notes_html}]

        existing = shifts_state.get(key)
        if existing is not None:
            # Verify the tracked shift still exists before trusting it for
            # a diff — a human can delete a shift directly in Connecteam,
            # and without this check the app would keep believing it's
            # still there (comparing against tracked state only, never
            # Connecteam) and silently never recreate it. Confirmed live
            # via /debug/inspect-sync: one shift on order #4012 had been
            # deleted this way and stayed missing indefinitely.
            live_shift = get_shift(client, existing["shiftId"])
            if live_shift is None:
                log.warning(
                    "Tracked shift %s for opportunity %s (service item %s) no longer "
                    "exists in Connecteam — recreating it.",
                    existing["shiftId"], opportunity_id, key,
                )
                del shifts_state[key]
                existing = None
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
            or existing.get("siteContact") != site_contact
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
    # silent change. Batched into ONE Chat message for this whole sync call
    # rather than one per shift.
    job_id_to_title = {v: k for k, v in job_title_cache.get("by_title", {}).items()}

    def _job_label(job_id: str | None) -> str:
        if not job_id:
            return "(none)"
        return job_id_to_title.get(job_id, job_id)

    published_edit_blocks: list[str] = []
    for (key, _, desired), shift_obj in zip(to_update, updated_shifts):
        if shift_obj.get("isPublished"):
            previous = previous_by_key.get(key, {})
            changes: list[str] = []
            if previous.get("jobId") != desired.get("jobId"):
                changes.append(
                    f"Job — Original: {_orange(_job_label(previous.get('jobId')))}, "
                    f"Updated to: {_green(_job_label(desired.get('jobId')))}"
                )
            if previous.get("startTime") != desired.get("startTime") or previous.get("endTime") != desired.get(
                "endTime"
            ):
                previous_time = _format_epoch(previous["startTime"]) + " – " + _format_epoch(previous["endTime"])
                desired_time = _format_epoch(desired["startTime"]) + " – " + _format_epoch(desired["endTime"])
                changes.append(
                    f"Time — Original: {_orange(previous_time)}, Updated to: {_green(desired_time)}"
                )
            if previous.get("title") != desired.get("title"):
                changes.append(
                    f'Title — Original: "{_orange(previous.get("title"))}", '
                    f'Updated to: "{_green(desired.get("title"))}"'
                )
            if previous.get("quantity") != desired.get("quantity"):
                changes.append(
                    f"Quantity required — Original: {_orange(previous.get('quantity'))}, "
                    f"Updated to: {_green(desired.get('quantity'))}"
                )
            if previous.get("address") != desired.get("address"):
                changes.append(
                    f"Address — Original: {_orange(previous.get('address'))}, "
                    f"Updated to: {_green(desired.get('address'))}"
                )
            if previous.get("description") != desired.get("description"):
                changes.append(
                    f'Notes — Original: "{_orange(previous.get("description"))}", '
                    f'Updated to: "{_green(desired.get("description"))}"'
                )
            if previous.get("siteContact") != desired.get("siteContact"):
                changes.append(
                    f'Site contact — Original: "{_orange(previous.get("siteContact"))}", '
                    f'Updated to: "{_green(desired.get("siteContact"))}"'
                )
            change_text = "\n".join(f"  • {c}" for c in changes) if changes else "  (no tracked-field change detected)"
            published_edit_blocks.append(
                f"Shift \"{desired.get('title')}\" — {_job_label(desired.get('jobId'))} "
                f"({shift_obj['id']}):\n{change_text}"
            )

    if published_edit_blocks:
        shift_word = "shift" if len(published_edit_blocks) == 1 else "shifts"
        notify_ops(
            client,
            f"✏️ {len(published_edit_blocks)} published Connecteam {shift_word} edited by sync.\n"
            f"{subject} — Order {order_number} (Opportunity {opportunity_id}):\n\n"
            + "\n\n".join(published_edit_blocks)
            + "\n\nPlease confirm crew are aware of the change.",
        )

    result = {
        "status": "ok",
        "created_count": len(created_shifts),
        "updated_count": len(updated_shifts),
        "unchanged_count": unchanged,
        "skipped_count": len(skipped),
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


@app.get("/debug/sweep-dead-lost")
async def sweep_dead_lost(token: str | None = None):
    """One-off sweep: check every opportunity with tracked shifts against
    Current RMS, and clean up any that are now Dead/Lost — covers
    opportunities that went dead/lost before the status_name eligibility
    check existed. Temporary; safe to remove once run."""
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        with sync_lock:
            state = _load_state()
            shifts_state: dict[str, Any] = state["shifts"]
            opportunity_ids = sorted({v["opportunityId"] for v in shifts_state.values()})

            cleaned: dict[str, Any] = {}
            with httpx.Client(timeout=30) as client:
                for opportunity_id in opportunity_ids:
                    try:
                        opportunity = fetch_opportunity(client, opportunity_id)
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 404:
                            cleaned[str(opportunity_id)] = cleanup_ineligible_opportunity(
                                client, opportunity_id, "deleted", state
                            )
                        else:
                            cleaned[str(opportunity_id)] = {"status": "error", "detail": str(exc)}
                        continue
                    status_name = opportunity.get("status_name")
                    if status_name in DEAD_OR_LOST_STATUS_NAMES:
                        cleaned[str(opportunity_id)] = cleanup_ineligible_opportunity(
                            client, opportunity_id, f"status '{status_name}'", state
                        )
            _save_state(state)
            return {"opportunities_checked": len(opportunity_ids), "cleaned_up": cleaned}

    result = await asyncio.to_thread(_run)
    return JSONResponse(result)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "time": int(time.time())}
