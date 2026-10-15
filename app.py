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
  - Operational edits/deletions of published shifts with work remaining
    notify ops in Google Chat. Past shifts, completed service lines and
    internal refreshes are silent. In-progress shifts retain alerts. See
    notify_ops / GOOGLE_CHAT_WEBHOOK_URL so a human knows a shift crew
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
from log_redaction import install_log_redaction
import os
import threading
import time
import re
import uuid
import html
from contextlib import contextmanager
from bridge_safety import RecoveryRequired, ReadRetryClient, load_state, save_state, state_lock
from rms_pagination import fetch_pages, ServiceListing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
install_log_redaction()
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
# manually assign that many people — but manually assigning a 2nd/3rd
# person does NOT add them to that one shift's assignedUserIds; Connecteam
# forks a whole separate shift record per extra assignee (same Job/title/
# custom fields, same original time). Confirmed live 2026-09-07 against
# order 100004013's "Set up" item (Qty Rqrd 2): a time edit only reached
# the originally-created shiftId, leaving the second assignee's forked
# shift at the old time. sync_opportunity's per-item loop now searches for
# and updates these forks too — see _find_sibling_shift_ids — and a
# tracked item's state entry holds a "shiftIds" list rather than one id.
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

# The recovery CLI must not run against a poller that can undo its mappings or
# ignore its validation quarantine. Bump only with a reviewed policy change.
ITEM_RECOVERY_POLICY_VERSION = 1

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

# Exact source Service names from the reviewed transport job mappings above.
# Do not infer transport from a job title, description, or partial name match.
TRANSPORT_ARRIVAL_SERVICE_NAMES = frozenset({
    "Truck 8 Ton - Collection (Distance)",
    "Truck 8 Ton - Delivery (Distance)",
    "Truck 8 Ton - Perth Collection",
    "Truck 8 Ton - Perth Collection (After hours and Weekend)",
    "Truck 8 Ton - Perth Delivery",
    "Truck 8 Ton - Perth Delivery (After hours and Weekend)",
    "Van 1 Ton Perth Collection",
    "Van 1 Ton Perth Collection  (After hours and Weekend)",
    "Van 1 Ton Perth Delivery",
    "Van 1 Ton Perth Delivery (After hours and Weekend)",
})

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
    data = load_state(STATE_FILE)
    data.setdefault("jobs", {})
    data.setdefault("job_title_cache", {})
    data.setdefault("poll_cursor", None)
    data.setdefault("pending_creates", {})
    data.setdefault("retry_opportunities", {})
    data.setdefault("exceptions", {})
    for entry in data["shifts"].values():
        if "shiftId" in entry:
            entry["shiftIds"] = [entry.pop("shiftId")]
    return data


def _save_state(state: dict[str, Any]) -> None:
    state["schema_version"] = 2
    save_state(STATE_FILE, state)


@contextmanager
def _sync_guard():
    with sync_lock, state_lock(STATE_FILE):
        yield


def _record_exception(state, opportunity_id, message):
    state.setdefault("exceptions", {})[str(opportunity_id)] = {"message": message, "at": int(time.time())}



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
            page_ids = {o["id"] for o in body["opportunities"]}
            if page > 1 and page_ids and not page_ids - ids:
                raise RecoveryRequired("OnRent pagination did not advance")
            ids.update(page_ids)
            meta = body.get("meta", {})
            if len(body["opportunities"]) < 100 or ("total_row_count" in meta and page * meta.get("per_page", 100) >= meta["total_row_count"]):
                break
            page += 1
            if page > 200:
                raise RecoveryRequired("OnRent pagination exceeded bounded scan")
    return sorted(ids)


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
    """Read completely before cleanup; gate newly revealed creates separately."""
    items, legacy_ids = fetch_pages(client,
        f"{CURRENT_RMS_BASE_URL}/api/v1/opportunities/{opportunity_id}/opportunity_items",
        rms_headers(), "opportunity_items")
    return ServiceListing([
        item for item in items
        if item.get("item_type") == "Service" and item.get("name") not in EXCLUDED_SERVICE_NAMES
    ], legacy_ids)


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
        offset += len(jobs)
        if offset > 20000:
            raise RecoveryRequired("Jobs pagination exceeded bounded scan")
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


def list_shifts(client, start, end):
    """Bounded, paginated reads with a forward-progress guard."""
    result = []
    seen = set()
    offset = 0
    while True:
        response = client.get(
            f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts",
            headers={"X-API-KEY": CONNECTEAM_API_KEY},
            params={"startTime": start, "endTime": end, "limit": 500, "offset": offset},
        )
        response.raise_for_status()
        page = response.json().get("data", {}).get("shifts", [])
        ids = {str(row["id"]) for row in page}
        if page and (not ids - seen or len(result) + len(page) > 20000):
            raise RecoveryRequired("Shift pagination did not advance or exceeded bounded scan")
        result.extend(row for row in page if str(row["id"]) not in seen)
        seen.update(ids)
        if len(page) < 500:
            return result
        offset += len(page)


def _note_html(shift):
    return " ".join(note.get("html", "") for note in shift.get("notes", []) if isinstance(note, dict))


def _find_sibling_shift_ids(client, order_number, shift_type, job_id, start, end, item_id=None):
    # Require the source item marker copied by Connecteam, plus exact previous
    # time. Job/description may be edited and are not reliable identities.
    if not item_id:
        return []
    marker = f"(opportunity item #{item_id})"
    return [row["id"] for row in list_shifts(client, start - 300, end + 300)
            if marker in _note_html(row)
            and row.get("startTime") == start and row.get("endTime") == end]


def _merge_owned_notes(live, generated):
    """Replace only the bridge paragraph, retaining manual notes and text."""
    result = []
    for note in live.get("notes", []):
        if not isinstance(note, dict):
            raise RecoveryRequired("Unexpected notes shape; refuse to overwrite manual notes")
        if "html" not in note:
            raise RecoveryRequired("Non-HTML manual notes present; preserve them for review")
        content = note["html"]
        if re.fullmatch(r"<p>Bridge identity: [a-f0-9]{32}</p>", content):
            continue
        if "Auto-created from Current RMS order " in content:
            content = re.sub(r"<p>Auto-created from Current RMS order .*?</p>", "", content, flags=re.S)
            content = re.sub(r"<p>Site contact: .*?</p>", "", content, flags=re.S)
            content = re.sub(r"<p>Bridge identity: .*?</p>", "", content, flags=re.S)
        if content.strip():
            result.append({"html": content})
    return result + generated


def _durable_create(client, key, payload, desired, state):
    pending = state.setdefault("pending_creates", {})
    intent = pending.get(key)
    if intent:
        matches = [row for row in list_shifts(client, intent["payload"]["startTime"] - 86400,
                    intent["payload"]["endTime"] + 86400)
                   if intent["marker"] in _note_html(row)]
        if len(matches) == 1:
            row = matches[0]
            if any(str(row["id"]) in {str(sid) for sid in entry.get("shiftIds", [])} for other_key, entry in state["shifts"].items() if other_key != key):
                raise RecoveryRequired("Recovered create identity already belongs to another item")
            state["shifts"][key] = {"shiftIds": [row["id"]], **intent["desired"]}
            del pending[key]
            _save_state(state)
            return row
        # Even an empty read is not proof that a timed-out POST never committed.
        # Deliberately hold rather than replay an ambiguous create.
        raise RecoveryRequired(f"Create outcome unresolved for item {key}; matches={len(matches)}")
    marker = "Bridge identity: " + uuid.uuid4().hex
    payload = {**payload, "notes": [dict(n) for n in payload.get("notes", [])]}
    payload["notes"].append({"html": f"<p>{marker}</p>"})
    desired = {**desired, "bridgeIdentity": marker}
    pending[key] = {"marker": marker, "payload": payload, "desired": desired, "opportunityId": desired["opportunityId"]}
    _save_state(state)  # write intent BEFORE sending any remote create
    try:
        rows = create_shifts(client, [payload])
    except httpx.HTTPStatusError as exc:
        # A definitive validation/auth rejection accepted no shift. Permit a
        # later corrected retry; timeouts and server failures remain ambiguous.
        if exc.response.status_code in {400, 401, 403, 404, 422, 429}:
            del pending[key]
            _save_state(state)
        raise
    if len(rows) != 1 or not rows[0].get("id"):
        raise RecoveryRequired(f"Unexpected create response for item {key}; intent retained")
    row = rows[0]
    if any(str(row["id"]) in {str(sid) for sid in entry.get("shiftIds", [])} for other_key, entry in state["shifts"].items() if other_key != key):
        raise RecoveryRequired("Create response identity already belongs to another item")
    state["shifts"][key] = {"shiftIds": [row["id"]], **desired}
    del pending[key]
    _save_state(state)  # commit each accepted identity before the next request
    return row


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

    # The interval is [start, end): midnight belongs only to the previous day.
    end_dt = datetime.fromtimestamp(end - 1, tz=PERTH_TZ)
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


def _transport_arrival_time(service: dict[str, Any]) -> int | None:
    """Identify an equal-time transport milestone without changing source data."""
    if service.get("name") not in TRANSPORT_ARRIVAL_SERVICE_NAMES:
        return None
    if not service.get("starts_at") or not service.get("ends_at"):
        return None
    start = datetime.fromisoformat(service["starts_at"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(service["ends_at"].replace("Z", "+00:00"))
    return _to_epoch_seconds(service["starts_at"]) if start == end else None


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
        if service.get("status_name") == FINALISED_STATUS_NAME:
            protected_keys.add(str(service["id"]))
            continue
        if not service.get("starts_at") or not service.get("ends_at"):
            skipped.append((service["name"], "missing schedule; existing shifts protected"))
            protected_keys.add(str(service["id"]))
            continue
        start = _to_epoch_seconds(service["starts_at"])
        end = _to_epoch_seconds(service["ends_at"])
        if _transport_arrival_time(service) is not None:
            # Arrival only: keep the original start, even for Day rate items
            # and midnight rollover. Bypass full-day expansion deliberately.
            expanded.append((str(service["id"]), service, start, end + 60))
            continue
        if end <= start:
            skipped.append((service["name"], "arrival milestone: equal times; destination support unverified" if end == start else "end before start; existing shifts protected"))
            protected_keys.add(str(service["id"]))
            continue

        full_day = service.get("service_rate_type_name") == "Day"
        segments = _split_into_daily_segments(start, end, full_day=full_day)
        if segments is None:
            skipped.append((service["name"], f"span exceeds {MAX_SHIFT_SPAN_DAYS}-day split cap"))
            protected_keys.add(str(service["id"]))
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


def _notification_work_remaining(*versions: dict[str, Any], now: float) -> bool:
    """Notification-only date gate; sync must never use this predicate.

    Use scheduled shift segments, not opportunity dates or Connecteam Job
    dates (Jobs are task types). Check both versions so moving a future
    shift into the past cannot hide its change. Retain in-progress alerts
    pending an explicit operational decision about that boundary. Unknown
    legacy dates retain alerts rather than silently hiding cancellations.
    """
    for version in versions:
        if version.get("notificationFinalised") or version.get("notificationCancelled"):
            continue
        end = version.get("endTime")
        if not isinstance(end, (int, float)) or isinstance(end, bool):
            return True
        if end > now:
            return True
    return False


def _cleanup_shift_keys(
    client: httpx.Client,
    keys: list[str],
    shifts_state: dict[str, Any],
    opportunity_id: int,
    reason: str,
    job_title_cache: dict[str, Any],
    state: dict[str, Any] | None = None,
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
    tracked_shift_count = 0
    deleted_published_count = 0
    notification_now = time.time()
    for key in keys:
        entry = shifts_state[key]
        if opportunity_id == 3950 or entry.get("recoveryValidationPending") or entry.get("notificationFinalised"):
            continue
        # Discover copied source-item identity at the old schedule before deletion.
        if state is not None and entry.get("quantity", 1) > 1:
            siblings = _find_sibling_shift_ids(client, entry.get("orderNumber"), entry.get("description"), entry.get("jobId"), entry["startTime"], entry["endTime"], key.split(":", 1)[0])
            owned_elsewhere = {sid for other_key, other in shifts_state.items() if other_key != key for sid in other.get("shiftIds", [])}
            if any(sid in owned_elsewhere for sid in siblings):
                raise RecoveryRequired("Cleanup sibling has conflicting ownership")
            entry["shiftIds"] = list(dict.fromkeys(entry.get("shiftIds", []) + siblings))
            _save_state(state)
        shift_ids = entry.get("shiftIds") or ([entry["shiftId"]] if "shiftId" in entry else [])
        tracked_shift_count += len(shift_ids)
        for shift_id in shift_ids:
            shift = get_shift(client, shift_id)
            if shift is None:
                already_gone += 1
                continue
            if shift.get("isPublished"):
                deleted_published_count += 1
            if (
                shift.get("isPublished")
                and not entry.get("notificationFinalised")
                and not entry.get("notificationCancelled")
                and _notification_work_remaining(entry, shift, now=notification_now)
            ):
                order_number = entry.get("orderNumber") or order_number
                title = entry.get("title")
                job_label = job_id_to_title.get(entry.get("jobId"), "(no Job)")
                deleted_published_blocks.append(f'"{title}" — {job_label} ({shift_id})')
            delete_shift(client, shift_id)
            deleted += 1
            entry["shiftIds"] = [sid for sid in entry.get("shiftIds", []) if sid != shift_id]
            if state is not None:
                _save_state(state)
        del shifts_state[key]
        if state is not None:
            _save_state(state)

    if deleted_published_blocks:
        shift_word = "shift has" if len(deleted_published_blocks) == 1 else "shifts have"
        notify_ops(
            client,
            f"🗑️ Published Connecteam {shift_word} been deleted — no longer required.\n"
            f"Opportunity {opportunity_id} (order {order_number}) — {reason}.\n"
            + "\n".join(deleted_published_blocks),
        )

    return {
        "tracked_shifts": tracked_shift_count,
        "deleted_count": deleted,
        "deleted_published_count": deleted_published_count,
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

    result = _cleanup_shift_keys(client, keys, shifts_state, opportunity_id, reason, state["job_title_cache"], state)
    return {"status": "cleaned_up", "reason": reason, **result}


# ---------------------------------------------------------------------------
# Core sync — idempotent, keyed off Current RMS opportunity_item IDs
# ---------------------------------------------------------------------------

def _assert_unique_ownership(state, opportunity_id):
    owners = {}
    for key, entry in state["shifts"].items():
        for sid in set(map(str, entry.get("shiftIds", []))):
            owners.setdefault(sid, []).append(key)
    conflicts = {sid: keys for sid, keys in owners.items() if len(keys) > 1 and any(state["shifts"][key].get("opportunityId") == opportunity_id for key in keys)}
    if conflicts:
        raise RecoveryRequired("Destination identity has multiple source owners: " + "; ".join(f"{sid}={','.join(keys)}" for sid, keys in conflicts.items()))


def reconcile_report(client, opportunity_id, state):
    """GET-only audit. Never invoke sync, cleanup, notifications or remote writes."""
    issues = []
    entries = {k: e for k, e in state["shifts"].items() if e.get("opportunityId") == opportunity_id}
    try:
        _assert_unique_ownership(state, opportunity_id)
    except RecoveryRequired as exc:
        issues.append(str(exc))
    try:
        opportunity = fetch_opportunity(client, opportunity_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise
        return {"mode": "report_only", "issues": issues + ["Source 404; cleanup requires review"], "checked_at": time.time()}
    eligible, reason = _is_eligible(opportunity)
    if not eligible or opportunity.get("status_name") in {"Cancelled", "Postponed", FINALISED_STATUS_NAME}:
        issues.append(f"Source status/state requires review: {opportunity.get('status_name')} / {reason}")
    services = fetch_service_items(client, opportunity_id)
    expanded, skips, _ = _expand_service_segments(services)
    source_ids = {str(item["id"]) for item in services}
    source_by_key = {key: (item, start, end) for key, item, start, end in expanded}
    for name, reason in skips:
        issues.append(f"{name}: {reason}")
    for key, entry in entries.items():
        if key.split(":", 1)[0] not in source_ids:
            issues.append(f"{key}: source item absent; no deletion performed")
        elif key in source_by_key:
            item, start, end = source_by_key[key]
            if (start, end) != (entry.get("startTime"), entry.get("endTime")) or (item.get("description") or "").strip() != entry.get("description", ""):
                issues.append(f"{key}: source schedule/description differs; no update performed")
        for sid in entry.get("shiftIds", []):
            live = get_shift(client, sid)
            if live is None:
                issues.append(f"{key}: destination {sid} absent; no recreation performed")
            elif any(live.get(field) != entry.get(field) for field in ("startTime", "endTime", "title", "jobId", "color")):
                issues.append(f"{key}: destination {sid} differs from tracked values; no update performed")
    for key in source_by_key.keys() - entries.keys():
        issues.append(f"{key}: source segment untracked; no creation performed")
    return {"mode": "report_only", "issues": issues, "checked_at": time.time()}


def sync_opportunity(client: httpx.Client, opportunity_id: int, state: dict[str, Any]) -> dict[str, Any]:
    """Create/update draft Connecteam shifts for one order, and its linked
    Job. Mutates `state` in place; caller is responsible for persisting it.
    Safe to call repeatedly for the same opportunity — matches existing
    shifts by opportunity_item ID and only sends an update when something
    actually changed."""
    # Explicit rollout exclusion, before pending-create reconciliation or cleanup.
    if opportunity_id == 3950:
        return {"status": "held", "reason": "Production Management 3950 excluded from rollout"}
    validation_held = any(
        entry.get("opportunityId") == opportunity_id and entry.get("recoveryValidationPending")
        for entry in state["shifts"].values()
    ) or any(
        intent.get("opportunityId") == opportunity_id
        and intent.get("desired", {}).get("recoveryValidationPending")
        for intent in state.get("pending_creates", {}).values()
    )
    if validation_held:
        return {"status": "held", "reason": "recovery destination validation requires review"}
    _assert_unique_ownership(state, opportunity_id)
    state.setdefault("exceptions", {}).pop(str(opportunity_id), None)
    # Resolve any uncertain earlier create before examining changed/deleted source.
    for key, intent in list(state.setdefault("pending_creates", {}).items()):
        if intent["opportunityId"] == opportunity_id:
            _durable_create(client, key, intent["payload"], intent["desired"], state)
    try:
        opportunity = fetch_opportunity(client, opportunity_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise
        return cleanup_ineligible_opportunity(client, opportunity_id, "deleted (source 404)", state)

    # Completion is archival; never erase crew history due to later metadata edits.
    if opportunity.get("status_name") == FINALISED_STATUS_NAME:
        for entry in state["shifts"].values():
            if entry.get("opportunityId") == opportunity_id:
                entry["notificationFinalised"] = True
        _save_state(state)
        return {"status": "skipped", "reason": "completed opportunity"}
    if opportunity.get("status_name") == "Postponed":
        _record_exception(state, opportunity_id, "Postponed: preserve shifts pending revised schedule")
        return {"status": "held", "reason": "postponed policy requires review"}

    eligible, reason = _is_eligible(opportunity)
    if not eligible:
        cleanup_result = cleanup_ineligible_opportunity(client, opportunity_id, reason, state)
        return {"status": "skipped", "reason": reason, **cleanup_result}

    services = fetch_service_items(client, opportunity_id)
    shifts_state: dict[str, Any] = state["shifts"]

    # Complete reads may reveal Services the legacy item reader never saw.
    # Preserve ordinary work for already-visible/tracked items; newly discovered
    # untracked items are report-only, including unrelated staffing.
    legacy_ids = getattr(services, "legacy_visible_ids", None)
    tracked_item_ids = {key.split(":", 1)[0] for key, entry in shifts_state.items()
                        if entry.get("opportunityId") == opportunity_id}
    newly_revealed_ids = {str(item["id"]) for item in services
                          if legacy_ids is not None and item["id"] not in legacy_ids
                          and str(item["id"]) not in tracked_item_ids}

    # Notification metadata only: remember completion even when a later
    # whole-opportunity cancellation enters cleanup before fetching items.
    # Include all tracked segment keys, including an old split layout.
    opportunity_completed = opportunity.get("status_name") == FINALISED_STATUS_NAME
    opportunity_cancelled = opportunity.get("status_name") == "Cancelled"
    finalised_ids = {str(s["id"]) for s in services if opportunity_completed or s.get("status_name") == FINALISED_STATUS_NAME}
    active_ids = {str(s["id"]) for s in services if s.get("status_name") not in {FINALISED_STATUS_NAME, "Cancelled"}}
    for key, entry in shifts_state.items():
        if entry.get("opportunityId") == opportunity_id:
            item_id = key.split(":", 1)[0]
            if item_id in finalised_ids:
                entry["notificationFinalised"] = True
            elif item_id in active_ids:
                entry.pop("notificationFinalised", None)

    # Removed source items retain the established cleanup policy. Validation,
    # completion and changed split layouts instead protect the old identities.
    expanded_segments, segment_skips, protected_keys = _expand_service_segments(services)
    if newly_revealed_ids:
        expanded_segments = [row for row in expanded_segments
                             if row[0].split(":", 1)[0] not in newly_revealed_ids]
        segment_skips.extend((item_id, "newly revealed service: report-only pending rollout review")
                             for item_id in sorted(newly_revealed_ids))
    # A source item survives validation failures and completion by identity,
    # not by whatever split layout its current dates happen to produce.
    held_ids = {key.split(":", 1)[0] for key in protected_keys}
    cancelled_ids = {str(item["id"]) for item in services if item.get("status_name") == "Cancelled"}
    postponed_ids = {str(item["id"]) for item in services if item.get("status_name") == "Postponed"}
    held_ids |= postponed_ids
    for item_id in postponed_ids | cancelled_ids:
        segment_skips.append((item_id, "cancelled/postponed: existing shifts held for review"))
    if opportunity_cancelled:
        segment_skips.append((str(opportunity_id), "cancelled opportunity: existing shifts held for review"))
    # Until cancellation policy is confirmed, never create or edit cancelled work.
    held_ids |= cancelled_ids
    if opportunity_cancelled:
        held_ids |= {str(item["id"]) for item in services}
        held_ids |= {key.split(":", 1)[0] for key, entry in shifts_state.items() if entry.get("opportunityId") == opportunity_id}
    expected_by_item = {}
    old_by_item = {}
    for key, _, _, _ in expanded_segments:
        expected_by_item.setdefault(key.split(":", 1)[0], set()).add(key)
    for key, entry in shifts_state.items():
        if entry.get("opportunityId") == opportunity_id:
            old_by_item.setdefault(key.split(":", 1)[0], set()).add(key)
    for item_id in expected_by_item.keys() & old_by_item.keys():
        if expected_by_item[item_id] != old_by_item[item_id]:
            held_ids.add(item_id)
            segment_skips.append((item_id, "segment layout changed; preserve assignments pending migration review"))
    protected_keys |= {key for key, entry in shifts_state.items() if entry.get("opportunityId") == opportunity_id and key.split(":", 1)[0] in held_ids}
    # Cancelled items continue through the notification transition check below,
    # but produce no create/update payloads.
    expanded_segments = [row for row in expanded_segments if row[0].split(":", 1)[0] not in held_ids or row[0].split(":", 1)[0] in cancelled_ids or opportunity_cancelled]
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
            state,
        )
        if orphaned_keys
        else None
    )

    if not services and not opportunity_cancelled:
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
    cancellation_candidates: list[tuple[dict[str, Any], dict[str, Any], list[str]]] = []
    notification_cancelled_by_key: dict[str, bool] = {}

    expanded_keys = {row[0] for row in expanded_segments}
    for key, entry in shifts_state.items():
        if entry.get("opportunityId") != opportunity_id or key in expanded_keys:
            continue
        if opportunity_cancelled or key.split(":", 1)[0] in cancelled_ids:
            notification_cancelled_by_key[key] = True
            if not entry.get("notificationCancelled") and not entry.get("notificationFinalised"):
                live = [get_shift(client, sid) for sid in entry.get("shiftIds", [])]
                published = [row["id"] for row in live if row and row.get("isPublished")]
                cancellation_candidates.append((dict(entry), dict(entry), published))

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

        arrival_time = _transport_arrival_time(service)
        arrival_note = None
        if arrival_time is not None:
            local = datetime.fromtimestamp(arrival_time, tz=PERTH_TZ)
            clock = f"{local.hour % 12 or 12}:{local.minute:02d}{'am' if local.hour < 12 else 'pm'}"
            arrival_note = (
                f"On site at {clock} sharp (AWST). "
                "The one-minute slot represents arrival only, not work duration."
            )

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
            "arrivalNote": arrival_note,
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
            f"(opportunity item #{service['id']})."
            + (f" {arrival_note}" if arrival_note else "")
            + "</p>"
        )
        if site_contact:
            notes_html += f"<p>Site contact: {html.escape(site_contact)}</p>"
        notes = [{"html": notes_html}]

        existing = shifts_state.get(key)
        previous_notification = dict(existing) if existing is not None else None
        published_ids: list[str] = []
        live_by_id: dict[str, Any] = {}
        cancelled = opportunity_cancelled or service.get("status_name") == "Cancelled"
        notification_cancelled_by_key[key] = cancelled
        force_sync_siblings = False
        if existing is not None:
            # Verify every tracked shift still exists before trusting it for
            # a diff — a human can delete a shift directly in Connecteam,
            # and without this check the app would keep believing it's
            # still there (comparing against tracked state only, never
            # Connecteam) and silently never recreate it. Confirmed live
            # via /debug/inspect-sync: one shift on order #4012 had been
            # deleted this way and stayed missing indefinitely.
            tracked_ids = existing.get("shiftIds") or ([existing["shiftId"]] if "shiftId" in existing else [])
            live_ids: list[str] = []
            primary_live_shift = None
            for shift_id in tracked_ids:
                live_shift = get_shift(client, shift_id)
                if live_shift is None:
                    raise RecoveryRequired(f"Tracked destination {shift_id} for item {key} is absent; preserve mapping pending review")
                live_ids.append(shift_id)
                live_by_id[str(shift_id)] = live_shift
                if live_shift.get("isPublished"):
                    published_ids.append(shift_id)
                if primary_live_shift is None:
                    primary_live_shift = live_shift

            # Qty Rqrd > 1 items: Connecteam forks a whole new shift record
            # (rather than adding to assignedUserIds) when a 2nd/3rd person
            # is manually assigned — see _find_sibling_shift_ids. Search
            # using the shift's still-live (pre-update) time so a
            # not-yet-tracked fork sitting at that time is found before this
            # sync's update is applied.
            if primary_live_shift is not None and max(quantity, existing.get("quantity", 1)) > 1:
                for sibling_id in _find_sibling_shift_ids(
                    client,
                    order_number,
                    existing.get("description", ""),
                    existing.get("jobId"),
                    primary_live_shift["startTime"],
                    primary_live_shift["endTime"],
                    str(service["id"]),
                ):
                    if sibling_id not in live_ids:
                        if any(sibling_id in other.get("shiftIds", []) for other_key, other in shifts_state.items() if other_key != key):
                            raise RecoveryRequired("Sibling belongs to another tracked source item; refusing reassignment")
                        sibling = get_shift(client, sibling_id)
                        if sibling is None:
                            continue
                        live_by_id[str(sibling_id)] = sibling
                        live_ids.append(sibling_id)
                        if sibling.get("isPublished"):
                            published_ids.append(sibling_id)
                        force_sync_siblings = True
                        log.info(
                            "Discovered Connecteam-forked sibling shift %s for opportunity %s "
                            "item %s (Qty Rqrd %s) — bringing it in sync with %s",
                            sibling_id, opportunity_id, key, quantity, tracked_ids,
                        )

            if not live_ids:
                del shifts_state[key]
                existing = None
            else:
                existing = {**existing, "shiftIds": live_ids}
                shifts_state[key] = existing
        # A Cancelled status does not currently remove shifts in the sync
        # contract. Alert on the transition independently of field edits,
        # without claiming the Connecteam shift was deleted.
        if cancelled and previous_notification and not previous_notification.get("notificationCancelled"):
            cancellation_candidates.append((previous_notification, desired, published_ids))
        if cancelled:
            unchanged += 1
            continue
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
            or existing.get("arrivalNote") != arrival_note
            or force_sync_siblings
        ):
            # One payload per tracked shiftId — a Qty Rqrd > 1 item can have
            # more than one live Connecteam shift (see the sibling-discovery
            # block above), and every one of them needs the same update.
            for shift_id in existing["shiftIds"]:
                live = live_by_id[str(shift_id)]
                generated_notes = [dict(n) for n in notes]
                if existing.get("bridgeIdentity"):
                    generated_notes.append({"html": f'<p>{existing["bridgeIdentity"]}</p>'})
                update_payload: dict[str, Any] = {
                    "shiftId": shift_id,
                    "startTime": start,
                    "endTime": end,
                    "title": title,
                    "notes": _merge_owned_notes(live, generated_notes),
                    "customFields": custom_fields,
                }
                if job_id:
                    update_payload["jobId"] = job_id
                if job_color:
                    update_payload["color"] = job_color
                if address:
                    update_payload["locationData"] = {"isReferencedToJob": False, "gps": {"address": address}}
                elif existing.get("address"):
                    current_address = ((live.get("locationData") or {}).get("gps") or {}).get("address")
                    if current_address != existing.get("address"):
                        raise RecoveryRequired("Destination address differs from bridge-owned value; manual review required")
                    update_payload["locationData"] = {"isReferencedToJob": False, "gps": None}
                if existing.get("color") and not job_color:
                    raise RecoveryRequired("Mapped color missing; preserve destination color pending review")
                if existing.get("jobId") and not job_id:
                    raise RecoveryRequired("Mapped Job missing; preserve destination Job pending mapping repair")
                to_update.append((key, update_payload, desired))
        else:
            unchanged += 1

    if skipped:
        _record_exception(state, opportunity_id, "; ".join(f"{name}: {reason}" for name, reason in skipped))
        for name, reason in skipped:
            log.warning("Skipped service '%s': %s", name, reason)

    created_shifts = []
    for key, payload, desired in to_create:
        created_shifts.append(_durable_create(client, key, payload, desired, state))

    # Update one source item (all of its siblings) at a time. If a response is
    # short/uncertain, retain the old state and retry idempotent fields next time.
    updated_shifts = []
    for key in dict.fromkeys(key for key, _, _ in to_update):
        group = [(k, p, d) for k, p, d in to_update if k == key]
        rows = update_shifts(client, [p for _, p, _ in group])
        expected = {str(p["shiftId"]) for _, p, _ in group}
        if {str(row.get("id")) for row in rows} != expected:
            raise RecoveryRequired(f"Update response identities incomplete for item {key}")
        updated_shifts.extend(rows)

    # Snapshot pre-update state before it's overwritten below, so the
    # published-edit notification can describe exactly what changed.
    previous_by_key = {key: dict(shifts_state.get(key, {})) for key, _, _ in to_update}

    for key, _, desired in to_update:
        existing = shifts_state.get(key, {})
        existing.update(desired)
        shifts_state[key] = existing

    _save_state(state)

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
    notification_now = time.time()
    # Match responses by identity, not response order, for alert formatting.
    updated_by_id = {str(shift["id"]): shift for shift in updated_shifts}
    for key, payload, desired in to_update:
        shift_obj = updated_by_id.get(str(payload["shiftId"]), {})
        previous = previous_by_key.get(key, {})
        if (
            shift_obj.get("isPublished")
            and not opportunity_completed
            and not notification_cancelled_by_key.get(key)
            and _notification_work_remaining(previous, desired, now=notification_now)
        ):
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
            if previous.get("arrivalNote") != desired.get("arrivalNote"):
                changes.append(
                    f'Arrival instruction — Original: "{_orange(previous.get("arrivalNote") or "No arrival-only instruction")}", '
                    f'Updated to: "{_green(desired.get("arrivalNote") or "No arrival-only instruction")}"'
                )
            if previous.get("siteContact") != desired.get("siteContact"):
                changes.append(
                    f'Site contact — Original: "{_orange(previous.get("siteContact"))}", '
                    f'Updated to: "{_green(desired.get("siteContact"))}"'
                )
            # Color refreshes, sibling discovery and bookkeeping-only
            # updates still sync but do not require crew action.
            if not changes:
                continue
            change_text = "\n".join(f"  • {c}" for c in changes)
            published_edit_blocks.append(
                f"Shift \"{desired.get('title')}\" — {_job_label(desired.get('jobId'))} "
                f"({shift_obj['id']}):\n{change_text}"
            )

    cancellation_blocks: list[str] = []
    for previous, desired, published_ids in cancellation_candidates:
        if not opportunity_completed and not previous.get("notificationFinalised") and _notification_work_remaining(previous, now=notification_now):
            for shift_id in published_ids:
                cancellation_blocks.append(f'"{desired.get("title")}" — {_job_label(desired.get("jobId"))} ({shift_id})')
    if cancellation_blocks:
        notify_ops(
            client,
            f"⚠️ OnRent work cancelled — Order {order_number} (Opportunity {opportunity_id}).\n"
            + "\n".join(cancellation_blocks)
            + "\nPlease review the published Connecteam shifts and confirm crew are aware.",
        )
    # Notification history only; never include it in the sync field diff.
    for key, cancelled in notification_cancelled_by_key.items():
        if key in shifts_state:
            shifts_state[key]["notificationCancelled"] = cancelled

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
        "newly_revealed_item_ids": sorted(newly_revealed_ids),
    }
    if orphan_result:
        result["orphaned_items_cleaned_up"] = orphan_result
    return result


def _sync_and_checkpoint(client, opportunity_id, state):
    """Never let advancing the poll cursor lose a failed opportunity."""
    key = str(opportunity_id)
    retry = state.setdefault("retry_opportunities", {})
    retry[key] = {"attempted_at": time.time()}
    _save_state(state)  # include webhook work before any outbound mutation
    try:
        result = sync_opportunity(client, opportunity_id, state)
        retry.pop(key, None)
        state.setdefault("reconciled_at", {})[key] = time.time()
        return result
    except Exception as exc:
        detail = type(exc).__name__
        if isinstance(exc, httpx.HTTPStatusError):
            detail += f" HTTP {exc.response.status_code}"
        retry[key]["error"] = detail
        _record_exception(state, opportunity_id, detail)
        log.exception("Sync failed for opportunity %s; retained for retry", opportunity_id)
        return {"status": "error", "detail": detail}
    finally:
        _save_state(state)


def poll_all_open_orders() -> dict[str, Any]:
    with _sync_guard():
        state = _load_state()
        poll_start = datetime.now(timezone.utc)
        cursor = state.get("poll_cursor")
        since = cursor or (poll_start - timedelta(hours=POLL_INITIAL_LOOKBACK_HOURS)).isoformat()
        results = {}
        with ReadRetryClient(timeout=30) as client:
            changed = fetch_opportunities_updated_since(client, since)
            retry_ids = [int(key) for key in state.setdefault("retry_opportunities", {})]
            # Direct reads catch deletes and state changes outside list filters.
            # At most ten tracked opportunities per poll, each at most daily.
            tracked = {e["opportunityId"] for e in state["shifts"].values() if not e.get("notificationFinalised")}
            tracked |= {e["opportunityId"] for e in state.get("pending_creates", {}).values()}
            checked = state.setdefault("reconciled_at", {})
            due = sorted((oid for oid in tracked if time.time() - checked.get(str(oid), 0) >= 86400), key=lambda oid: (checked.get(str(oid), 0), oid))[:10]
            ids = list(dict.fromkeys(changed + retry_ids))
            for opportunity_id in ids:
                results[str(opportunity_id)] = _sync_and_checkpoint(client, opportunity_id, state)
            for opportunity_id in due:
                try:
                    report = reconcile_report(client, opportunity_id, state)
                except Exception as exc:
                    report = {"mode": "report_only", "issues": [type(exc).__name__], "checked_at": time.time()}
                state.setdefault("reconciliation_reports", {})[str(opportunity_id)] = report
                checked[str(opportunity_id)] = time.time()
                log.info("Report-only reconciliation opportunity %s: %d issue(s)", opportunity_id, len(report["issues"]))
        state["poll_cursor"] = (poll_start - timedelta(minutes=POLL_OVERLAP_MINUTES)).isoformat()
        state["last_poll_completed_at"] = time.time()
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
        with _sync_guard():
            state = _load_state()
            # Always fetch source truth, including destroy actions (404 cleanup).
            # This also reconciles any pending create before removing mappings.
            with ReadRetryClient(timeout=30) as client:
                return _sync_and_checkpoint(client, int(opportunity_id), state)

    try:
        result = await asyncio.to_thread(_run)
    except httpx.HTTPStatusError as exc:
        log.exception("Upstream API error while processing opportunity %s", opportunity_id)
        raise HTTPException(
            status_code=502,
            detail=f"upstream error: {exc.response.status_code} {exc.response.text[:500]}",
        ) from exc

    if result.get("status") == "error":
        raise HTTPException(status_code=502, detail="Sync failed and retained for retry")
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


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "time": int(time.time())}


@app.get("/readyz")
async def readyz():
    """Only aggregate operational health; no source records or credentials."""
    try:
        state = await asyncio.to_thread(_load_state)
    except (RecoveryRequired, OSError):
        return JSONResponse({"status": "state-recovery-required"}, status_code=503)
    pending = len(state.get("pending_creates", {}))
    retries = len(state.get("retry_opportunities", {}))
    last_poll = state.get("last_poll_completed_at")
    stale = ENABLE_SCHEDULER and (last_poll is None or time.time() - last_poll > max(900, 3 * POLL_INTERVAL_SECONDS))
    return JSONResponse({"status": "degraded" if pending or retries or stale else "ok",
                         "item_recovery_policy_version": ITEM_RECOVERY_POLICY_VERSION,
                         "pending_creates": pending, "retry_opportunities": retries,
                         "exceptions": len(state.get("exceptions", {})),
                         "reconciliation_mode": "report_only",
                         "reconciliation_attention": sum(bool(r.get("issues")) for r in state.get("reconciliation_reports", {}).values()),
                         "last_poll_completed_at": last_poll}, status_code=503 if pending or retries or stale else 200)
