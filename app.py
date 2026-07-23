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
  - If the opportunity is no longer eligible (flag unticked, or it went
    dead/lost/reverted) -> DELETE any shifts we created that are still
    draft; leave already-published shifts alone for manual review.
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

# Shared secret appended to protected URLs as ?token=... . Current RMS
# webhooks aren't signed, so this query-string token is the gate for the
# webhook endpoint — it's also required for /sync so randoms can't trigger
# an unscheduled full sync. Keep the URL itself private too.
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")

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


def _load_job_title_cache(client: httpx.Client) -> dict[str, str]:
    """Fetch every Job in this scheduler and index by exact title. Jobs are
    curated task types (see /debug/create-test-jobs) — this never creates
    one, only looks them up."""
    headers = {"X-API-KEY": CONNECTEAM_API_KEY}
    by_title: dict[str, str] = {}
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
            if j.get("title"):
                by_title[j["title"]] = j["jobId"]
        if len(jobs) < 500:
            break
        offset = body.get("paging", {}).get("offset", offset + 500)
    return by_title


def find_job_by_service_name(
    client: httpx.Client, service_name: str, jobs_state: dict[str, Any]
) -> str | None:
    """Look up the task-type Job whose title is
    f"{CONNECTEAM_JOB_PREFIX}{service_name}" (e.g. "TEST Lighting
    Technician" while testing, or just "Lighting Technician" in
    production). Returns None — and logs a warning — if no matching Job
    exists; it does NOT create one. jobs_state is a title->jobId cache
    persisted in the state file so repeated syncs don't refetch the whole
    Jobs list every time."""
    # Built with an explicit space rather than relying on a trailing space
    # surviving inside CONNECTEAM_JOB_PREFIX itself — env var UIs (Render
    # included) tend to silently trim trailing whitespace on save, which
    # would otherwise turn "TEST Lighting technician" into
    # "TESTLighting technician" and break every lookup.
    prefix = CONNECTEAM_JOB_PREFIX.strip()
    title = f"{prefix} {service_name}" if prefix else service_name

    cache: dict[str, str] = jobs_state.setdefault("by_title", {})
    if title in cache:
        return cache[title]

    # Cache miss: refresh the whole title->jobId map once (cheap — Jobs
    # lists are small) and look again, in case a Job was added since the
    # cache was last built.
    cache.clear()
    cache.update(_load_job_title_cache(client))

    job_id = cache.get(title)
    if job_id is None:
        log.warning(
            "No Connecteam Job titled '%s' found for scheduler %s — shift will be created without a Job",
            title, CONNECTEAM_SCHEDULER_ID,
        )
    return job_id


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


def cleanup_ineligible_opportunity(
    client: httpx.Client, opportunity_id: int, reason: str, state: dict[str, Any]
) -> dict[str, Any]:
    """An opportunity that used to be eligible (Order, or flagged Quotation)
    no longer is — flag was unticked, or it went dead/lost/reverted.
    Draft (unpublished) shifts we created for it are deleted automatically.
    Published shifts are left alone — a human has already put real
    scheduling work into a published shift, so this only deletes what's
    still safely a draft. (Notifying ops about left-behind published shifts
    is planned but not wired up yet.)"""
    shifts_state: dict[str, Any] = state["shifts"]
    keys = [k for k, v in shifts_state.items() if v.get("opportunityId") == opportunity_id]
    if not keys:
        return {"status": "skipped", "reason": reason, "tracked_shifts": 0}

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
                "Opportunity %s is no longer eligible (%s) but shift %s is already published — "
                "leaving it in place for manual review.",
                opportunity_id, reason, shift_id,
            )
            continue
        delete_shift(client, shift_id)
        deleted += 1
        del shifts_state[key]

    return {
        "status": "cleaned_up",
        "reason": reason,
        "tracked_shifts": len(keys),
        "deleted_draft_count": deleted,
        "left_published_count": left_published,
        "already_gone_count": already_gone,
    }


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
    if not services:
        return {"status": "skipped", "reason": "no dated Service line items found"}

    address = fetch_venue_address(client, opportunity)
    order_number = opportunity.get("number")
    job_title_cache: dict[str, Any] = state["job_title_cache"]

    subject = opportunity.get("subject") or f"Order {opportunity.get('number', opportunity['id'])}"
    if "#" in subject:
        subject = subject.split("#", 1)[1].strip()
    shifts_state: dict[str, Any] = state["shifts"]

    to_create: list[tuple[str, dict[str, Any], dict[str, Any]]] = []  # (key, payload, desired)
    to_update: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    unchanged = 0
    skipped: list[tuple[str, str]] = []

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

        key = str(service["id"])
        desired = {
            "opportunityId": opportunity_id,
            "startTime": start,
            "endTime": end,
            "title": title,
            "jobId": job_id,
            "orderNumber": order_number,
            "address": address,
            "quantity": quantity,
        }

        custom_fields = []
        if order_number:
            custom_fields.append(
                {"customFieldId": CONNECTEAM_JOBNO_CUSTOM_FIELD_ID, "value": str(order_number)}
            )
        custom_fields.append(
            {"customFieldId": CONNECTEAM_QTY_CUSTOM_FIELD_ID, "value": str(quantity)}
        )

        existing = shifts_state.get(key)
        if existing is None:
            payload: dict[str, Any] = {
                "startTime": start,
                "endTime": end,
                "title": title,
                "isPublished": False,
                "notes": [
                    {
                        "html": (
                            f"<p>Auto-created from Current RMS order "
                            f"{opportunity.get('number', opportunity['id'])} "
                            f"(opportunity item #{service['id']}).</p>"
                        )
                    }
                ],
                "customFields": custom_fields,
            }
            if job_id:
                payload["jobId"] = job_id
            if address:
                payload["locationData"] = {"isReferencedToJob": False, "gps": {"address": address}}
            to_create.append((key, payload, desired))
        elif (
            existing.get("startTime") != start
            or existing.get("endTime") != end
            or existing.get("title") != title
            or existing.get("jobId") != job_id
            or existing.get("orderNumber") != order_number
            or existing.get("address") != address
            or existing.get("quantity") != quantity
        ):
            update_payload: dict[str, Any] = {
                "shiftId": existing["shiftId"],
                "startTime": start,
                "endTime": end,
                "title": title,
                "customFields": custom_fields,
            }
            if job_id:
                update_payload["jobId"] = job_id
            if address:
                update_payload["locationData"] = {"isReferencedToJob": False, "gps": {"address": address}}
            to_update.append((key, update_payload, desired))
        else:
            unchanged += 1

    if skipped:
        for name, reason in skipped:
            log.warning("Skipped service '%s': %s", name, reason)

    created_shifts = create_shifts(client, [p for _, p, _ in to_create])
    if len(created_shifts) != len(to_create):
        log.warning(
            "Created %d shifts but requested %d for opportunity %s — response ordering assumption may be wrong",
            len(created_shifts), len(to_create), opportunity_id,
        )
    for (key, _, desired), shift_obj in zip(to_create, created_shifts):
        shifts_state[key] = {"shiftId": shift_obj["id"], **desired}

    updated_shifts = update_shifts(client, [p for _, p, _ in to_update])
    for key, _, desired in to_update:
        existing = shifts_state.get(key, {})
        existing.update(desired)
        shifts_state[key] = existing

    return {
        "status": "ok",
        "created_count": len(created_shifts),
        "updated_count": len(updated_shifts),
        "unchanged_count": unchanged,
        "skipped_count": len(skipped),
    }


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


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "time": int(time.time())}
