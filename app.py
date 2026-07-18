"""
Current RMS -> Connecteam draft shift bridge.

When a Current RMS opportunity is converted to an order, and whenever its
Service line items are later edited, this service keeps Connecteam in sync:

  1. A webhook fires instantly on `opportunity_convert_to_order`.
  2. A background poll (every POLL_INTERVAL_SECONDS, default 15 minutes)
     scans every opportunity in "Order" state that's been updated since the
     last poll, so later edits to a Service item's dates/name are picked up
     even though Current RMS has no "opportunity updated" webhook.

Both paths funnel into the same idempotent sync routine, keyed off each
Current RMS opportunity_item's ID:
  - First time we see an item -> CREATE a draft Connecteam shift for it.
  - If we've already created a shift for that item -> UPDATE that same
    shift in place if the title/time/job changed, otherwise do nothing.
This guarantees we never create duplicate shifts, no matter how many times
an order is processed (webhook retries, overlapping polls, re-conversions).

It also finds or creates a Connecteam Job per order, with the Current RMS
order number in the Job's "Job No." (code) field and the order's venue
address in the Job's address field, and keeps the address in sync too.

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
            data.setdefault("jobs", {})
            data.setdefault("poll_cursor", None)
            return data
        except (json.JSONDecodeError, OSError):
            log.warning("Could not read state file %s, starting fresh", STATE_FILE)
    return {"shifts": {}, "jobs": {}, "poll_cursor": None}


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


def fetch_orders_updated_since(client: httpx.Client, since_iso: str) -> list[int]:
    """Return IDs of every opportunity in 'Order' state (state == 3) that's
    been updated since since_iso. Used by the background poll to catch
    Service-item edits made after conversion."""
    ids: list[int] = []
    page = 1
    while True:
        resp = client.get(
            f"{CURRENT_RMS_BASE_URL}/api/v1/opportunities",
            headers=rms_headers(),
            params={
                "q[state_eq]": 3,
                "q[updated_at_gteq]": since_iso,
                "per_page": 100,
                "page": page,
                "sort": "-updated_at",
            },
        )
        resp.raise_for_status()
        body = resp.json()
        ids.extend(o["id"] for o in body["opportunities"])
        meta = body.get("meta", {})
        if page * meta.get("per_page", 100) >= meta.get("total_row_count", 0):
            break
        page += 1
    return ids


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


def find_or_create_job(
    client: httpx.Client, opportunity: dict[str, Any], address: str | None, jobs_state: dict[str, Any]
) -> str | None:
    """Find (by Job No. / code) or create the Connecteam Job for this order,
    so its "Job No." box holds the Current RMS order number and its address
    field holds the venue address. Keeps the address in sync on later calls.
    Returns the Connecteam jobId, or None if the order has no number to key
    off of."""
    number = opportunity.get("number")
    if not number:
        return None

    headers = {"X-API-KEY": CONNECTEAM_API_KEY, "Content-Type": "application/json"}
    subject = opportunity.get("subject") or f"Order {number}"
    title = f"{subject} ({number})"

    cached = jobs_state.get(number)
    if cached:
        job_id = cached["jobId"]
        if cached.get("address") != address:
            resp = client.put(
                f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs/{job_id}",
                headers=headers,
                json={
                    "title": cached.get("title", title),
                    "code": number,
                    "assign": {"type": "both", "userIds": [], "groupIds": []},
                    "gps": {"address": address} if address else None,
                },
            )
            if resp.status_code >= 400:
                log.error("Connecteam rejected job address update: %s %s", resp.status_code, resp.text)
                resp.raise_for_status()
            cached["address"] = address
        return job_id

    # Not cached locally — look for an existing job with this Job No. before
    # creating, so a state-file reset doesn't produce duplicate jobs.
    resp = client.get(
        f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
        headers=headers,
        params={"jobCodes": number, "instanceIds": CONNECTEAM_SCHEDULER_ID},
    )
    resp.raise_for_status()
    existing = resp.json().get("data", {}).get("jobs", [])
    if existing:
        job = existing[0]
        jobs_state[number] = {"jobId": job["jobId"], "title": job.get("title", title), "address": address}
        return job["jobId"]

    job_payload: dict[str, Any] = {
        "instanceIds": [int(CONNECTEAM_SCHEDULER_ID)],
        "title": title,
        "code": number,
        "assign": {"type": "both", "userIds": [], "groupIds": []},
    }
    if address:
        job_payload["gps"] = {"address": address}

    resp = client.post(
        f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
        headers=headers,
        json=[job_payload],
    )
    if resp.status_code >= 400:
        log.error("Connecteam rejected job creation: %s %s", resp.status_code, resp.text)
        resp.raise_for_status()
    job_id = resp.json()["data"]["jobs"][0]["jobId"]
    jobs_state[number] = {"jobId": job_id, "title": title, "address": address}
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

    if opportunity.get("state_name") != "Order":
        return {"status": "skipped", "reason": f"opportunity state is '{opportunity.get('state_name')}', not 'Order'"}

    services = fetch_service_items(client, opportunity_id)
    if not services:
        return {"status": "skipped", "reason": "no dated Service line items found"}

    address = fetch_venue_address(client, opportunity)
    job_id = find_or_create_job(client, opportunity, address, state["jobs"])

    subject = opportunity.get("subject") or f"Order {opportunity.get('number', opportunity['id'])}"
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

        title = f"{subject} — {service['name']}"
        key = str(service["id"])
        desired = {"startTime": start, "endTime": end, "title": title, "jobId": job_id}

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
            }
            if job_id:
                payload["jobId"] = job_id
                payload["locationData"] = {"isReferencedToJob": True}
            to_create.append((key, payload, desired))
        elif (
            existing.get("startTime") != start
            or existing.get("endTime") != end
            or existing.get("title") != title
            or existing.get("jobId") != job_id
        ):
            update_payload: dict[str, Any] = {
                "shiftId": existing["shiftId"],
                "startTime": start,
                "endTime": end,
                "title": title,
            }
            if job_id and existing.get("jobId") != job_id:
                update_payload["jobId"] = job_id
                update_payload["locationData"] = {"isReferencedToJob": True}
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
    """Background/manual poll: sync every Order-state opportunity updated
    since the last poll, so Service-item edits made after conversion (which
    fire no webhook) still reach Connecteam."""
    with sync_lock:
        state = _load_state()
        poll_start = datetime.now(timezone.utc)
        cursor = state.get("poll_cursor")
        since = cursor or (poll_start - timedelta(hours=POLL_INITIAL_LOOKBACK_HOURS)).isoformat()

        results: dict[str, Any] = {}
        with httpx.Client(timeout=30) as client:
            opportunity_ids = fetch_orders_updated_since(client, since)
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


# ---------------------------------------------------------------------------
# TEMPORARY one-off recovery endpoints — remove after use.
# Reconstructs state["shifts"]/state["jobs"] from what's actually live in
# Connecteam (parsing the opportunity_item ID out of each shift's notes),
# and reports any opportunity_item that has more than one shift (duplicates
# created before this reconciliation existed). Dry-run by default.
# ---------------------------------------------------------------------------

import re as _re

_ITEM_RE = _re.compile(r"opportunity item #(\d+)")


@app.get("/debug/reconcile")
async def debug_reconcile(token: str | None = None, apply: bool = False):
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY}
        with httpx.Client(timeout=30) as client:
            # All jobs -> keyed by code (Current RMS order number).
            jobs_by_code: dict[str, Any] = {}
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
                    if j.get("code"):
                        jobs_by_code[j["code"]] = j
                if len(jobs) < 500:
                    break
                offset = body.get("paging", {}).get("offset", offset + 500)

            # All shifts in a wide window (5 years back/forward covers any
            # realistic order date).
            now = int(time.time())
            shifts: list[dict[str, Any]] = []
            offset = 0
            while True:
                resp = client.get(
                    f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts",
                    headers=headers,
                    params={
                        "startTime": now - 5 * 365 * 86400,
                        "endTime": now + 5 * 365 * 86400,
                        "limit": 500,
                        "offset": offset,
                        "sort": "created_at",
                        "order": "asc",
                    },
                )
                resp.raise_for_status()
                body = resp.json()
                batch = body.get("data", {}).get("shifts", [])
                shifts.extend(batch)
                if len(batch) < 500:
                    break
                offset = body.get("paging", {}).get("offset", offset + 500)

        by_item: dict[str, list[dict[str, Any]]] = {}
        for s in shifts:
            note_text = " ".join(n.get("html", "") for n in s.get("notes", []))
            m = _ITEM_RE.search(note_text)
            if not m:
                continue
            by_item.setdefault(m.group(1), []).append(s)

        new_shifts_state: dict[str, Any] = {}
        duplicates: dict[str, list[str]] = {}
        for item_id, group in by_item.items():
            group.sort(key=lambda s: s.get("creationTime") or 0)
            keeper = group[0]
            new_shifts_state[item_id] = {
                "shiftId": keeper["id"],
                "startTime": keeper["startTime"],
                "endTime": keeper["endTime"],
                "title": keeper["title"],
                "jobId": keeper.get("jobId"),
            }
            if len(group) > 1:
                duplicates[item_id] = [s["id"] for s in group[1:]]

        new_jobs_state: dict[str, Any] = {}
        for code, j in jobs_by_code.items():
            new_jobs_state[code] = {
                "jobId": j["jobId"],
                "title": j.get("title"),
                "address": (j.get("gps") or {}).get("address"),
            }

        all_our_shift_ids = [s["id"] for group in by_item.values() for s in group]
        all_our_job_ids = [j["jobId"] for j in jobs_by_code.values()]

        summary = {
            "jobs_found": len(jobs_by_code),
            "shifts_found": len(shifts),
            "shifts_with_parsed_item_id": sum(len(g) for g in by_item.values()),
            "unique_items": len(by_item),
            "duplicate_groups": duplicates,
            "duplicate_shift_count": sum(len(v) for v in duplicates.values()),
            "all_our_shift_ids": all_our_shift_ids,
            "all_our_job_ids": all_our_job_ids,
            "applied": False,
        }

        if apply:
            state = _load_state()
            state["shifts"] = new_shifts_state
            state["jobs"] = new_jobs_state
            _save_state(state)
            summary["applied"] = True

        return summary

    return await asyncio.to_thread(_run)


@app.get("/debug/shift-lookup")
async def debug_shift_lookup(ids: str, token: str | None = None):
    """ids = comma-separated Connecteam shift IDs. Returns title/time/notes
    for each, so duplicates can be reviewed before deletion."""
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")
    id_list = [i for i in ids.split(",") if i]

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY}
        out = []
        with httpx.Client(timeout=30) as client:
            for sid in id_list:
                resp = client.get(
                    f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts/{sid}",
                    headers=headers,
                )
                if resp.status_code == 404:
                    out.append({"id": sid, "found": False})
                    continue
                resp.raise_for_status()
                s = resp.json()["data"]
                out.append({
                    "id": sid,
                    "found": True,
                    "title": s.get("title"),
                    "startTime": s.get("startTime"),
                    "endTime": s.get("endTime"),
                    "creationTime": s.get("creationTime"),
                    "jobId": s.get("jobId"),
                    "notes": [n.get("html") for n in s.get("notes", [])],
                })
        return {"shifts": out}

    return await asyncio.to_thread(_run)


@app.get("/debug/service-types")
async def debug_service_types(token: str | None = None, max_orders: int = 60):
    """Scan the most-recently-updated Order-state opportunities and return
    every distinct Service line-item name seen, with an example order and a
    count. Read-only; touches Current RMS only."""
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        names: dict[str, dict[str, Any]] = {}
        orders_scanned = 0
        with httpx.Client(timeout=30) as client:
            page = 1
            while orders_scanned < max_orders:
                resp = client.get(
                    f"{CURRENT_RMS_BASE_URL}/api/v1/opportunities",
                    headers=rms_headers(),
                    params={
                        "q[state_eq]": 3,
                        "per_page": 25,
                        "page": page,
                        "sort": "-updated_at",
                    },
                )
                resp.raise_for_status()
                body = resp.json()
                opps = body["opportunities"]
                if not opps:
                    break
                for o in opps:
                    if orders_scanned >= max_orders:
                        break
                    orders_scanned += 1
                    try:
                        services = fetch_service_items(client, o["id"])
                    except httpx.HTTPStatusError:
                        continue
                    for s in services:
                        entry = names.setdefault(
                            s["name"],
                            {"count": 0, "example_order": o.get("number"), "example_subject": o.get("subject")},
                        )
                        entry["count"] += 1
                meta = body.get("meta", {})
                if page * meta.get("per_page", 25) >= meta.get("total_row_count", 0):
                    break
                page += 1

        return {
            "orders_scanned": orders_scanned,
            "distinct_service_names": len(names),
            "services": names,
        }

    return await asyncio.to_thread(_run)


@app.get("/debug/schedulers")
async def debug_schedulers(token: str | None = None):
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY}
        with httpx.Client(timeout=30) as client:
            resp = client.get(f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers", headers=headers)
            resp.raise_for_status()
            return resp.json()

    return await asyncio.to_thread(_run)


@app.get("/debug/jobs-list")
async def debug_jobs_list(token: str | None = None):
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY}
        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
                headers=headers,
                params={"instanceIds": CONNECTEAM_SCHEDULER_ID, "limit": 500, "offset": 0},
            )
            resp.raise_for_status()
            return resp.json()

    return await asyncio.to_thread(_run)


@app.get("/debug/shift-custom-fields")
async def debug_shift_custom_fields(token: str | None = None):
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY}
        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/custom-fields/shifts",
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()

    return await asyncio.to_thread(_run)


@app.post("/debug/create-test-jobs")
async def debug_create_test_jobs(token: str | None = None, prefix: str = "TEST "):
    """One-off: create a Connecteam Job (category) for each distinct Current
    RMS Service line-item name, named "<prefix><service name>", in whichever
    scheduler CONNECTEAM_SCHEDULER_ID currently points at. Skips names that
    already have a matching-titled Job (idempotent-ish safety net)."""
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY, "Content-Type": "application/json"}
        with httpx.Client(timeout=30) as client:
            # 1. Pull the authoritative Service catalog from Current RMS
            # (System Setup > Services), not just names seen on recent orders.
            names: set[str] = set()
            page = 1
            while True:
                resp = client.get(
                    f"{CURRENT_RMS_BASE_URL}/api/v1/services",
                    headers=rms_headers(),
                    params={"per_page": 100, "page": page, "q[active_eq]": "true"},
                )
                resp.raise_for_status()
                body = resp.json()
                for s in body["services"]:
                    names.add(s["name"])
                meta = body.get("meta", {})
                if page * meta.get("per_page", 100) >= meta.get("total_row_count", 0):
                    break
                page += 1

            # 2. Existing job titles in this scheduler, to skip duplicates.
            existing_titles: set[str] = set()
            offset = 0
            while True:
                resp = client.get(
                    f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
                    headers=headers,
                    params={"instanceIds": CONNECTEAM_SCHEDULER_ID, "limit": 500, "offset": offset},
                )
                resp.raise_for_status()
                jb = resp.json()
                jobs = jb.get("data", {}).get("jobs", [])
                for j in jobs:
                    if j.get("title"):
                        existing_titles.add(j["title"])
                if len(jobs) < 500:
                    break
                offset = jb.get("paging", {}).get("offset", offset + 500)

            # 3. Create one Job per new service name.
            created: list[str] = []
            skipped: list[str] = []
            errors: list[dict[str, Any]] = []
            for name in sorted(names):
                title = f"{prefix}{name}"
                if title in existing_titles:
                    skipped.append(title)
                    continue
                resp = client.post(
                    f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
                    headers=headers,
                    json=[{
                        "instanceIds": [int(CONNECTEAM_SCHEDULER_ID)],
                        "title": title,
                        "assign": {"type": "both", "userIds": [], "groupIds": []},
                    }],
                )
                if resp.status_code >= 400:
                    errors.append({"title": title, "status": resp.status_code, "body": resp.text[:300]})
                    continue
                created.append(title)

        return {
            "scheduler_id": CONNECTEAM_SCHEDULER_ID,
            "distinct_service_names_found": len(names),
            "created": created,
            "skipped_already_existed": skipped,
            "errors": errors,
        }

    return await asyncio.to_thread(_run)


@app.delete("/debug/shifts")
async def debug_delete_shifts(ids: str, token: str | None = None):
    """ids = comma-separated Connecteam shift IDs to permanently delete."""
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")
    id_list = [i for i in ids.split(",") if i]

    def _run() -> dict[str, Any]:
        headers = {"X-API-KEY": CONNECTEAM_API_KEY}
        deleted: list[str] = []
        errors: list[dict[str, Any]] = []
        with httpx.Client(timeout=30) as client:
            for sid in id_list:
                resp = client.request(
                    "DELETE",
                    f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts/{sid}",
                    headers=headers,
                )
                if resp.status_code >= 400:
                    errors.append({"id": sid, "status": resp.status_code, "body": resp.text[:500]})
                    continue
                deleted.extend(resp.json().get("data", {}).get("deletedShiftIds", [sid]))
        return {"deleted_count": len(deleted), "deleted": deleted, "errors": errors}

    return await asyncio.to_thread(_run)
