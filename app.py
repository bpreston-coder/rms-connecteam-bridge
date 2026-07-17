"""
Current RMS -> Connecteam draft shift bridge.

When a Current RMS opportunity is converted to an order, this service:
  1. Receives the `opportunity_convert_to_order` webhook from Current RMS.
  2. Looks up (or creates) a Connecteam Job for the order, with the Current
     RMS order number in the Job's "Job No." (code) field and the order's
     venue address in the Job's address field.
  3. Fetches the order's Service line items (item_type == "Service") which
     carry a title (name) and a start/end time.
  4. Creates one DRAFT shift per service in Connecteam, titled
     "<Opportunity subject> — <Service name>", linked to that Job (so it
     carries the Job No. and address), using the service's start/end time.

See README.md for setup instructions.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
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

# Shared secret appended to the webhook target URL as ?token=... to stop
# randoms on the internet from POSTing fake "convert to order" events at us.
# Current RMS webhooks have no signing, so this query-string token is the
# only gate — keep the URL itself private too.
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")

# Where we remember which opportunities we've already pushed, so Current
# RMS's webhook retries (it retries failed deliveries for ~13 hours) don't
# create duplicate shifts.
STATE_FILE = Path(os.environ.get("STATE_FILE", "./processed_orders.json"))

MAX_SHIFT_SECONDS = 24 * 60 * 60  # Connecteam: a shift can't exceed 24h.

app = FastAPI(title="Current RMS -> Connecteam bridge")


# ---------------------------------------------------------------------------
# Small persistent "already processed" set
# ---------------------------------------------------------------------------

def _load_processed() -> set[int]:
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text()))
        except (json.JSONDecodeError, OSError):
            log.warning("Could not read state file %s, starting fresh", STATE_FILE)
    return set()


def _save_processed(ids: set[int]) -> None:
    STATE_FILE.write_text(json.dumps(sorted(ids)))


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


def find_or_create_job(client: httpx.Client, opportunity: dict[str, Any], address: str | None) -> str | None:
    """Find (by Job No. / code) or create the Connecteam Job for this order,
    so its "Job No." box holds the Current RMS order number and its address
    field holds the venue address. Returns the Connecteam jobId, or None if
    the order has no number to key off of."""
    number = opportunity.get("number")
    if not number:
        return None

    headers = {"X-API-KEY": CONNECTEAM_API_KEY, "Content-Type": "application/json"}

    # Look for an existing job with this Job No. first, so re-runs (retries,
    # re-processing) don't create duplicate jobs.
    resp = client.get(
        f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
        headers=headers,
        params={"jobCodes": number, "instanceIds": CONNECTEAM_SCHEDULER_ID},
    )
    resp.raise_for_status()
    existing = resp.json().get("data", {}).get("jobs", [])
    if existing:
        return existing[0]["jobId"]

    subject = opportunity.get("subject") or f"Order {number}"
    job_payload: dict[str, Any] = {
        "instanceIds": [int(CONNECTEAM_SCHEDULER_ID)],
        "title": f"{subject} ({number})",
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
    return resp.json()["data"]["jobs"][0]["jobId"]


def build_draft_shifts(
    opportunity: dict[str, Any], services: list[dict[str, Any]], job_id: str | None
) -> list[dict[str, Any]]:
    subject = opportunity.get("subject") or f"Order {opportunity.get('number', opportunity['id'])}"
    shifts = []
    skipped = []

    for service in services:
        start = _to_epoch_seconds(service["starts_at"])
        end = _to_epoch_seconds(service["ends_at"])

        if end <= start:
            skipped.append((service["name"], "end time is not after start time"))
            continue
        if end - start > MAX_SHIFT_SECONDS:
            skipped.append((service["name"], "duration exceeds Connecteam's 24h shift limit"))
            continue

        shift: dict[str, Any] = {
            "startTime": start,
            "endTime": end,
            "title": f"{subject} — {service['name']}",
            "isPublished": False,  # draft shift
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
            # Link to the Job so the shift carries its Job No. and address.
            shift["jobId"] = job_id
            shift["locationData"] = {"isReferencedToJob": True}
        shifts.append(shift)

    if skipped:
        for name, reason in skipped:
            log.warning("Skipped service '%s': %s", name, reason)

    return shifts


def push_draft_shifts(client: httpx.Client, shifts: list[dict[str, Any]]) -> dict[str, Any]:
    url = f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers/{CONNECTEAM_SCHEDULER_ID}/shifts"
    headers = {"X-API-KEY": CONNECTEAM_API_KEY, "Content-Type": "application/json"}

    created: list[Any] = []
    # Connecteam accepts up to 500 shifts per call; chunk defensively.
    for i in range(0, len(shifts), 500):
        chunk = shifts[i : i + 500]
        resp = client.post(url, headers=headers, json=chunk, params={"notifyUsers": "false"})
        if resp.status_code >= 400:
            log.error("Connecteam rejected shift batch: %s %s", resp.status_code, resp.text)
            resp.raise_for_status()
        created.extend(resp.json().get("data", {}).get("shifts", []))

    return {"created_count": len(created), "shifts": created}


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_opportunity(opportunity_id: int) -> dict[str, Any]:
    with httpx.Client(timeout=30) as client:
        opportunity = fetch_opportunity(client, opportunity_id)

        if opportunity.get("state_name") != "Order":
            return {
                "status": "skipped",
                "reason": f"opportunity state is '{opportunity.get('state_name')}', not 'Order'",
            }

        services = fetch_service_items(client, opportunity_id)
        if not services:
            return {"status": "skipped", "reason": "no dated Service line items found"}

        address = fetch_venue_address(client, opportunity)
        job_id = find_or_create_job(client, opportunity, address)

        shifts = build_draft_shifts(opportunity, services, job_id)
        if not shifts:
            return {"status": "skipped", "reason": "all service items were skipped (see logs)"}

        result = push_draft_shifts(client, shifts)
        return {"status": "ok", **result}


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

@app.post("/webhooks/current-rms/opportunity-converted")
async def opportunity_converted(request: Request, token: str | None = None):
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")

    payload = await request.json()
    action = payload.get("action", {})

    # Current RMS convert_to_order fires with subject_type "Opportunity".
    if action.get("subject_type") != "Opportunity":
        return JSONResponse({"status": "ignored", "reason": "not an Opportunity action"})

    opportunity_id = action.get("subject_id")
    if opportunity_id is None:
        raise HTTPException(status_code=400, detail="action.subject_id missing")

    processed = _load_processed()
    if opportunity_id in processed:
        log.info("Opportunity %s already processed, skipping duplicate webhook", opportunity_id)
        return JSONResponse({"status": "skipped", "reason": "already processed"})

    try:
        result = process_opportunity(opportunity_id)
    except httpx.HTTPStatusError as exc:
        log.exception("Upstream API error while processing opportunity %s", opportunity_id)
        raise HTTPException(
            status_code=502,
            detail=f"upstream error: {exc.response.status_code} {exc.response.text[:500]}",
        ) from exc

    if result["status"] == "ok":
        processed.add(opportunity_id)
        _save_processed(processed)

    log.info("Processed opportunity %s: %s", opportunity_id, result)
    return JSONResponse({"opportunity_id": opportunity_id, **result})


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "time": int(time.time())}


@app.get("/debug/job")
async def debug_job(code: str, token: str | None = None):
    """Temporary diagnostic route: look up a Connecteam Job by its Job No.
    (code) to verify it was created correctly. Remove after verifying."""
    if WEBHOOK_TOKEN and not hmac.compare_digest(token or "", WEBHOOK_TOKEN):
        raise HTTPException(status_code=403, detail="invalid or missing token")
    with httpx.Client(timeout=30) as client:
        resp = client.get(
            f"{CONNECTEAM_BASE_URL}/jobs/v1/jobs",
            headers={"X-API-KEY": CONNECTEAM_API_KEY},
            params={"jobCodes": code, "instanceIds": CONNECTEAM_SCHEDULER_ID},
        )
        resp.raise_for_status()
        return resp.json()
