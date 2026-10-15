"""Complete pagination and pure payload builder for isolated recovery only.

Do not wire these readers into the live poller without a separate rollout review.
"""
from __future__ import annotations
from datetime import datetime
import html
from typing import Any
import httpx
from rms_pagination import fetch_pages
from app import (
    CURRENT_RMS_BASE_URL, CURRENT_RMS_STATE_ORDER, CURRENT_RMS_STATE_QUOTATION,
    EXCLUDED_SERVICE_NAMES, PERTH_TZ, CONNECTEAM_JOBNO_CUSTOM_FIELD_ID,
    CONNECTEAM_QTY_CUSTOM_FIELD_ID, CONNECTEAM_SHIFT_TYPE_CUSTOM_FIELD_ID,
    RecoveryRequired, rms_headers, _transport_arrival_time,
)


def _fetch_rms_pages(client, path, collection, params=None):
    rows, _ = fetch_pages(client, f"{CURRENT_RMS_BASE_URL}{path}", rms_headers(), collection, params)
    return rows


def fetch_opportunities_updated_since(client: httpx.Client, since_iso: str) -> list[int]:
    """Return IDs of every opportunity in 'Order' OR 'Quotation' state that's
    been updated since since_iso. Used by the background poll to catch
    Service-item edits made after conversion, and Quotation-state
    opportunities that had "Draft shifts in Connecteam" ticked (or
    unticked/gone dead/lost — sync_opportunity() decides eligibility and
    handles cleanup either way)."""
    ids: set[int] = set()
    for state_id in (CURRENT_RMS_STATE_ORDER, CURRENT_RMS_STATE_QUOTATION):
        rows = _fetch_rms_pages(client, "/api/v1/opportunities", "opportunities", {
            "q[state_eq]": state_id, "q[updated_at_gteq]": since_iso,
            "sort": "-updated_at",
        })
        ids.update(row["id"] for row in rows)
    return sorted(ids)


def fetch_service_items(client: httpx.Client, opportunity_id: int) -> list[dict[str, Any]]:
    """Read every item page before filtering Services (including undated ones)."""
    items = _fetch_rms_pages(client,
        f"/api/v1/opportunities/{opportunity_id}/opportunity_items", "opportunity_items")

    return [
        item
        for item in items
        if item.get("item_type") == "Service"
        and item.get("name") not in EXCLUDED_SERVICE_NAMES
    ]


def _service_shift_content(opportunity, service, start, end, job_id, job_color,
                           address, site_contact, subject):
    """Pure fields shared by ordinary sync and reviewed item-only recovery."""
    opportunity_id = opportunity["id"]
    order_number = opportunity.get("number")
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

    return title, quantity, description, arrival_note, desired, custom_fields, notes
