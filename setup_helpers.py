"""
One-off helper commands for setting things up. Run with the same environment
variables as app.py loaded (e.g. `set -a; source .env; set +a` first).

Usage:
    python setup_helpers.py list-schedulers
        List your Connecteam schedulers and their IDs, so you can pick the
        right CONNECTEAM_SCHEDULER_ID.

    python setup_helpers.py create-webhook https://your-server.example.com
        Registers the Current RMS webhook that fires when an opportunity is
        converted to an order, pointing at your deployed app.

    python setup_helpers.py list-webhooks
        Show existing Current RMS webhooks (to check it registered, or to
        find the ID to delete/update).
"""

from __future__ import annotations

import os
import sys

import httpx

CURRENT_RMS_SUBDOMAIN = os.environ["CURRENT_RMS_SUBDOMAIN"]
CURRENT_RMS_API_KEY = os.environ["CURRENT_RMS_API_KEY"]
CURRENT_RMS_BASE_URL = os.environ.get("CURRENT_RMS_BASE_URL", "https://api.current-rms.com")
CONNECTEAM_API_KEY = os.environ.get("CONNECTEAM_API_KEY")
CONNECTEAM_BASE_URL = os.environ.get("CONNECTEAM_BASE_URL", "https://api.connecteam.com")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")


def rms_headers() -> dict[str, str]:
    return {
        "X-SUBDOMAIN": CURRENT_RMS_SUBDOMAIN,
        "X-AUTH-TOKEN": CURRENT_RMS_API_KEY,
        "Content-Type": "application/json",
    }


def list_schedulers() -> None:
    resp = httpx.get(
        f"{CONNECTEAM_BASE_URL}/scheduler/v1/schedulers",
        headers={"X-API-KEY": CONNECTEAM_API_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    for s in resp.json()["data"]["schedulers"]:
        flag = " (archived)" if s["isArchived"] else ""
        print(f"{s['schedulerId']}\t{s['name']}{flag}\t{s['timezone']}")


def create_webhook(target_base_url: str) -> None:
    target_url = target_base_url.rstrip("/") + "/webhooks/current-rms/opportunity-converted"
    if WEBHOOK_TOKEN:
        target_url += f"?token={WEBHOOK_TOKEN}"

    resp = httpx.post(
        f"{CURRENT_RMS_BASE_URL}/api/v1/webhooks",
        headers=rms_headers(),
        json={
            "webhook": {
                "name": "Push draft shifts to Connecteam on order conversion",
                "event": "opportunity_convert_to_order",
                "target_url": target_url,
                "active": True,
            }
        },
        timeout=30,
    )
    print(resp.status_code, resp.text)
    resp.raise_for_status()


def list_webhooks() -> None:
    resp = httpx.get(f"{CURRENT_RMS_BASE_URL}/api/v1/webhooks", headers=rms_headers(), timeout=30)
    resp.raise_for_status()
    for w in resp.json().get("webhooks", []):
        print(f"{w['id']}\t{w['name']}\t{w['event']}\t{w['target_url']}\tactive={w['active']}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "list-schedulers":
        list_schedulers()
    elif cmd == "create-webhook":
        if len(sys.argv) != 3:
            print("Usage: python setup_helpers.py create-webhook https://your-server.example.com")
            sys.exit(1)
        create_webhook(sys.argv[2])
    elif cmd == "list-webhooks":
        list_webhooks()
    else:
        print(__doc__)
        sys.exit(1)
