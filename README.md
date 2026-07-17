# Current RMS → Connecteam draft-shift bridge

When a Current RMS opportunity is **converted to an order**, this service automatically
creates one **draft shift** in Connecteam for each dated Service line item on that order —
titled `<Opportunity name> — <Service name>`, using the service's start/end time, and linked
to a Connecteam **Job** that carries the Current RMS order number (in the Job's "Job No." /
code field) and the order's venue address.

If a Service item's name or times are edited *after* the order was converted, the matching
Connecteam shift is updated in place on the next sync — it's never duplicated.

Draft shifts are not published or assigned to anyone; they just appear in the Connecteam
scheduler for your team to review, assign, and publish.

## How it works

1. Current RMS fires an `opportunity_convert_to_order` webhook to this app the moment a
   quotation is converted to an order, for an instant first sync.
2. A background poll also runs every 15 minutes (`POLL_INTERVAL_SECONDS`), checking every
   order updated since the last poll. This is what catches Service-item edits made *after*
   conversion — Current RMS has no "opportunity updated" webhook, so polling is the only way
   to notice those changes. You can also trigger a poll on demand: `GET /sync?token=...`.
3. Either path fetches the order's line items and keeps only the ones where
   `item_type == "Service"` and both a start and end time are set (e.g. "TRANSPORT - TRUCK
   UP TO 9T - DELIVERY", "Lighting technician", etc.). Group/header rows and rental products
   are ignored.
4. If the order has a venue set, the app looks up the venue's address via Current RMS's
   member record.
5. It finds or creates a Connecteam **Job** for the order: `code = <order number>` (this is
   what fills the "Job No." box), `gps.address = <venue address>`, `title = "<opportunity
   subject> (<order number>)"`. If the venue address later changes, the Job is updated in
   place.
6. **Idempotent shift sync, keyed by Current RMS opportunity_item ID:**
   - First time an item is seen → a draft shift is **created** (`isPublished: false`),
     linked to the Job above.
   - If a shift already exists for that item → it's **updated in place** (same shift, new
     title/time) only if something actually changed; otherwise nothing is sent.

   This mapping is remembered in `processed_orders.json`, so no combination of webhook
   retries, overlapping polls, or re-processing can ever create a duplicate shift for the
   same line item.

## Keeping the 15-minute poll running

The background poll only fires while the app process is alive. Render's **free** web service
tier spins down after ~15 minutes of no incoming HTTP traffic, which would silently stop the
poll. Two ways to keep it reliable:

- **Upgrade to a paid Render instance type** (e.g. Starter) so the service never sleeps —
  the built-in scheduler then just runs continuously. This is what's configured for this
  deployment.
- Or, on the free tier, have something external hit `GET /sync?token=...` (or even just
  `/healthz`) every 15 minutes — a Render Cron Job, or a free uptime-ping service — to both
  wake the app and trigger the sync.

## Prerequisites

- A place to run a small always-on Python web service (a VPS, Fly.io, Render, Railway,
  a Raspberry Pi on your network with a reverse proxy — anything that gives it a stable,
  reachable HTTPS URL). Current RMS needs to be able to POST to it.
- Python 3.11+.

## 1. Get your API credentials

**Current RMS**
- Subdomain: the part before `.current-rms.com` in your login URL (yours is `eavp01`).
- API key: System Setup → Integrations → API → generate a key.
- Webhooks must be enabled first: System Setup → Company Information → enable Webhooks.

**Connecteam**
- Log in → Settings → API Keys → Add API key.
- Scheduler ID: run `python setup_helpers.py list-schedulers` (step 3 below) once you have
  the API key, and pick the scheduler you want shifts created in.

## 2. Install

```bash
cd rms_to_connecteam
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env with your real values
```

Generate a webhook token (a shared secret so random internet traffic can't trigger the
endpoint — Current RMS webhooks aren't signed):

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Put it in `.env` as `WEBHOOK_TOKEN`.

## 3. Find your Connecteam scheduler ID

```bash
set -a; source .env; set +a
python setup_helpers.py list-schedulers
```

Put the right `schedulerId` into `.env` as `CONNECTEAM_SCHEDULER_ID`.

## 4. Run it

Locally, for testing:

```bash
set -a; source .env; set +a
uvicorn app:app --host 0.0.0.0 --port 8000
```

For production, run it behind a process manager (systemd, supervisor, or your host's
equivalent) so it restarts on crash/reboot, and put it behind HTTPS (a reverse proxy like
Caddy/Nginx, or your PaaS's built-in TLS). Current RMS will POST to this exact path:

```
POST https://your-domain.example.com/webhooks/current-rms/opportunity-converted?token=<WEBHOOK_TOKEN>
```

## 5. Register the Current RMS webhook

Once the app is deployed and reachable over HTTPS:

```bash
set -a; source .env; set +a
python setup_helpers.py create-webhook https://your-domain.example.com
```

This creates a Current RMS webhook for the `opportunity_convert_to_order` event pointing at
your app. Verify it with:

```bash
python setup_helpers.py list-webhooks
```

## 6. Test end to end

1. In Current RMS, take a test quotation with at least one dated Service line item and
   convert it to an order.
2. Check your app's logs — you should see `Processed opportunity <id>: {'status': 'ok', ...}`.
3. In Connecteam, open the scheduler you configured and confirm the draft shift(s) appear
   (they're only visible to admins/schedulers until published).

If nothing arrives, check:
- Current RMS webhook log (System Setup → Integrations → Webhooks → the webhook → Log) for
  the HTTP status your app returned.
- That your server's HTTPS URL is actually reachable from the public internet.
- The `token` query param matches `WEBHOOK_TOKEN`.

## Notes and limitations

- **24-hour shift cap**: Connecteam shifts can't exceed 24 hours. A Service item spanning
  longer than that (unusual, but possible for e.g. a multi-day dry-hire line) is skipped and
  logged — it won't silently get truncated.
- **Time zones**: Current RMS times are converted to UTC epoch seconds, which is what
  Connecteam's API expects; Connecteam will display them in the scheduler's own timezone.
- **Only "Service" items become shifts**: rental products and group/header rows are ignored.
  If you also want a shift per rental item, or want to key off a different set of item types,
  adjust the filter in `fetch_service_items()` in `app.py`.
- **Retries and duplicates**: Current RMS retries a failed webhook delivery up to 6 times
  over ~13 hours, and the 15-minute poll may also pick up the same order — none of this
  creates duplicates, since shifts are matched and updated by opportunity_item ID rather than
  re-created.
- **Reverted/re-converted orders**: if an order is reverted to quotation, its shifts are left
  as-is in Connecteam (they're not deleted automatically). If it's later re-converted, any
  Service items with the same IDs are updated, not duplicated.
- **Removed Service items**: if a Service item is deleted from an order after its shift was
  created, the shift is *not* automatically deleted from Connecteam — remove it manually if
  needed.
- **State file is per-deployment disk**: `processed_orders.json` lives on the web service's
  local disk. A full redeploy wipes it, so the next sync after a redeploy may re-create shifts
  it previously matched (it will fall back to Connecteam's own dedup where possible, e.g.
  jobs are matched by Job No. even after a state reset, but shift-level matching is lost).
