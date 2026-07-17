# Current RMS → Connecteam draft-shift bridge

When a Current RMS opportunity is **converted to an order**, this service automatically
creates one **draft shift** in Connecteam for each dated Service line item on that order —
titled `<Opportunity name> — <Service name>`, using the service's start/end time.

Draft shifts are not published or assigned to anyone; they just appear in the Connecteam
scheduler for your team to review, assign, and publish.

## How it works

1. Current RMS fires an `opportunity_convert_to_order` webhook to this app whenever a
   quotation is converted to an order.
2. The app fetches the order and its line items, and keeps only the ones where
   `item_type == "Service"` and both a start and end time are set (e.g. "TRANSPORT - TRUCK
   UP TO 9T - DELIVERY", "Rigger call", etc. — anything you've priced as a Service in Current
   RMS with dates on it). Group/header rows and rental products are ignored.
3. For each of those, it POSTs a draft shift to Connecteam:
   `title = "<opportunity subject> — <service name>"`, `startTime`/`endTime` from the
   service's dates, `isPublished: false`.
4. It remembers which opportunity IDs it has already processed (in `processed_orders.json`)
   so Current RMS's automatic webhook retries don't create duplicate shifts.

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
  over ~13 hours. `processed_orders.json` prevents duplicate shift creation on retry. If you
  ever need to reprocess an order (e.g. after fixing a bug), remove its ID from that file.
- **Re-conversions**: if an order is reverted to quotation and re-converted, it will be
  treated as already processed and skipped. Delete its ID from `processed_orders.json` if you
  want it to run again.
