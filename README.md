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


## Google Chat change alerts (October 2026)

Chat reports operational changes to published shifts with scheduled work remaining:
changes to task, time, title, quantity, address, notes or site contact; removal of
previously active work; and an OnRent opportunity/service transition to `Cancelled`.
The latter asks ops to review Connecteam rather than claiming the shift was deleted:
this notification update does not alter the existing synchronization rules.

Completed opportunities/service lines, shifts whose old and new schedules have ended,
unchanged values, color-only refreshes and sibling/bookkeeping-only updates are silent.
Cancellation uses the prior active tracked state, so the new inactive status does not
hide a relevant alert. Completed/cancelled notification history is stored alongside
shift tracking. Legacy tracked entries are treated as previously active until observed.

Timing uses the existing epoch-second shift segments (including Perth daily splitting),
not an opportunity-wide date or the Connecteam Job, which represents a task type.
Both old and new edit times are considered. Mixed jobs are filtered per shift.
In-progress shifts and legacy entries with unknown dates retain alerts pending an
explicit decision to suppress those cases. Nothing here changes crew publication,
assignment, API failure handling, or the actual shift create/update/delete behavior.

Offline regression tests (no live credentials or messages):

```bash
python -m unittest discover -s tests -v
```

## Equal-time transport arrivals

For the ten exact delivery/collection Service names listed in
`TRANSPORT_ARRIVAL_SERVICE_NAMES` (the reviewed Truck 8 Ton and Van 1 Ton
variants), identical source start/end instants become a one-minute Connecteam
slot: the original start is retained and the end is extended by exactly 60
seconds. This also applies to Day rate transport milestones and midnight
rollover. OnRent data is never modified.

The generated note says, for example, “On site at 2:45pm sharp (AWST). The
one-minute slot represents arrival only, not work duration.” It is replaced
with the bridge-owned paragraph when the arrival changes, or removed when
the source becomes a normal duration; manual notes are retained. Tracking
includes this note so a change between an arrival milestone and an actual
one-minute duration is detected even when destination times are identical.

Unlisted names are not inferred from descriptions or destination job names.
Non-transport equal times, reversed intervals, completed items, normal
durations, and existing identity/layout safety rules keep their prior handling.

Arrival-instruction changes are included in operational edit notifications,
including a transition to/from an ordinary one-minute duration with identical
destination times. Existing published/active/work-remaining gates still apply;
unchanged polls remain silent.

### Targeted catch-up after deployment (procedure only)

Previously skipped orders may not have changed since the poll cursor. `/sync`
only triggers the incremental poll, and daily reconciliation is report-only;
neither guarantees these orders will be synced.

1. First verify each candidate opportunity ID with read-only source calls
   (`fetch_opportunity` and `fetch_service_items`), confirming the subject,
   eligibility, status, exact transport names and equal times. Do not infer an
   event name from an ID such as 4104. Audit the entire opportunity using
   `reconcile_report` and inspect tracked IDs and any pending creates before
   approving a targeted sync: this sync processes all its services, including
   existing update/cleanup and notification behavior.
2. After deployment and approval, POST to the existing protected
   `/webhooks/current-rms/opportunity-converted` route with JSON
   `{"action":{"subject_type":"Opportunity","subject_id":<verified ID>}}`.
   Supply the configured webhook token securely without printing it or placing
   it in shell history. This invokes the existing sync guard and durable
   checkpoint/retry path for that opportunity, without resetting the poll cursor
   or editing source data. Check the response for errors before proceeding.
3. Read back the state under the sync guard, then use `get_shift` for every
   recorded destination ID for the affected items. Verify original start,
   end=start+60, the AWST arrival-only note, source-item marker, job, publication
   status and preserved assignments. Compare source dates again to the preflight
   snapshot. Run the GET-only `reconcile_report` as an additional check; it does
   not validate arrival-note text, so explicit destination note inspection is
   required. Do not create synthetic shifts to validate this procedure.

## Scoped four-item recovery with compatible polling (review before live use)

This correction supersedes the unsafe recovery-only rollout proposed in
`2b785e8`: an unchanged old poller could delete mapped tail-page items or mutate
validation-held records. **Do not apply the helper against deployed c423d9c.**
The CLI now requires `app.ITEM_RECOVERY_POLICY_VERSION == 1`.

### Compatibility patch and mutation boundaries

- Item reads in `app.fetch_service_items` now exhaust actual rows through the
  shared `rms_pagination.py` reader. Incomplete, overlapping, changing-total or
  capped reads raise before orphan cleanup. It never equates a 25-row response
  to a complete listing simply because 100 rows were requested.
- The reader also records which items the old item-pagination stopping rule
  would have exposed. Newly revealed **untracked** Services are report-only:
  no create, update, delete or notification. Their IDs appear in
  `newly_revealed_item_ids`; they require a separate rollout review. Already
  tracked items, including the four validated transport recoveries, remain
  visible and can continue their ordinary lifecycle after full reads.
- Broader opportunity discovery remains exactly the deployed c423d9c function.
  Its pagination correction stays in `recovery_support.py` for later review;
  it is not connected to normal polling.
- Opportunity **3950 (Production Management)** is explicitly held before any
  pending-create reconciliation, source-driven update, cleanup or notification.
  The shared deletion routine also excludes it.
- Any mapped or pending `recoveryValidationPending` record holds its entire
  opportunity before ordinary sync does anything, including source deletion,
  cancellation and completion paths. The shared cleanup routine independently
  skips validation-held records. Only the recovery helper's successful strict
  read-back clears the flag; ordinary polls cannot bypass it.

The new policy is local until separately approved and deployed. It changes no
live settings or credentials. Normal behavior for previously visible items
continues except for the explicit 3950 and validation-quarantine holds.

### Smallest safe reviewed rollout

1. Review and approve the compatibility patch and recovery code/tests together.
2. Deploy only the compatible `app.py` plus `rms_pagination.py` first, retaining
   existing `bridge_safety.py`, `log_redaction.py`, requirements, settings, state
   and disk. Verify `/readyz` reports `item_recovery_policy_version: 1` from
   the running process (other recovery issues may still make readiness degraded). No manual/global sync is needed.
   This deployment is a prerequisite, not an action performed by this review.
3. Stage matching `transport_recovery.py` and `recovery_support.py` from the
   same reviewed commit. Run in the deployed project working directory and
   use that exact deployed `app`, `rms_pagination`, state file and lock path.
   Importing these modules does not start the scheduler.
4. Establish the full relevant scheduler history and future horizon through
   authenticated GET-only inventory. Select explicit epoch-second scan bounds
   covering that range and run the dry-run:

   ```bash
   python -m transport_recovery --scan-start <epoch-start> --scan-end <epoch-end>
   ```

5. Review the four source subjects, item names/times, payloads, actions, bounds
   and digest. Only **4104/126172**, **4104/126173**, **4095/125805**, and
   **4095/125847** are writable. Confirm the approved Crooked Colours and Master
   Painter transport items from source, not guessed event names.
6. After the dry-run review, apply that exact digest with the same bounds:

   ```bash
   python -m transport_recovery --scan-start <epoch-start> --scan-end <epoch-end> --apply-digest <reviewDigest>
   ```

The helper uses the normal process/file lock, preflights all four targets before
any create, and never calls whole-opportunity sync. It creates unassigned drafts
with `notifyUsers=false` through durable intents/mappings. It does not alter
staffing, update/delete shifts, send Chat messages, edit source, or reset poll,
retry or reconciliation state. A changed source specification or scan range
invalidates the digest.

Fresh-create read-back validates times/title/job, full generated notes and
identity, custom fields, location, supplied color, publication=false and empty
assignments. Failure saves the identity with a durable validation hold and stops
further creates. Once validated, later human publication/assignment is preserved
on repeated helper runs. Ambiguous POSTs are never blindly repeated. A pending
marker can be reconciled or remains held; a partial apply can safely resume from
its durably stored identities.

### Scan coverage and pagination limits

The former ±1-day scan could miss moved shifts. The helper now exhausts all
pages in the explicitly reviewed range without job/title/publication filters,
checking target source markers across that range and holding unmarked same-time
lookalikes. Empty terminal pages establish exhaustion; overlaps and a 200-page
cap stop writes. Pending reconciliation still uses the existing durable-create
reader's ±1-day lookup: a discrepancy can stop recovery but never replays POST.

Connecteam requires date bounds; see
[Getting Shifts](https://developer.connecteam.com/docs/scheduler-get-shifts).
Rows outside those bounds, stripped source markers, invisible records, and
concurrent human/API edits remain limitations. Review inventory coverage before
apply; local locking serializes bridge processes, not outside users.

The complete OnRent reader checks actual unique row counts, advertised totals,
short/empty pages, overlap and a 200-page cap. No live inventory, settings change,
manual sync, helper apply or deployment occurs as part of preparing these files.
