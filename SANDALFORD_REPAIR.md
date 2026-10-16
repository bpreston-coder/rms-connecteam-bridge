# 4071 coordinated repair — review stage only

No production execution is authorized by this document. The helper currently
refuses to plan or apply because `DELETE_IDS` is empty. Obtain the ten exact IDs
from the approved Mac report, insert them into the code allowlist and independently
review the resulting manifest. Never infer them from a date range.

The only remote writes permitted are PUTs to the retained delivery and collection
and DELETEs to those ten IDs. No creates, notifications, source changes, operator
updates, generic cleanup or ordinary sync are called. `notifyUsers=false` is sent
on PUT. Existing staffing, publication, custom fields and manual notes are checked
again after writes. Arrival-only explanatory notes are appended to preserved notes.

The helper reads the complete source service listing, rejects inactive targets,
requires exact source times/order, checks unique ownership and snapshots all 4071
mappings. It verifies the missing operator is absent, the approved replacement
has its exact identity/assignment/job/time/quantity, and the other operator remains
unchanged. All ten deletion rows must be mapped exclusively to the two source
transport items, unpublished and unassigned.

Before a future execution, deploy the reviewed hold-aware poller with separate
authorization. `/readyz` must report `opportunity_hold_policy_version: 1`. The
helper checks the running process at `127.0.0.1:$PORT` (default 10000), not just
the staged Python module. Never stage the helper against the old process and
assume it respects a hold. Do not restart during the ongoing Mac repair.

Future operator workflow, only after allowlist and deployment review:

1. Run `python sandalford_repair.py > /tmp/4071-review.json` on the production
   host using the same state file, credentials and environment as the service.
   This acquires the shared lock and performs reads only. Protect the report:
   it includes source details, crew assignments and manual notes, but no API keys.
2. Independently review the entire plan, including exact original/final times,
   ownership, published/assigned state and each deletion ID. Record its digest.
3. Pass the exact file and digest to
   `python sandalford_repair.py --plan-file /tmp/4071-review.json --apply-digest DIGEST`.
   All preconditions are read again before the first operation. A changed plan
   stops without writes. Once started, the durable hold and journal survive
   every exception. Each operation persists intent first, reads the exact ID
   again, and requires exact post-write read-back. Resume with the same file
   and digest; never delete the journal to force a retry.
4. Completion reports `verified_held`. All destination read-backs must pass before
   one atomic state save collapses the transport split keys to `125886` and
   `125887` and replaces only the missing operator ID under `126320`.

**Hold release is intentionally not implemented.** Before release, independently
verify no unresolved intents, exact destination/source/ownership convergence,
and that a read-only normal-sync plan would not recreate or alter unintended
shifts. Ordinary sync must not be used as a probe. A separate reviewed release
step is required; this helper never temporarily removes the hold.

Polling remains `POLL_INTERVAL_SECONDS=300`. Approved target 90 is deferred during
repair and pending actual quota verification. Connecteam's published account-wide
limits are Expert 100/minute and 10,000/day; Enterprise 200/minute and 20,000/day;
SBP 5/minute and 100/day. See
https://developer.connecteam.com/docs/rate-limiting-1 . Inspect only the six
`x-ratelimit-minute-*`/`x-ratelimit-day-*` response headers when checking the actual
account allowance; never print credentials. At a persistent 21 Connecteam calls
per cycle and 12 seconds of work, a 90-second wait estimates 17,788 calls/day,
before webhooks, daily reconciliation or other integrations. Repair may reduce
that retry workload; measure again before changing cadence.
