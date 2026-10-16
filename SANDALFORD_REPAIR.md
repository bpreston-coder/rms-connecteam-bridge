# 4071 coordinated repair — review stage only

No production execution is authorized by this document. The exact
ten approved IDs are frozen in `DELETE_BY_ITEM`, five each for delivery `125886`
and collection `125887`. Approval cannot transfer between source items. The two
retained transport IDs and all operator IDs are explicitly excluded. Independently
review the resulting live manifest before any future apply. Never infer targets
from a date range. The user separately approved saving only the dedicated repair
feature branch to the existing `bpreston-coder/rms-connecteam-bridge` repository
for review. Main merge and deployment still require separate authorization.

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
authorization. `/readyz` must report both `opportunity_hold_policy_version: 1`
and `sandalford_repair_policy_version: 2` (sanitizer-safe notes and amendment support). The
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

5. With the same plan file and digest, add `--check-release` for a read-only
   convergence review. It rechecks all ten deletions, both retained transports,
   both operators, source data, ownership, and the completed journal. Actual
   normal-sync logic then runs on a private state copy with remote mutations
   blocked and disk saves disabled only in that execution context. It must plan
   zero mutations and preserve mappings. Source/destinations are checked again.
6. After that review and execution authorization, add `--release-hold` instead
   to repeat all checks and atomically remove the hold while marking the journal
   released. A failure retains the durable hold. This never removes the real
   hold temporarily to run normal sync. Subsequent source edits use ordinary sync.

The appended arrival paragraph uses the visible prefix
`Bridge arrival (opportunity item #125886):` (or `#125887`). Later ordinary time
edits remove only that exact generated paragraph and regenerate the arrival note,
preserving manual text. Unowned pre-existing arrival text requires review.

## Resume the first collection PUT stopped by HTML sanitization

The original attribute-based manifest must not be regenerated, overwritten or
silently re-digested. Keep the original plan file and original review digest.
After separately approved deployment of policy 2:

1. Add `--review-amendment` to the original plan-file/apply-digest arguments.
   This performs GET-only checks. It requires the original in-progress journal,
   empty confirmations, and the collection as its sole current intent. It proves
   that collection matches the exact expected post-PUT row with only the known
   `data-rms-bridge-arrival="1"` attribute removed from the single appended
   paragraph. Every other field and all original/manual notes must match exactly.
   Delivery, all ten deletion candidates, operators, source and state must still
   match their reviewed preconditions.
2. Review the returned amendment and its separate digest. The collection amendment
   PUT contains only `shiftId` and `notes`; it replaces its proven generated
   paragraph with the visible ownership prefix. It does not replay the time update.
   Delivery uses the same visible prefix in its still-pending normal time update.
3. Apply/resume using the original plan file and original digest plus
   `--amendment-digest AMENDMENT_DIGEST`. The original plan/digest remain unchanged;
   a full original-journal snapshot and the reviewed amendment are saved durably
   before mutation. The old collection intent is recorded as reconciled. Later
   interrupted amendment writes resume by strict exact-ID read-back, never broad
   HTML normalization or blind retry. Both canonical read-backs must be confirmed
   and still match immediately before every deletion.
4. Use the ordinary check-release/release-hold steps with the original plan/digest.
   They verify the journaled amendment's final rows. Release requires the new
   running policy, ensuring future time edits replace the visible owned note.

No other legacy journal shape is eligible for this amendment. Any meaningful text,
manual content, assignment/publication, ownership or source drift retains the hold.

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

## Separate Crooked Colours location investigation

The source venue and target job address were reported empty, while a created shift
contains the literal address `"None"`. Do not accept that GPS as empty and do not
clear its validation hold. Future recovery creates omit `locationData` when the
source has no address, consistent with the optional field in the official
[create guide](https://developer.connecteam.com/docs/scheduler-create-shifts).
Read-back still rejects inherited location and any nonempty GPS for that case.

An existing-ID correction remains unimplemented pending the exact shift IDs,
source item IDs and current location JSON. The intended scope is one minimal
`shiftId`/`locationData` PUT per explicitly reviewed existing ID with
`notifyUsers=false`, under the shared lock and a durable hold/journal, preserving
and verifying all other fields. The official
[update guide](https://developer.connecteam.com/docs/scheduler-update-shifts)
confirms omitted fields are retained, but its location example only demonstrates
setting a nonempty address. It does not establish whether null GPS, an empty
object, or an empty address clears this account's existing location. No candidate
encoding is authorized for live experimentation. Obtain documented/support-confirmed
clearing behavior before finalizing this correction, then require exact read-back
with no GPS and unchanged assignment/publication/notes/times.

Field `1353115` is still included on write as an empty string when the source
description is empty. The narrow read-back comparison accepts its omission only
for that explicitly empty expected value. The create guide makes the overall
custom-fields array optional; this does not establish requiredness of this
account's individual field. A string type definition and omitted GET value do
not prove that requirement. No broader normalization or write omission is added.
