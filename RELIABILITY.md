# Reliability release — 14 October 2026

Previous live revision: 3c56aaf (notification filtering; 20 offline tests).

## Behavior

Tracking state now uses atomic replacement, file/directory fsync, a previous valid
backup (.bak), and a one-time pre-hardening snapshot. Corrupt state stops writes.
A process/file lock serializes poll and webhook work. Create intent (including a
unique note marker) is saved before POST; success commits each returned identity.
An ambiguous timeout never blindly repeats POST: a unique marker match is adopted,
otherwise the opportunity is held for review. Explicit validation/auth rejections
can be retried after correction. Updates remain idempotent; incomplete responses
leave previous tracked values for retry. Deletes checkpoint each accepted ID.

Every opportunity is queued durably before work. Failed opportunities are retried
on later polls independently of cursor advancement. A bounded daily reconciliation
of tracked jobs reports source deletion and states outside the incremental filter.
It performs GETs only; historical repairs are deferred pending review. Ordinary
incremental changes and durable retries continue to synchronize.
GET-only transient retries and per-poll Jobs/member caches reduce redundant calls.
The shared TLS context avoids repeatedly parsing CA certificates; it is not proof
that the observed long-running memory growth is fixed.

Completed opportunities/items retain their shift history, including old split keys
and missing dates. Midnight end times are exclusive when splitting long day-rate
services. Existing split-key layout changes are held for a reviewed migration;
assignments are never lost by an automatic delete/recreate. Sibling discovery uses
source-item note identity and previous timestamps, with pagination and ownership
checks, rather than mutable descriptions/job labels. Existing multiple-owner
identities stop the affected opportunity before sync. Missing tracked destinations
are held without dropping mappings or automatically recreating records.

Updates preserve manual HTML notes and omit assignment/publication fields. API
custom-field updates target only the three bridge-owned fields. Non-HTML notes
are held because this endpoint only documents HTML input. An absent source address
clears the previous bridge-owned address only if the live value still agrees;
manual overrides are held. A missing mapped Job is held rather than silently
claiming the old destination Job was cleared.

Cancelled status remains non-destructive pending the owner's explicit policy:
existing shifts are held and upcoming published-work alerts retained. Postponed
work is held. Dead/Lost, unticked eligible quotations and deleted source work keep
their established cleanup policy; completed history is protected. In-progress work
retains alerts. Equal-time arrivals retain exact source intent but remain held
until Connecteam acceptance/display semantics are verified. No guessed duration.

## Migration and rollout

No bulk rewrite, purge, new credentials, service tier change or scheduler change.
State schema version 2 adds optional recovery/health fields while retaining existing
shift mappings. The first save creates a pre-hardening snapshot on the persistent
disk before changes. Backups contain operational data and remain on that disk.

Read-only first-pass audit: 72 tracked entries / 10 jobs; all eligible, no orphan
source items, no affected midnight day-rate items. Full planner audit is available
via `python tools/read_only_plan.py`; all outbound mutations are replaced and the
transport refuses any HTTP method other than GET. It never sends Google Chat.

## Validation

`python -m unittest discover -s tests -q`
Tests cover notification semantics plus atomic-write interruption, corruption,
retry cursor retention, ambiguous/partial creates, marker adoption after source
removal, incomplete updates, completed/missing-date/split-layout protection,
manual note/assignment preservation, address ownership, source 404 cleanup,
exclusive midnight boundaries, exact sibling identity, paging and GET-only retry.

`/healthz` is liveness. `/readyz` exposes only aggregate counts and poll time,
returning 503 for unreadable tracking state, pending creates/retries or stale polls.
Review recorded exceptions even if readiness is otherwise healthy.

## Recovery and rollback

Do not delete tracking state or blindly restore an old snapshot: accepted remote
creates/deletes may be newer than that snapshot. Stop sync before recovery, compare
backup mappings with live IDs and pending markers, then adopt only verified records.
An unresolved zero-match create needs an operator to prove the POST was rejected
before clearing its pending intent. Multiple matches require explicit duplicate
review; there is no automatic purge.

Code rollback: redeploy 3c56aaf with the current state file retained. If any pending
create/retry exists, pause and resolve it first: the older code does not understand
those recovery queues. Do not run two independently stored bridge instances against
the same scheduler. The local file lock applies only to a shared persistent file.

## Staged release decision

The initial full planner proposed three recreations (3830 and 4070) and five
updates (3830). Historical logs showed the old sibling matcher cross-associating
destination IDs across daily keys. None of these repairs is applied by this
release. Daily reconciliation is report-only, stored in reconciliation_reports
and summarized by /readyz. Review reports even when the normal poll is healthy.
Automatic historical repair and explicit Cancelled deletion remain deferred.
There is no bulk mapping migration. Missing/ambiguous records retain their IDs.
