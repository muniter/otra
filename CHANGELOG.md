# Changelog

This contains the changes between releases.

otra has not been released yet, so there are no version sections below:
numbering begins at the first release. The database schema is pre-release in
the same sense. `sql/schema.sql` is idempotent and `otra.schema_version()`
reports `main`, but there is deliberately **no migration story** — the
supported way to pick up a schema change is to drop the `otra` schema and
reapply the file, which is only acceptable because nobody is yet running a
schema we owe compatibility to. Migrations become mandatory at the first
tagged release, which is also when `otra.schema_version()` starts reporting
something other than `main`.

# Unreleased

## Added

* Five new testing layers raise confidence from example-based to
  property-based: fast-check value properties over the public seams;
  `otra.check_invariants(queue)`, a read-only oracle that returns
  violations of the engine's cross-table rules (lost wakeups, lost
  settlements, claim/terminal/attempt incoherence) instead of describing
  them in prose; a crash/replay fuzzer that generates random task
  programs as data plus injected fault schedules, then proves the
  defining property of the engine on each: fault-free-reference results,
  exactly one journal row per step, exactly `failures + 1` real
  executions per step function, exact-once child spawns, and a silent
  oracle — with fast-check shrinking any failure to a minimal program;
  a pairwise serializability differential that fires every pair of
  coordination calls concurrently on identical fixtures and asserts the
  outcome equals one of the two serial orders (never a deadlock); and a
  multi-worker chaos harness — three workers, force-expired leases,
  cancels and kills at random rounds — with the oracle run between every
  round, not just at quiescence. A nightly workflow soaks the fuzz,
  chaos and property layers at depths CI cannot afford.

* Wakeups are now LISTEN/NOTIFY-driven. Every app shares one lazy dedicated
  `LISTEN otra_wake` connection; idle workers park on it and wake the moment
  a spawn, event, settlement, cancellation, retry, or completion commits —
  end-to-end latency drops from the poll interval to single-digit
  milliseconds. Clock-driven work never notifies, so an idle worker instead
  sleeps exactly until `otra.next_due_local(queue)` (the earliest pending
  timer, retry, lease expiry, or deadline), and the polling loop survives
  only as a slow safety net (60s default while listening). Terminal
  transitions notify too, so `getResult` returns the instant an execution
  finishes instead of polling out its backoff. The listener names itself
  `otra-listen` in `pg_stat_activity`, reconnects with backoff if its
  connection dies, and treats every (re)connect as a missed-notification
  hazard by polling once immediately.

  Notifications are strictly a latency optimization, never a correctness
  dependency: a worker whose listener cannot connect degrades to a fast
  poll instead of silently keeping the long fallback, `getResult` waiters
  filter wakes by queue and coalesce bursts, and
  `otra.set_wake_notifications(false)` turns the whole layer off for
  extreme-throughput deployments (NOTIFY-ing transactions serialize
  briefly at commit), leaving a plain polling system.

* Queue storage can now be reclaimed: `app.dropQueue(name, { force })` drops a
  queue and every physical relation named after it. It refuses while any
  execution is still non-terminal — those workers would hit a vanished table
  mid-replay — unless you force it.
* `app.cleanup(queue?, { ttl, limit })` runs the retention sweep: finished
  execution trees whose root settled past the TTL, and event facts older than
  it. Omitting the queue sweeps **every** queue, each under its own stored
  policy. Every call returns what it deleted per queue, and those counts are
  worth reading — each batch is bounded, so a `rootsDeleted` equal to the
  limit means the sweep saturated and owes another pass. A scheduler that
  ignores them falls quietly behind on any queue retiring more trees per day
  than its `cleanupLimit`.
* `app.dropDetachedPartition(table)` completes the partition retirement flow
  that `app.listDetachCandidates()` starts: you run the `DETACH` (PostgreSQL
  will not do it concurrently from inside a function), this reclaims the
  storage. Every safety gate is a distinct error rather than a silent no-op —
  the relation must exist in the `otra` schema, be named like a partition otra
  generates, and actually be detached.
* `app.retry(ref, { maxAttempts })` resumes a permanently failed **root**
  execution in place. The journal is kept, so replay fast-forwards through
  every settled step and only the work after the failure point runs again.
  Retrying a child is refused (its parent has already observed the write-once
  child promise reject — retry the root), and so are `completed` and
  `cancelled` executions.
* Execution deadlines: `maxDelaySeconds` and `maxDurationSeconds` on
  `spawn` / `ctx.spawn`, validated at spawn time. A blown deadline is a
  *graceful* cancellation, not a kill, so `catch`/`finally` compensation runs
  exactly as it does for an operator cancel.
* `ctx.uninterruptible` sections may now suspend — sleep, await a child, wait
  on an event — without a pending cancellation splitting the critical section
  in half. Delivery lands after the shield exits.
* An error-code taxonomy callers can branch on instead of matching error text.
  `OT004` (not found) and `OT005` (precondition failed) join `OT001` (claim
  lost) and `OT002` (killed); `OT003` narrows to genuinely invalid arguments.
  The SDK exports `isNotFound` and `isPreconditionFailed` alongside
  `isClaimLost` and `isKilled`. Every `raise` in the schema now carries a code
  from that set, or a written-down reason it does not.
* `otra.schema_version()` and `app.schemaVersion()` report which build of the
  schema a database has: `main` during development, the tag once release
  automation stamps one.
* `app.spawn` now returns `created` alongside the execution address, so an
  idempotent spawn can tell a fresh schedule from a deduplicated one. This is
  what a cron or webhook caller needs to log "scheduled" versus "already
  scheduled"; the field is additive, so existing destructuring is unaffected.
* Two guides: [docs/replay-compatibility.md](docs/replay-compatibility.md) on
  changing task code with executions in flight (what the determinism guard
  catches, and the silent hazards it cannot), and [docs/cron.md](docs/cron.md)
  on recurring work without a scheduler primitive — external scheduler plus a
  slot-derived idempotency key, with the exact dedup semantics, backfill and
  overlap policies.

## Changed

* **Queue storage is named after the queue, not its UUID.** Tables and indexes
  are now `x_orders`, `p_orders`, `x_orders_202601`, `xi_orders_ri` and so on,
  which makes `\dt`, `pg_locks`, autovacuum logs and `EXPLAIN` output readable
  where `x_019bc186de00…` told an operator nothing. Two prices, paid
  deliberately: queue **names are immutable** (renaming means drop and
  recreate) and capped at 54 bytes. Names ending in `_<6 digits>` or `_d`
  are rejected because they would shadow another queue's partition names, and
  provisioning refuses to adopt a pre-existing relation that already claims a
  generated name. The SDK-facing API is unchanged — `queues.id` remains the
  routing and token identity.
* `app.cleanup()` with no queue name now sweeps every queue rather than only
  the app's own, matching `ensurePartitions()` and the underlying SQL. Name
  the queue explicitly if you want the old single-queue behavior.
* `ctx.now()` reads the database clock instead of memoizing the worker's
  `Date.now()`. Worker and database clocks drift, and every timer in otra runs
  on the database clock; now the value a task sees agrees with the one its
  sleeps are measured against.
* `ctx.spawn` honors the child task's registered `maxAttempts` and
  `retryStrategy`. Previously a child of a task registered with a retry policy
  silently got the SQL defaults, while `app.spawn` of the same task did not.
* Emitting and settling now report what happened instead of succeeding
  silently. `app.emitEvent` returns whether *this* call created the fact (a
  repeat emit, including one carrying a different payload, changes nothing and
  says so), and `app.resolvePromise` / `app.rejectPromise` distinguish a benign
  repeat settle (`false`) from a token that names no external promise at all
  (an error — that is a bug, not a lost race).
* Retry backoff is jittered by up to +25% before its absolute caps, so a fleet
  of executions knocked over by one downstream outage stops retrying in
  lockstep. A zero backoff stays exactly zero.
* Unknown-function defers are jittered deterministically per execution instead
  of a flat 15 seconds, so a fleet deferring the same batch during a rolling
  deploy stops re-colliding on the same instant.
* `getResult` backs off exponentially to a one-second ceiling instead of
  polling at a flat interval (a 30-second wait was up to 1200 round trips),
  rejects invalid options with `TypeError`, and expires with a typed
  `TimeoutError`.
* `app.close()` stops the workers it created, draining their in-flight drives,
  before ending a pool it owns — rather than yanking connections out from
  under running executions and stranding their claims for a full lease.
* `app.spawn` of an **unregistered** task name now requires an explicit
  `{ queue }`. A typo'd name used to spawn silently with SQL defaults and then
  cycle in the unknown-function defer loop forever with no diagnostic; an
  explicit queue marks a deliberate cross-process spawn.
* Durable values are constrained to JSON structurally, at every boundary:
  task params and results, `ctx.run` results, and the explicit type arguments
  of `waitForEvent` and `ctx.promise`. Interfaces, optional properties and
  arrays of interfaces are accepted; `Date`, `Map`, `Set`, `bigint`, functions
  and class instances are rejected with a named reason instead of a bare "not
  assignable to never". A task typed as returning a `Date` was handing its
  caller a string after replay.
* `otra.ensure_partitions(name)` on an unpartitioned queue is an error rather
  than a silent no-op.
* The test suite requires Node >= 24 and now says so with an actionable
  message rather than an opaque "bad option" from the runtime, and gained
  EXPLAIN-plan regression tests that assert the hot claim, sweep, cleanup and
  event-lookup statements still reach the indexes built for them — a class of
  regression no correctness test can see, because only the plan rots.

## Fixed

* **A worker whose claim was swept could spin through 100 wasted replays.**
  `otra.suspend_local` answered "don't park" identically for a settled
  blocker (replay now) and a stolen claim, and a re-parking replay is often
  fully memoized — so the zombie never hit an ownership guard and redrove
  itself until the worker's replay cap threw a spurious operator error,
  precisely when the system was already degraded. A stolen claim now raises
  `OT001` (and a killed execution `OT002`) from the park itself, which the
  driver reports as `lost`/`killed`. Found by the multi-worker chaos harness.
* **`_backoff` accepted `{"max_s": "NaN"}`.** PostgreSQL sorts `NaN` above
  every other float8, so the `max_s >= 0` guard passed and a NaN cap was
  silently reinterpreted as the one-day hard cap; it is now rejected with
  `OT003` like every other malformed strategy field. Found by fast-check
  (`base_s` and `factor` were already saved by their upper bounds).
* **A jsonb-hostile string in a step result was a poison pill.** A `U+0000`
  or a lone UTF-16 surrogate in any checkpointed value made the journal
  write fail on every replay, wedging the claim until its lease expired,
  forever. Step results are now sanitized (hostile code points become
  U+FFFD) and, as a backstop, a Postgres data error (class 22) from a
  journal write records a permanent, readable failure instead of escaping.
  Found by fast-check.
* **`parseDuration` accepted a long-enough digit string.** 309 nines
  followed by `"s"` overflowed `Number()` to `Infinity` and sailed through —
  the exact value the numeric branch refuses. Both branches now reject
  non-finite results. Found by fast-check.
* **Cancellation could sit undelivered for a whole backoff.** Requesting a
  cancel now expedites an execution parked on retry backoff, and a failing
  attempt with an undelivered cancel retries immediately. Compensation
  retries, once the cancellation is journaled, keep their backoff.
* **`kill()` was invisible to a running step.** Losing a claim — stolen lease
  or an operator kill — now aborts `ctx.signal` from the heartbeat, so a
  well-behaved long step unblocks instead of running to completion against an
  execution that no longer exists, and the two cases are reported distinctly
  (killed versus lost).
* **Heartbeat failures were swallowed.** An unreachable database used to be
  ignored indefinitely while the lease expired elsewhere and the drive kept
  running user code. A drive with no successful heartbeat for a full lease now
  trips a watchdog, abandons locally and aborts `ctx.signal`; a step finishing
  after its claim vanished no longer checkpoints its result.
* **A user step could make an execution permanently un-cancellable.** Labels
  beginning with `$` are engine-reserved, and `Ctx` now rejects user-supplied
  ones: a step keyed `$cancel` would occupy the cancellation journal.
* **Partition maintenance could wedge a queue permanently.** Rows stranded in
  the default partition while maintenance lapsed made creating the next week's
  partition fail forever. New week partitions now drain the matching default
  rows first (promises parked before executions move, so the move cannot
  cascade history away). One queue's maintenance failure no longer poisons the
  rest of the run, and the multi-queue sweep no longer holds every queue's
  maintenance barrier in a single transaction.
* **A week of partition coverage vanished at every spring-forward.** Week
  stepping used session-time-zone `+ 7 days`, which lands inside the same ISO
  week across a DST transition; stepping is now pinned to UTC week buckets.
* **Terminal transitions could deadlock against cancellation walks.**
  Completing, failing or finalizing an execution used to lock child-then-parent
  while cancel and kill walks locked the tree in id order — a reproducible ABBA
  deadlock. Every multi-row execution lock now uses one global ascending
  `(root_id, id)` order; cross-root loops visit roots in `root_id` order;
  cleanup takes its candidate trees in that order and deletes promise rows
  before execution rows, matching the direction resolvers take.
* Retention sweeps and cleanup scans gained the indexes they were missing, and
  waking an execution only sends a notification when something was actually
  woken.
* Every `to_regclass('otra.' || name)` lookup was an unquoted identifier
  lookup — safe only while storage names were hex, and a latent hazard the
  moment they became user text. All of them are quoted now, and a queue named
  `Weird "Name" ñ` is exercised end to end.
* The SDK README quickstart and `examples/order.ts` never provisioned their
  queue and failed at runtime.
* Test runs leaked one database per test (~85 per suite) and could silently
  share a single database when the global setup had not run, producing dozens
  of incomprehensible failures instead of naming the real problem.
