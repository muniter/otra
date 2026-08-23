# otra — agent guide

otra is a **generator-based durable execution engine on Postgres, built on
durable promises**. Tasks are TypeScript generator functions; every
suspension point (`yield*`) is a write-once promise row in Postgres; recovery
replays the generator from the top, fast-forwarding through the memoized
history ("¡otra!" — again). All coordination logic lives in stored functions
(`sql/schema.sql`); the TypeScript SDK (`sdks/typescript/src/`) is a thin
replay driver.

Read `README.md` first for the user-facing model. This file is for you: the
invariants you must not break, how this repo is developed, the reference
systems, and the decisions already made so you don't relitigate them.

## Load-bearing invariants (do not break these)

1. **Postgres owns coordination.** Claiming (`FOR UPDATE SKIP LOCKED`),
   timer/event/claim-expiry sweeps (inside `claim()` — there is no separate
   scheduler), retry backoff (computed in the failure transaction),
   suspension, waking, cancellation state — all in `sql/schema.sql` stored
   functions. Never move scheduling or locking logic into TypeScript.
2. **Single-author journal.** Only the owning execution creates promise rows
   in its history. Outside code may only *settle* designated promises:
   `external`-kind by unforgeable token, events by name. This is what keeps
   replay deterministic. (Resonate chose the opposite — globally creatable
   promises; we deliberately did not.)
3. **Everything that determines a replay's path must be journal.** That is
   why cancellation delivery is itself a promise row (kind `cancel`, key
   `$cancel`, recording *where* CancelledError was thrown) — so compensation
   can suspend and every replay re-delivers at the same yield, even if the
   forward promise settles later.
4. **Suspension is control flow, never an exception.** The driver simply
   stops resuming the generator. absurd's `throw SuspendTask` (swallowable by
   user `catch`, broken across `Promise.all`) is the failure mode this whole
   project exists to avoid.
5. **Wake-on-any + replay-re-suspend** is the correctness model: resolving
   any promise wakes a suspended owner; if it is still blocked, the replay
   re-parks. Cheap and race-free. The no-lost-wakeup guarantee rests on lock
   order: `suspend()` locks the execution row before checking blockers;
   resolvers lock promise row first, execution row second. On top of that,
   **every multi-row execution lock uses one global order: ascending
   `(root_id, id)`** — `_wake_local`, the cancel/kill tree walks (which
   never lock terminal rows), cleanup, and terminal transitions (which lock
   self + parent via `_lock_terminal_scope` before writing; uuid_v7 ids in
   the same millisecond are NOT parent-before-child, hence order-by-id, not
   tree order). Cross-root loops (event emits, claim sweeps) visit roots in
   `root_id` order. Proven under forced contention in
   `sdks/typescript/tests/locking.test.ts` (including an ABBA regression) —
   extend those tests if you touch lock order.
6. **Ownership guards on every history write.** `_assert_owner` raises
   sqlstate `OT001` (claim lost/zombie) or `OT002` (killed) on
   `record_run`/`create_sleep`/`create_event_wait`/`create_external`/
   parent-scoped `spawn`. The driver maps these to quiet `lost`/`killed`
   outcomes. A zombie worker must never write into a stolen history.
   Those two codes are part of a closed taxonomy every `raise exception`
   in `sql/schema.sql` belongs to — `OT003` invalid argument, `OT004` not
   found, `OT005` precondition failed, plus Postgres's own `40001` — set
   out in the "Error codes" block at the top of that file. Adding a raise
   means picking one of them (or writing down why it stays `P0001`), and
   the split between OT003 and OT005 is the test: OT003 is malformed on
   its face, OT005 is well formed but forbidden by current state.
7. **Events are immutable one-shot facts** per (queue, name): first write
   wins, repeat emits are no-ops, repeat waits return the same fact.
   Recurring signals derive names (`tick:${i}`) or use `ctx.promise`. We
   shipped retained-latest first and walked it back (absurd did the same,
   their commit `7b63b7a`); don't reintroduce mutability.
8. **The engine owns the terminal state after cancel delivery.** Catching
   `CancelledError` and returning normally still finalizes `cancelled`,
   never `completed` (Temporal's documented footgun, deliberately closed).
   Exhausted compensation retries finalize `cancelled`, never `failed`.
9. **`claimed_by` survives on terminal rows** — the forensic record for
   double-execution investigations (an absurd lesson). Don't "clean it up".
10. **Determinism guard:** promise keys are label-based with `#N` counters
    for repeats; a key re-encountered with a different kind/label raises
    `DeterminismViolationError` (non-retryable). Labels starting with `$`
    are engine-reserved (`$sleep`, `$event:*`, `$spawn:*`, `$promise`,
    `$cancel`) and `Ctx` REJECTS user-supplied `$`-labels — a user step
    keyed `$cancel` would occupy the cancellation journal and make the
    execution permanently un-cancellable. What this does and does not catch
    when task code changes under live executions — and the label/`#N`
    shifts it cannot see — is written up in
    `docs/replay-compatibility.md`; keep that doc honest if you touch
    `checkRecorded` or `keyFor`.
11. **Cancellation delivery never waits out a backoff** (`request_cancel`
    expedites pending rows; a failing attempt with an undelivered cancel
    retries immediately), **never splits a `ctx.uninterruptible` section**
    (a shielded park is legal via `suspend_local(p_shielded)`; delivery
    lands after the shield exits), and **a lost claim or kill() aborts
    `ctx.signal` from the heartbeat** so well-behaved long steps unblock.
12. **Partition maintenance must never wedge on the default partition.**
    `ensure_partitions` drains stranded default-partition rows into the new
    week partition (promises parked first so the execution move cannot
    cascade history away) and steps weeks through `week_bucket_utc` — plain
    `+ interval '7 days'` is session-time-zone arithmetic and skips a week
    across DST. One queue's maintenance failure must not poison the rest.

## How this repo is developed

- **TDD is the house rule.** Every behavior change lands as a failing test
  first; run it red, implement, run the full suite. Regression tests cite
  what motivated them (several cite absurd commits — keep that habit).
- **Tests run against real Postgres, on Node >= 24** (the runner uses
  `--test-global-setup`). `make test` starts a session-scoped
  Postgres 16 Testcontainer, applies the schema to a template database, and
  gives each test an isolated clone. Test files run eight-wide by default;
  `OTRA_TEST_CONCURRENCY` overrides the limit. Set
  `OTRA_TEST_DB=postgres://postgres@127.0.0.1:5433/postgres` to use an
  existing database instead; that mode runs serially and resets its `otra`
  schema per test. Root commands are `make install`, `make check`,
  `make build`, and `make test`.
- **Local Postgres quickstart** (any PG ≥ 14; as root you must run it as the
  postgres user):
  ```sh
  su postgres -c "/usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/devdata -U postgres --no-sync -A trust"
  su postgres -c "/usr/lib/postgresql/16/bin/pg_ctl -D /var/lib/postgresql/devdata \
    -o '-p 5433 -c fsync=off -c listen_addresses=127.0.0.1' -l /var/lib/postgresql/pg.log start"
  ```
- **Deterministic time:** the clock is `otra.now()`, backed by a config
  *table* (not a GUC, unlike absurd) so every pooled connection sees the
  same fake time without a max-1 pool workaround. Tests freeze it
  (`select otra.set_fake_now(...)`) and step it
  (`select otra.advance_fake_now(interval ...)`); workers are driven manually
  with `worker.tick()` / `worker.drain()` — never `sleep()` and hope.
- **Wakeups are LISTEN/NOTIFY-driven** (`src/wake.ts`): one lazy dedicated
  `LISTEN otra_wake` connection per app (application_name `otra-listen`),
  shared by workers and `getResult`. NOTIFY is best-effort, so three
  fallbacks keep it sound: a reconnect emits a `null` reset wake ("poll
  once, you may have missed something"), idle workers sleep until
  `otra.next_due_local(queue)` (earliest timer/retry/lease-expiry/deadline —
  clock-driven work never notifies), and `pollIntervalMs` remains a slow
  safety-net poll (60s default when listening). If you add a state
  transition that makes work runnable NOW, it must `pg_notify('otra_wake',
  <queue name>)` — via `otra._notify_wake` (never `pg_notify` directly:
  the wrapper honors the `set_wake_notifications` off switch). The whole
  layer is a LATENCY OPTIMIZATION, never a correctness dependency —
  liveness must hold with notifications disabled (there is a test), and a
  disconnected hub makes workers poll fast rather than trust a dead wire.
- **Gates, not sleeps:** async coordination in tests uses `EventEmitter` +
  `once(gate, "release")` rendezvous (a pattern taken from absurd's TS
  suite) and a `waitFor(condition)` poller
  (`sdks/typescript/tests/helpers.ts`).
- **Race tests:** force the interleaving with two `pg.Client`s — session A
  holds a row lock in an open transaction, session B blocks, the test
  *observes* the block via `pg_stat_activity.wait_event_type = 'Lock'`,
  then commits A and asserts convergence. See
  `sdks/typescript/tests/locking.test.ts` and the cancel-vs-suspend test. Use
  this for any new concurrency claim.

## References: keep these within reach

**absurd** — the parent project and the single best reference. Same
philosophy (SQL owns coordination), production mileage, and a commit history
full of paid-for lessons. **Clone it for exploration:**

```sh
git clone https://github.com/earendil-works/absurd /tmp/absurd
```

What to mine there: `sql/absurd.sql` (their entire engine, one file),
`tests/test_lock_ordering.py` (the deadlock-test technique),
`tests/conftest.py` (fake_now + libfaketime), and `git log` — when you're
adding a feature, check whether absurd already shipped and then *fixed* it.
Lessons already absorbed into otra: event race `bcde0df`, first-write-wins
events `7b63b7a`, retry-delay overflow `866480d`, zombie guards (`AB002`
family), claim-expiry-as-failure `47e6710`, bounded sweeps, idempotent spawn.

**otra's own development history** lives in the fork it was born in:
`muniter/absurd`, branch `claude/durable-execution-generators-pvthqt` —
commit-by-commit TDD rounds, hardening passes, and the design rationale for
everything below. `docs/cancellation-design.md` in this repo is the full
cancellation survey and design.

**Theory** — Jack Vanlightly's series (the conceptual foundation; "errors
propagate up, cancellations propagate down the tree"):
- https://jack-vanlightly.com/blog/2025/11/24/demystifying-determinism-in-durable-execution
- https://jack-vanlightly.com/blog/2025/12/4/the-durable-function-tree-part-1
- https://jack-vanlightly.com/blog/2025/12/4/the-durable-function-tree-part-2
- https://jack-vanlightly.com/blog/2025/12/10/the-three-durable-function-forms

**Peer systems** — study before designing any new surface; the taste bar is
"smallest API that covers the scenarios" (Resonate is the benchmark):
- **Resonate** — durable promises as THE concept; user-chosen promise IDs
  double as address + idempotency key. https://docs.resonatehq.io and the
  durable promise spec in github.com/resonatehq/async-rpc.io.
- **Restate** — awakeables (`{id, promise}`, settle by id), named workflow
  promises, leaf-first cancellation with first-class compensation.
  https://docs.restate.dev (see /guides/sagas and the graceful-cancellations
  blog post).
- **Temporal** — the incumbent: cancellation scopes, ParentClosePolicy,
  activity heartbeat cancellation + AbortSignal. https://docs.temporal.io.
- **DBOS** — the deliberate contrast: status-flip cancel, uncatchable by
  design, resumable because no compensation ran. https://docs.dbos.dev.

## Decisions already made (don't relitigate without new evidence)

- **Events = facts; targeted signals = `ctx.promise` tokens.** Two scopes of
  one concept, matching where Restate independently landed.
- **Design C (named promise addresses `{execution, name}` + per-execution
  FIFO inbox) is designed and deliberately deferred** — it is the *sessions*
  form in disguise. The full design with scenarios is in the fork's
  history; build it only when sessions become an explicit decision.
- **Stateless functions only** (Vanlightly's first form). No per-execution
  signal/query handlers, no actors — each is a decision, not a feature.
- **Cancellation**: request-flag (`cancel_requested_at`), never a status
  flip; discovery via the heartbeat return; journaled delivery; suspending
  compensation shipped. `kill` is the no-compensation escape hatch.
- **Queue storage is named after the queue, not its UUID** (`x_orders`,
  `p_orders`, `x_orders_202601`) — absurd's scheme, chosen for operability:
  `\dt`, `pg_locks`, autovacuum logs and EXPLAIN all become readable, where
  `x_019bc186de00…` was opaque. `otra.queues.id` stays as the SDK's
  routing/token identity (`ExecutionRoute`, promise tokens are unchanged).
  The price, accepted: queue **names are immutable** —
  `_protect_queue_storage_identity` refuses renames alongside `id` and
  `storage_mode`, so renaming means drop-and-recreate — and capped at 54
  bytes (longest generated identifier is a week partition,
  `x_<name>_<IYYYIW>` = N + 9 ≤ 63; the arithmetic is in the comment above
  `validate_queue_name`). Two collision guards, which the UUID scheme did not
  need: names ending in `_<6 digits>` or `_d` are rejected outright (they
  would shadow another queue's partitions), and `create_queue` refuses to
  provision over an existing relation instead of letting
  `create table if not exists` adopt it. Names are never sanitized — every
  generated identifier is `%I`-quoted, so spaces, case and non-ASCII are
  legal. See `docs/queue-storage-design.md`.
- **Concept budget**: six concepts (task, execution, durable promise,
  handle, event, queue/worker). Anything new must fit an existing row or
  justify a seventh. No `race` combinator until a real use case (losers keep
  running — needs a semantics note first). **No scheduler primitive** either
  (absurd's call too): recurring work is an external scheduler plus a
  slot-derived `idempotencyKey`, documented in `docs/cron.md`.

## Known backlog (in rough priority order)

1. Cancellation remaining tiers: leaf-first finalization (parent waits for
   descendants before compensating), preemptible runs (AbortSignal into the
   step fn, result discarded — DBOS-style), operator pause/resume as a
   distinct `paused` status (sound only because compensation didn't run).
2. `TaskError` rename/cleanup (watchlist: exists only to mark
   non-retryable; consider `NonRetryableError`).
3. History growth: no continue-as-new equivalent yet; long-looping tasks
   replay O(history). `docs/cron.md` tells users to avoid durable sleep
   loops for exactly this reason — revisit it when continue-as-new lands.
4. npm publish (the name `otra` was free as of 2026-08).

## Sharp edges to keep in mind

- `ctx.run` side effects are **at-least-once** (crash between execute and
  record re-runs them). Idempotency keys downstream are the user's job;
  `spawn` idempotency and write-once checkpoints are the engine's.
- `ctx.cancelRequested` outside a `catch` is nondeterministic input across
  replays (same class as `Date.now()`); branching forward logic on it is a
  user error — the delivered `CancelledError` is the deterministic signal.
- The schema file is idempotent (`create or replace` + targeted
  `drop function` for signature changes) but there is **no migration story**
  for altering existing tables — `create table if not exists` won't alter.
  That is deliberate pre-release freedom, written down above
  `otra.schema_version()` (which reports `'main'` until release automation
  stamps a tag): the upgrade procedure is drop-and-reapply, and migrations
  become mandatory the moment a tag exists to migrate from. User-facing
  changes accumulate under `# Unreleased` in `CHANGELOG.md`.
- Retained facts expire with `cleanup()`'s TTL; an untimed wait registered
  after expiry parks until a live emit. Untimed external promises park until
  settled or killed.
