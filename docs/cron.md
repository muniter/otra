# Recurring work without a scheduler

Assumes otra at **pre-release `main`** — `app.spawn` /
`SpawnOptions.idempotencyKey` in `sdks/typescript/src/app.ts`, and
`otra.spawn_local` in `sql/schema.sql`.

otra has no schedule primitive: no `app.schedule("0 2 * * *", task)`, no cron
rows in the database, no recurring-execution concept. That is a decision, not
a gap waiting to be filled. The concept budget is six — task, execution,
durable promise, handle, event, queue/worker — and a schedule would be a
seventh that every operating system, container platform and cloud already
provides. absurd made the same call for the same reason.

What otra does provide is the piece those schedulers lack: **at-most-once
spawning per time slot**. Wire the two together and you have cron without a
scheduler in the engine.

## The pattern

An external scheduler — crontab, a systemd timer, a Kubernetes `CronJob`,
Cloud Scheduler, GitHub Actions, whatever already exists — runs a small
process that calls `app.spawn` with an **idempotency key derived from the task
name and the time slot**:

```typescript
import { Otra } from "otra";

const app = new Otra({ db: process.env.DATABASE_URL, queue: "jobs" });

/** UTC minute slot: 2026-08-23T02:00 */
function slot(at: Date, granularityMinutes: number): string {
  const ms = granularityMinutes * 60_000;
  return new Date(Math.floor(at.getTime() / ms) * ms).toISOString().slice(0, 16);
}

const now = new Date();
const key = `cron:rebuild-search-index:${slot(now, 24 * 60)}`;

// The task lives in the worker process, not here, so name it as a string and
// pass the queue explicitly -- app.spawn refuses an unregistered task name
// otherwise, precisely to catch typos.
await app.spawn(
  "rebuild-search-index",
  { scheduledFor: slot(now, 24 * 60) },
  { queue: "jobs", idempotencyKey: key },
);

await app.close();
```

Run that every five minutes if you like; run three replicas of it; restart it
mid-deploy. Every call inside the same slot collapses onto one execution.

## What idempotent spawn actually guarantees

Verified against `otra.spawn_local`:

- **At most one top-level execution per `(queue, key)`.** In `unpartitioned`
  queues that is a partial unique index on `x_<queue>(idempotency_key)`; in
  `partitioned` queues it is a separate `i_<queue>` table with the key as
  primary key, mapping to `(root_id, execution_id)`. Different queues do not
  share a key space.
- **The loser gets the winner's address back, and knows that it lost.** A
  duplicate call returns the same `ExecutionRef` — `{ queueId, rootId,
  executionId }` — with no error and no second row, and a `created` flag
  saying which side of the race it was on: `true` when this call is what put
  the execution there, `false` when the slot was already taken. That is the
  difference between logging "scheduled 2026-08-23" and "2026-08-23 already
  scheduled", and it saves the scheduler from diffing execution ids against
  its own records to find out:

  ```typescript
  const { executionId, created } = await app.spawn(
    "rebuild-search-index",
    { scheduledFor: slot(now, 24 * 60) },
    { queue: "jobs", idempotencyKey: key },
  );
  if (!created) return;                 // another replica already has this slot
  ```
- **The loser's arguments are discarded.** Params, `maxAttempts`,
  `retryStrategy`, `delaySeconds` all come from whichever call won. Do not
  encode anything the run needs into a call that might lose.
- **Races are handled in the database**, not by read-then-write: the insert is
  `on conflict do nothing` followed by a locked re-read. If the winning row
  has been deleted by retention in that window, the call raises sqlstate
  `40001` (`concurrent idempotent spawn was cleaned up; retry`). Treat `40001`
  as retryable in the scheduler process.
- **Keys are not kept forever.** `cleanup` deletes the execution tree and its
  idempotency registration together, so once a slot's tree ages past the
  queue's retention TTL (default 30 days), that key spawns fresh again. This
  only matters for backfills, below.

Child spawns need none of this: `ctx.spawn` is deduplicated by the parent's
promise key, so `idempotencyKey` exists only on top-level `app.spawn`.

## Choosing the slot granularity

The slot is the unit of deduplication, so it must match the **schedule's
period, not the scheduler's tick**:

- A daily job gets a date slot (`2026-08-23`), even if the scheduler process
  runs hourly and skips 23 of those runs.
- A five-minute job gets a five-minute slot, floored — not "now rounded to the
  minute", which would produce five distinct keys per intended run.
- Never put `Date.now()`, a pod name, or a random value in the key. That
  silently turns dedup off, and you will only find out when a deploy overlap
  double-charges someone.

Compute slots in **UTC**. Local-time slots repeat an hour every autumn and
skip one every spring, which means one duplicate run and one missed run per
year, found the hard way.

For long or awkward inputs, hash them — the key is just text:

```typescript
import { createHash } from "node:crypto";

function cronKey(taskName: string, expr: string, slotIso: string): string {
  const digest = createHash("sha256").update(`${taskName}|${expr}|${slotIso}`).digest("hex");
  return `cron:${taskName}:${digest.slice(0, 24)}`;
}
```

Keep the task name readable in the key even when hashing the rest; the keys
show up in `x_<queue>` when you are debugging at 3am.

## Catch-up: a missed slot stays missed

There is no schedule state in the database, so nothing knows a slot was
skipped. If the scheduler was down from 02:00 to 06:00, those runs did not
happen and never will unless you ask for them:

```typescript
for (const missed of missedSlots) {          // slots you decided to replay
  await app.spawn(
    "rebuild-search-index",
    { scheduledFor: missed, backfill: true },
    { queue: "jobs", idempotencyKey: `cron:rebuild-search-index:${missed}` },
  );
}
```

Because the key is the same one the live scheduler would have used, a backfill
that overlaps a slot which *did* run is a no-op — as long as that slot's tree
still exists. Past the retention TTL the registration is gone and the backfill
really does re-run the work, so bound backfills to the retention window, or
make the task itself idempotent for its slot.

Two related habits: pass the slot into the task's params (a task that reads
the wall clock cannot tell a backfill from a live run), and use
`delaySeconds` if you want to place a run at a future instant precisely —
`spawn` sets `run_after = now() + delay`, so a scheduler that wakes early can
still land the execution on the slot boundary.

## Overlap: a run that outlives its slot

Nothing stops slot N+1 from starting while slot N is still working. Decide
explicitly:

**Skip while the previous run is unfinished.** Persist the previous slot's
full `ExecutionRef` — all three fields; `getExecution` needs the queue and
root ids for partition-pruned routing — and check it first:

```typescript
const previous = await loadRef("rebuild-search-index");      // your own storage
if (previous !== null) {
  const snapshot = await app.getExecution(previous);
  const busy =
    snapshot !== null &&
    (snapshot.status === "pending" ||
      snapshot.status === "running" ||
      snapshot.status === "suspended");
  if (busy) return;                    // this slot is deliberately dropped
}
```

If you have nowhere to persist the ref, re-spawning with the *previous* slot's
key returns that execution's ref without creating anything — but only while
the tree is inside retention, after which it would spawn a duplicate of an old
slot. Persisting the ref is safer.

**Or let them overlap** and make the task tolerate it: take a Postgres
advisory lock in the first `ctx.run`, or have the run detect that its slot's
work is already done and return early. Overlap is often fine; what is never
fine is discovering the policy by accident.

## Why not a durable sleep loop

The tempting alternative is one immortal task:

```typescript
// Don't do this.
const ticker = app.task("ticker", function* (_params: null, ctx) {
  while (true) {
    yield* ctx.run("work", async () => doWork());
    yield* ctx.sleep("1h");
  }
});
```

It is durable, it survives restarts, and it degrades badly:

- **History grows without bound.** Every iteration appends promise rows
  (`work#2`, `$sleep#2`, `work#3`, …), and every replay loads the whole
  journal before doing anything. After a month at one tick an hour, each wake
  reloads ~1,500 rows to run one step. otra has no continue-as-new equivalent
  — it is a known backlog item, not a solved problem — so nothing truncates
  that history for you.
- **It maximizes replay-compatibility exposure.** A task alive for months
  crosses every deploy you ship. One relabeled step is enough to break it; see
  [replay-compatibility.md](replay-compatibility.md).
- **It concentrates failure.** Exhaust `maxAttempts` on one iteration and the
  whole recurring job is permanently `failed`, with no resume verb to bring it
  back. Independent per-slot executions fail one slot.

If you genuinely need self-scheduling (no external scheduler exists in your
environment), bound the loop: run a fixed number of iterations, then spawn a
successor execution and return. That is continue-as-new done by hand, and it
keeps each journal small.

## Doing it inside Postgres with pg_cron

If the database already runs [`pg_cron`](https://github.com/citusdata/pg_cron),
skip the scheduler process entirely — the stored function is the API:

```sql
select cron.schedule(
  'otra-rebuild-search-index-daily',
  '0 2 * * *',
  $$
  select * from otra.spawn_local(
    'rebuild-search-index',
    jsonb_build_object('scheduled_for', to_char(now() at time zone 'utc', 'YYYY-MM-DD')),
    'jobs',
    jsonb_build_object(
      'idempotency_key',
      'cron:rebuild-search-index:' || to_char(now() at time zone 'utc', 'YYYY-MM-DD')
    )
  );
  $$
);
```

The argument order is `otra.spawn_local(function, params, queue, opts)`, and
the options object uses SQL-side names: `idempotency_key`, `max_attempts`,
`retry_strategy`, `delay_s`, `on_parent_cancel`. The queue must already exist
(`app.createQueue`), and the function name is not validated at spawn time — a
typo produces an execution that every worker defers forever rather than an
error, so keep these strings next to the task definitions in review.

## While you are there

Two maintenance jobs nothing calls for you, and this is where they belong:

- `app.cleanup(queue)` — retention sweep for terminal trees and expired event
  facts. Daily is a reasonable default.
- `app.ensurePartitions()` — extends the week windows for partitioned queues.
  Run it well ahead of the boundary; letting it lapse strands rows in the
  default partition.
