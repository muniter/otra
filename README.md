# otra

*¡Otra! — durable execution that plays it again.*

otra is a generator-based durable execution engine on Postgres, built on
durable promises. It began as an experimental sibling of
[absurd](https://github.com/earendil-works/absurd) and keeps its philosophy —
all coordination logic lives in Postgres stored functions, the SDK stays
thin — rebuilt around **durable promises** and a **generator-based API**.
The coordination engine lives in [`sql/schema.sql`](sql/schema.sql), and the
current SDK lives in [`sdks/typescript`](sdks/typescript). The repository keeps
language SDKs independently buildable so more runtimes and operator tools can
be added without coupling their toolchains. Standalone operator applications
belong under `apps/<name>` when introduced.

```text
sql/                 Postgres coordination engine
sdks/typescript/     TypeScript SDK, tests, and examples
docs/                Design documentation
```

The design follows Jack Vanlightly's *Theory of Durable Execution* series,
which is the best available explanation of the primitives involved:

- [Demystifying Determinism in Durable Execution](https://jack-vanlightly.com/blog/2025/11/24/demystifying-determinism-in-durable-execution)
- [The Durable Function Tree — Part 1](https://jack-vanlightly.com/blog/2025/12/4/the-durable-function-tree-part-1)
- [The Durable Function Tree — Part 2](https://jack-vanlightly.com/blog/2025/12/4/the-durable-function-tree-part-2)
- [The Three Durable Function Forms](https://jack-vanlightly.com/blog/2025/12/10/the-three-durable-function-forms)

("¡Otra!" is what the crowd shouts for an encore — *again!* — which is
exactly how recovery works here: re-execute from the top, fast-forwarding
through memoized results.)

## Why a second experiment?

absurd works, but two of its design choices fight each other:

1. **Suspension is a thrown exception.** `ctx.sleep()` and `ctx.awaitEvent()`
   throw a `SuspendTask` sentinel that unwinds the handler's stack. Any user
   `catch` block can accidentally swallow it, `finally` blocks run during a
   suspension that will later resume, and the signal is meaningless across a
   `Promise.all`.
2. **There is no parent/child relationship.** Spawning a task from inside a
   task is fire-and-forget; awaiting the child means busy-polling while
   holding the worker's claim and concurrency slot — which deadlocks if the
   child needs that same slot, so same-queue awaits are forbidden outright.

Both problems have the same root: an `async` function cannot be paused from
the outside, so suspension has to be smuggled through exceptions, and a
waiting parent has to keep its stack alive on a worker.

Generators dissolve this. A generator's body *only* advances when the driver
calls `next()`. Every durable operation is a `yield`, so the driver — not
user code — owns every await:

- Suspension is simply *not calling `next()` again*. No exception, nothing
  to swallow, no `finally` surprises.
- When a parent awaits an unresolved child, the driver parks the execution
  in Postgres and the worker moves on. The parent consumes **zero** resources
  until the child settles — same queue, same worker, no deadlock possible.
- Resumption is replay: re-run the generator from the top, injecting the
  memoized result at each `yield`, until it either finishes or blocks again.

## The model: everything is a durable promise

Every suspension point in a task is a **durable promise**: a write-once
register in Postgres, addressed by a deterministic key inside the owning
execution's history. There are four kinds:

| kind       | created by            | resolved by                                 |
| ---------- | --------------------- | ------------------------------------------- |
| `run`      | `ctx.run(label, fn)`  | the worker, immediately after running `fn`  |
| `sleep`    | `ctx.sleep(duration)` | the claim sweep, when the timer is due      |
| `event`    | `ctx.waitForEvent(n)` | `emit_event` (or rejected on timeout)       |
| `child`    | `ctx.spawn(task, p)`  | the child execution completing or failing   |
| `external` | `ctx.promise(label?)` | outside code, by token (or timeout rejects) |

Executions form a tree through `child` promises. A failed child rejects the
parent's promise; the parent sees a `ChildFailedError` at its `yield` — which
it can catch — and failures stay contained to their branch, exactly as the
durable-function-tree model prescribes.

Promise keys are label-based (`step-name`, `step-name#2` for repeats), like
absurd's checkpoints, so renaming a step orphans its old state rather than
misreading it. Reusing a key with a *different shape* (a recorded `run`
replayed as an event wait, say) is detected and fails the execution with a
`DeterminismViolationError` instead of silently corrupting it.

## Example

```typescript
import { Otra } from "otra";

const app = new Otra({ db: process.env.DATABASE_URL, queue: "orders" });
await app.createQueue();

const processPayment = app.task("process-payment", function* (params: { amount: number }, ctx) {
  return yield* ctx.run("charge", async () => {
    const charge = await stripe.charges.create({ amount: params.amount });
    return { chargeId: charge.id };
  });
});

const orderFulfillment = app.task("order-fulfillment", function* (params: Order, ctx) {
  // Real fan-out: children are independent executions on the same queue.
  const payment = yield* ctx.spawn(processPayment, { amount: params.amount });
  const inventory = yield* ctx.spawn(reserveInventory, { items: params.items });

  // The parent suspends here -- zero worker footprint -- until both settle.
  const [charge, stock] = yield* ctx.all([payment, inventory]);

  // Suspend for up to 30 days awaiting the warehouse.
  const shipment = yield* ctx.waitForEvent(`shipment.packed:${params.orderId}`, {
    timeout: "30d",
  });

  yield* ctx.run("notify", async () => {
    await sendEmail(params.email, shipment);
  });
  return { chargeId: charge.chargeId, tracking: shipment.trackingNumber };
});

await app.spawn(orderFulfillment, order);
app.startWorker();
```

The complete runnable version is
[`sdks/typescript/examples/order.ts`](sdks/typescript/examples/order.ts).

### Context API

Everything is consumed with `yield*`, which is what threads the result types
through:

- `ctx.run(label, fn)` — checkpoint a side effect; `fn` runs at most once per
  recorded result and must return a `JsonValue`. Project rich SDK responses
  into plain JSON objects. A callback with no return value is recorded and
  typed as `null`. A throw fails the attempt and the task retries (recorded
  steps are skipped on replay).
- `ctx.sleep("5m")` — durable timer. Accepts `ms/s/m/h/d` strings or seconds.
- `ctx.waitForEvent(name, { timeout? })` — suspend until the event is emitted
  on this queue. Events are cached, so emit-then-await is race-free. On
  timeout an `EventTimeoutError` is thrown *into* the task (catchable).
- `ctx.spawn(task, params, opts?)` — start a child execution, get a
  `DurableHandle`. Replay-safe: the child promise is the memo, so replays
  never double-spawn.
- `ctx.promise(label?, { timeout? })` — create an externally-settleable
  promise: a normal handle plus an opaque token (`otr1_...`) to hand to the
  outside world (an approval link, a webhook correlation id). Regular code
  settles exactly that promise with `app.resolvePromise(token, value)` /
  `app.rejectPromise(token, error)` — write-once, waking the suspended
  execution through the same race-free path events use. With a `timeout`,
  the await throws a catchable `TimeoutError` instead of waiting forever.
- `ctx.await(handle)` / `ctx.all([h1, h2, ...])` — redeem handles; suspends
  if unresolved, throws `ChildFailedError` if the child failed permanently.
- `ctx.call(task, params)` — `spawn` + `await` in one.
- `ctx.now()`, `ctx.random()`, `ctx.uuid()` — deterministic-by-memoization
  versions of the usual non-deterministic suspects.
- `ctx.executionId`, `ctx.attempt`, `ctx.queue`.
- Cancellation-aware code: `ctx.cancelRequested` (flips via heartbeat),
  `ctx.signal` (an `AbortSignal` for in-flight I/O), `ctx.throwIfCancelled()`
  for long loops, and `ctx.uninterruptible(fn)` for forward-critical
  sections that must not be interrupted midway.

### Client API

`app.task(nameOrOpts, handler)`, `app.spawn(task, params, opts?)`,
`app.emitEvent(name, payload?)`, `app.resolvePromise(token, value)` /
`app.rejectPromise(token, error)`, `app.getResult(execution)`,
`app.getExecution(execution)`, `app.cancel(execution)`, `app.kill(execution)`,
`app.createQueue(name?)`, `app.getQueue(name?)`, `app.listQueues()`,
`app.setQueuePolicy(name, policy)`, `app.getQueuePolicy(name?)`,
`app.ensurePartitions(name?)`, `app.listDetachCandidates(name?)`,
`app.createWorker()` / `app.startWorker()`, `app.applySchema()`.

Top-level spawns accept an `idempotencyKey` (at-most-one execution per
`(queue, key)`, race-safe), protecting the API boundary against double
delivery -- child spawns are already deduplicated by the parent's promise
key. A spawn returns an `ExecutionRef` containing the stable queue ID, root ID,
and execution ID required for direct queue and partition routing. Workers run
up to `concurrency` executions concurrently with slot-based
claiming: one slow step never blocks the worker from picking up other work,
and `stop()` drains in-flight executions gracefully.

### Cancellation

Two verbs, following every production engine surveyed in
[docs/cancellation-design.md](docs/cancellation-design.md):

- `app.cancel(execution, { cascade, reason })` — **graceful**. Cancellation is a
  *request* against a live execution (a `cancel_requested_at` column; status
  stays `running`), discovered through the claim or the heartbeat and
  delivered as a catchable `CancelledError` thrown into the generator at the
  first effect needing new work. Compensation in plain `catch`/`finally` may
  run `ctx.run` steps that **checkpoint normally** — no `nonCancellable`
  wrapper, otra's ergonomic edge over Temporal TS. Compensation may even
  **suspend**: call durable children (`ctx.call(refundTask, …)`), sleep, or
  wait — delivery is journaled as a promise row (kind `cancel`, key
  `$cancel`) recording *where* the CancelledError was thrown, so every
  replay re-delivers at the same yield even if the forward promise settles
  later, and a crashed or failed compensation retries from its own
  checkpoints. However the generator ends, the engine finalizes to
  `cancelled` — catching and returning normally never yields `completed`
  (Temporal's documented footgun, closed), and exhausted compensation
  retries finalize as `cancelled`, never `failed`. Cascades down the tree by
  default; children spawned with `onParentCancel: 'detach'` are exempt.
  Test with `isCancellation(err)` — also true for an awaited child that was
  cancelled.
- `app.kill(execution, { cascade, reason })` — **immediate**, no compensation. The
  driving worker discovers sqlstate `OT002` at its next history write and
  abandons (reported as `killed`, distinct from a stolen claim's `OT001`).

## How the pieces fit

**Postgres owns**: spawning (idempotent under a parent promise key), claiming
(with `FOR UPDATE SKIP LOCKED`), the timer sweep, event fan-out, suspension
(atomic against concurrent resolution — no lost wakeups), retry scheduling
with backoff, claim-expiry recovery, and result propagation up the tree. All
of it inside [`sql/schema.sql`](sql/schema.sql); the SDK never issues SQL
beyond `select * from otra.<function>(...)`.

**The worker owns**: replaying generators. Claim a batch, drive each one:
inject memoized results at each `yield`, execute the first unrecorded `run`
(with a driver-owned heartbeat while `fn` is in flight — no more silent lease
loss during a long step), and park the execution when it blocks on an
unresolved remote promise.

Each provisioned queue owns an `x_<queue>` execution tree, a `p_<queue>`
promise journal, and an `e_<queue>` event-fact table — named after the queue
(`x_orders`, `p_orders`), so a queue's storage is recognizable on sight in
`\dt`, `pg_locks` and slow-query logs. The price is that queue names are
immutable and capped at 54 bytes; see
[`docs/queue-storage-design.md`](docs/queue-storage-design.md).
Partitioned queues range-partition executions and promises together by
`root_id`. States:
`pending → running → suspended/completed/failed/cancelled`. Retries are
attempt-scoped like absurd: the *task* retries, checkpointed steps don't
re-run.

Two details worth stealing even if you throw the rest away:

- The suspension transition locks the execution row *before* checking whether any
  blocker already settled, while resolvers lock promise-then-execution; the
  race between "I'm going to sleep" and "your child just finished" therefore
  always converges (refuse-to-suspend → immediate replay, or wake-after-park).
- Waking on *any* promise resolution and letting the replay re-suspend if it
  is still blocked keeps the SQL trivially correct; replay is cheap because
  history injection is a single bulk load.

## Testing

Tests are plain `node:test` against a real Postgres. `make test` starts a
Postgres 16 Testcontainer, prepares one schema-bearing template database, and
runs up to eight test files in parallel against disposable database clones.
`OTRA_TEST_CONCURRENCY` overrides that limit. `OTRA_TEST_DB` can point the
suite at an existing Postgres 14 or newer; that mode runs serially and resets
the supplied database's `otra` schema between tests. Database time is frozen
via `otra.set_fake_now()` (stored in a table, not a GUC, so every pooled
connection sees the same fake clock — no `max: 1` workaround needed):

```bash
# Install the TypeScript SDK dependencies, then start an isolated Postgres 16
# container with Docker.
make install
make test

# Or use an existing database; tests drop/recreate the otra schema.
OTRA_TEST_DB=postgres://postgres@127.0.0.1:5433/postgres make test
```

The suite covers memoization across suspension, replay determinism, the
determinism guard, same-queue parent/child, fan-out with `ctx.all`, child
failure propagation (caught and uncaught), event caching/broadcast/timeouts,
retry backoff and `maxAttempts`, non-retryable errors, and crash recovery
through claim expiry. Worker tests use absurd's EventEmitter-gate pattern
(handlers rendezvous on `once(gate, "release")` instead of sleeping), which
pins the concurrency cap, wake-on-free claiming, and graceful drain.

Two further suites encode the sharp edges found by adversarial review and by
mining absurd's commit history for bugs it already paid for — each test cites
the absurd commit it descends from: the await/emit lost-wakeup race
(serialized via per-event advisory locks; absurd `bcde0df`), zombie workers
writing checkpoints or spawning children into stolen executions (ownership
guards raising sqlstate `OT001`; absurd's `AB002`), retry-strategy validation
at spawn plus a one-day hard cap and a poison-proof claim sweep (absurd
`866480d`), backoff overflow saturation, bounded subtree-aware cleanup,
idempotent spawn races, two-worker claim races, worker head-of-line blocking,
and driver-escaping errors failing the attempt instead of stranding a live
claim (absurd `4aec33e`).

## Status and non-goals (for now)

An experiment, not a product. Deliberately out of scope so far, roughly in
the order they'd matter:

- `LISTEN/NOTIFY`-driven wakeups (the SQL already emits `otra_wake`
  notifications; the worker currently just polls).
- A `race` combinator next to `all`.
- Cancellation, remaining tiers: leaf-first finalization (a parent waits for
  descendants before compensating), preemptible runs, operator
  pause/resume — see docs/cancellation-design.md.
- Vanlightly's other two function forms: this implements *stateless
  functions* only — no sessions (external signals directed at one execution
  are approximated by events, which broadcast per queue) and no actors.
- Partitioning, per-queue tables, migrations, and the operational maturity
  absurd has been accumulating.
- Serialization is `JSON.stringify`, with all the absurd-inherited caveats
  (`undefined` → `null`, `Date` → string, no `Map`/`Set`/`BigInt`).
- Event semantics are absurd's: an event name is an **immutable one-shot
  fact** per queue — first write wins, later emits are no-ops, and every
  wait (a repeat wait in the same execution included) resolves with that
  same fact. A recurring signal derives names (`packed:${orderId}`,
  `tick:${i}`); an instance-scoped signal uses `ctx.promise`. Facts expire
  with `cleanup`'s TTL. (otra briefly shipped retained-latest semantics
  with per-execution consumption tracking; it was replaced with facts —
  the model absurd also converged on after being burned by mutable events.)
