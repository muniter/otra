# Queue Storage Design

Status: **implemented**

otra treats queues as explicit storage, retention, and execution-tree
boundaries, following the production-hardened shape used by absurd.

## Motivation

The shared-table model is compact, but every queue shares indexes, autovacuum,
buffer-cache pressure, cleanup work, and retention policy. Queue names are not
provisioned, so a typo can create work that no worker claims. Cross-queue child
executions also prevent a queue from being independently retained, removed, or
operated.

Production storage needs:

- explicit queue provisioning and validation
- queue-local cleanup and retention policies
- isolation from noisy queues
- bounded deletion of terminal execution trees and their promise histories
- a database-enforced rule that one execution tree belongs to one queue

Partitioned queues use the same execution-tree identity and promise ownership
model as unpartitioned queues. Storage mode is immutable after provisioning.

## Previous Storage

The schema has four fixed tables:

```text
otra.config
otra.executions   queue column
otra.promises     queue implied by execution_id
otra.events       queue column
```

`claim(queue, ...)` scopes claims, timer sweeps, event timeouts, and expired
claim recovery to one queue. Events are immutable facts keyed by
`(queue, name)`. Cleanup is global: it deletes eligible terminal root trees and
relies on foreign-key cascades to remove descendants and promises.

This cleanup is logically sound, but it performs row deletion against shared
relations. PostgreSQL must later reclaim the dead tuples through vacuum.

## Reference: absurd

absurd provisions a physical table set for every queue:

```text
absurd.queues

t_<queue>   tasks
r_<queue>   runs
c_<queue>   checkpoints
e_<queue>   events
w_<queue>   waits
i_<queue>   idempotency keys for partitioned queues
```

Queues can use `unpartitioned` or `partitioned` storage. Partitioned queues
create weekly UUIDv7 range partitions for task, run, checkpoint, and wait
tables. Queue policy controls partition windows, cleanup TTL and batch size,
and detach eligibility.

Partitions do not replace cleanup. absurd's lifecycle is:

1. Delete expired rows in bounded batches.
2. Identify old weekly partitions that have become empty.
3. Detach and optionally drop those empty partitions.

The design is implemented with dynamic SQL because queue names determine table
identifiers. Its production experience and extensive partition, cleanup, and
lock-ordering tests are stronger evidence than a purely static-SQL design
preference.

Primary references in the local checkout:

- `/tmp/absurd/sql/absurd.sql`
- `/tmp/absurd/docs/storage.md`
- `/tmp/absurd/docs/cleanup.md`
- `/tmp/absurd/tests/test_partition_detach.py`
- `/tmp/absurd/tests/test_partition_utils.py`

## Target Invariants

1. Every queue is explicitly provisioned in `otra.queues`.
2. Every execution tree belongs to exactly one queue.
3. A child execution inherits its parent's queue and root identifier.
4. `ctx.spawn()` cannot choose another queue.
5. Events and durable promises are contained by their queue.
6. Queue policy owns cleanup TTL and cleanup batch size.
7. Queue deletion refuses to remove live work unless an explicit destructive
   operation is designed.
8. Queue ownership and same-tree relationships are enforced in PostgreSQL,
   not only by the SDK.
9. Cleanup deletes a root only after every descendant is terminal.
10. Promise history is retained and deleted with its execution tree.

Cross-queue coordination remains possible outside a durable tree: application
code can observe one terminal result and spawn a new top-level execution on a
different queue. A task cannot durably await a child owned by another queue.

## Physical Layout

Each queue owns three table families:

```text
otra.queues

x_<queue>   executions
p_<queue>   durable promises
e_<queue>   immutable event facts
```

Physical names are derived from the **queue name**, not from its UUID:
`x_orders`, `p_orders`, `e_orders`. This follows absurd, and it is an
operability decision — a queue's storage is identifiable on sight in `\dt`,
`pg_stat_user_tables`, `pg_locks`, `EXPLAIN` output, autovacuum logs, and bloat
reports, where `x_019bc186de00…` was opaque. `otra.queues.id` stays: it remains
the SDK's routing and token identity (`ExecutionRoute`, promise tokens), so no
SDK surface changes with this scheme.

Three rules keep name-derived storage safe:

1. **Names are quoted, never sanitized.** Every generated identifier goes
   through `format(... %I ...)`, so spaces, case, non-ASCII and embedded quotes
   are legal queue names. Only the byte length is capped —
   `otra.validate_queue_name` limits a name to 54 bytes so the longest
   identifier we generate (a week partition, `x_<name>_<IYYYIW>` = N + 9) still
   fits PostgreSQL's 63-byte limit. The arithmetic is spelled out above the
   function.
2. **Names are immutable.** The name is now part of the storage identity, so
   `_protect_queue_storage_identity` refuses to change it, exactly as it
   refuses to change `id` or `storage_mode`. Renaming a queue means dropping
   and recreating it. This is the trade-off consciously reversed: the UUID
   scheme made renaming free and operations opaque; we chose the opposite,
   following absurd.
3. **Collisions are refused, not adopted.** A name-derived scheme creates a
   hazard the UUID scheme could not have: queue `orders_202601` would want the
   table `x_orders_202601`, which is queue `orders`'s week partition, and
   `orders_d` its default partition. Two guards close this:
   - `validate_queue_name` rejects any name ending in our own generated
     partition suffixes (`_<6 digits>` or `_d`), independent of creation order
     and of whether the colliding partition exists yet.
   - `create_queue` checks `to_regclass` for every relation it is about to
     create — the four base tables *and* the index names, since tables and
     indexes share one `pg_class` namespace — and raises `physical name
     collision with existing relation "…"` rather than letting
     `create table if not exists` silently adopt a stranger's table as a
     queue's execution store. The check runs only when the `otra.queues` row
     was just inserted, so re-provisioning an existing queue stays idempotent.

   Base relations can never collide with our index names: `x_` always has its
   separator at position 2 while `xi_` does not, so the two prefix families are
   disjoint. That is why `orders` and `orders_ri` are both legal.

   absurd, which has used name-derived storage from the start, ships neither
   guard: its `validate_queue_name` checks only emptiness and byte length, its
   `ensure_queue_tables` is plain `create table if not exists`, and the only
   acknowledgement is a caveat in its `docs/storage.md` about weekly partition
   tags rolling over every ten years "to avoid partition name collisions". We
   add the guards because a silently adopted relation is a corrupted queue,
   not an error.

Execution rows carry both `id` and `root_id`. Root executions set
`root_id = id` at insertion; children inherit the parent's `root_id` and queue.
Promise rows also carry `root_id`, allowing execution and promise storage to
use the same partition bounds.

Queue-local keys are:

```text
x_<queue> primary key:        (root_id, id)
x_<queue> parent reference:   (root_id, parent_id) -> (root_id, id)
p_<queue> primary key:        (root_id, id)
p_<queue> owner reference:    (root_id, execution_id) -> (root_id, id)
p_<queue> deterministic key:  unique (root_id, execution_id, key)
p_<queue> child reference:    (root_id, child_execution_id) -> (root_id, id)
e_<queue> immutable fact:     unique (name)
```

No foreign key crosses a queue table set or execution tree. PostgreSQL requires
partitioned primary and unique constraints to contain the partition key, so
`root_id` is part of these keys for unpartitioned and partitioned queues.

## Partitioned Queues

The design uses these partitioning foundations:

- stable internal queue IDs decouple physical storage from queue names
- UUIDv7 `root_id` values provide a time-ordered partition key
- descendants and promises retain their root's `root_id`
- primary, unique, and foreign keys include `root_id`
- queue storage mode is explicit metadata rather than inferred from relations

Partitioned queues can own `i_<queue>`, an unpartitioned idempotency-key
registry. PostgreSQL cannot enforce queue-wide uniqueness for an index that
omits the `root_id` partition key.

These choices let coordination target the same queue-local parent relations in
both storage modes. Converting an existing unpartitioned queue in place is a
separate migration and is not part of this design.

An unpartitioned queue uses ordinary queue-local tables. A partitioned queue
uses UUIDv7 range partitions based on `root_id`:

```text
x_<queue>
├── x_<queue>_202601
├── x_<queue>_202602
└── x_<queue>_d

p_<queue>
├── p_<queue>_202601
├── p_<queue>_202602
└── p_<queue>_d
```

Partitioning by each execution's own creation ID would scatter a long-lived
tree across weeks. Partitioning by `root_id` keeps descendants and promise
history in the root's storage generation even when they are created later.

A long-lived tree intentionally keeps an old partition non-empty. Other
expired trees in that partition are deleted in bounded cleanup batches. The
partition becomes detachable only after all retained trees have left it.
Detach eligibility is generation-wide: the matching execution and promise
partitions must both be attached, old enough, and empty. An empty promise
partition remains attached while its execution partition contains a live tree.

Events have a separate lifecycle and can remain unpartitioned initially. A
high-volume event table can later use its own UUIDv7 creation-time partitions.

## Cleanup Lifecycle

Queue cleanup reads the queue's TTL and batch limit, then:

1. Selects terminal root executions older than the TTL.
2. Excludes roots with any non-terminal descendant.
3. Deletes a bounded batch of eligible roots.
4. Cascades deletion through descendants and durable promises.
5. Deletes expired event facts in a separate bounded batch.

Promises are not independently expired. A promise is replay history and must
exist for exactly as long as its execution tree is retained.

## Dynamic SQL Boundary

The queue-local design requires dynamic SQL for both storage management and
coordination because stored functions must address queue-specific tables.
Dynamic identifiers are derived from the provisioned queue's (immutable)
name and always passed through `%I` formatting.

The implementation centralizes table-name derivation and tests provisioning
races, relation-family collisions, and queue drop behavior. Every dynamic
coordination path needs tests because PostgreSQL cannot validate generated
table and column references when a function is created.

Queue-local coordination takes a compatible lock on its queue metadata row
before touching physical tables. Policy changes lock that row for update,
forming a maintenance barrier, and acquire partition parents before leaf
partitions.

Dynamic coordination follows absurd's proven identifier/value split:

- derive relation names from the persisted queue name (immutable, %I-quoted)
- interpolate identifiers only through `format(... %I ...)`
- bind every UUID, timestamp, worker ID, key, and JSON value through
  `EXECUTE ... USING`
- keep each state transition inside one stored-function transaction
- order contested rows deterministically before `FOR UPDATE`
- use bounded `SKIP LOCKED` sweeps for queue maintenance

Unlike absurd, public coordination functions resolve a provisioned queue
before deriving relations. Missing queues therefore produce one stable error
instead of relation-not-found errors from scattered dynamic statements.

## Coordination Routing Contract

Runtime coordination uses the provisioned table families throughout. The
cutover spans spawn, claim, replay, history writes, suspension, settlement,
events, cancellation, inspection, and cleanup; there is no shared-table
fallback.

A queue-local execution is addressed internally by:

```typescript
interface ExecutionRoute {
  queueId: string;
  rootId: string;
  executionId: string;
}
```

Each field is load-bearing:

- `queueId` selects the physical table family without depending on a mutable
  queue label.
- `rootId` selects the partition and participates in every composite key.
- `executionId` selects the execution inside its tree.

Queue names remain user-facing routing labels for top-level spawn, worker
claim, and event emission. SQL resolves the label once and returns the stable
queue ID. After claim, the worker and replay driver carry `ExecutionRoute`
through every database call instead of repeatedly rediscovering it.

Top-level roots are inserted atomically with `id = root_id`; the current
insert-then-update pattern is invalid for partitioned storage. `claim()` returns
`queue_id` and `root_id` with each execution. Child spawn receives the parent's
route and derives queue and root in PostgreSQL.

The recommended public execution reference is the serializable route itself:

```typescript
interface ExecutionRef {
  queueId: string;
  rootId: string;
  executionId: string;
}
```

`app.spawn()` returns an `ExecutionRef`; inspection, result, cancel, and kill
accept it. This avoids a global `execution_id -> queue_id, root_id` routing
registry and guarantees partition-pruned access. Durable child handles retain
the same routing fields internally. `ctx.executionId` remains the execution
UUID, while database coordination uses the full route.

Task definitions do not own queues. Queue selection belongs to the app or a
top-level spawn. Child spawn options omit `queue` and `idempotencyKey`; children
always inherit their parent's queue and root.

## Coordination Module

PostgreSQL remains the deep coordination module. Its internal routing helper
resolves a stable queue ID, acquires the metadata lock, and derives `x`, `p`,
`e`, and `i` relation names. Public stored functions hide relation naming,
partition keys, lock ordering, retries, and wakeups from every SDK.

Hot coordination uses a queue metadata `FOR KEY SHARE` lock before touching a
physical table. Policy, drop, and partition maintenance use `FOR UPDATE`. This
barrier prevents maintenance from changing or detaching storage while a
coordination transaction is using it; compatible hot operations still run
concurrently.

The routing helper has two entry seams:

```text
queue label -> stable queue route   spawn, claim, emit
queue ID    -> stable queue route   replay, settlement, inspection
```

Both seams resolve the same queue row; the ID seam reads `name` from it (under
the same `FOR KEY SHARE` barrier) and derives the relation names from that.
Internal helpers that receive only a queue ID (`_wake_local`,
`_settle_child_promises_local`, `_fail_attempt_local`, `_assert_owner_local`)
do a plain unlocked `select name` — their callers already hold the barrier, and
the name cannot change under them. Coordination functions then build one
dynamic statement where practical instead of exposing table names to the
TypeScript SDK.

## Coordination Matrix

| Area | Current address | Queue-local change |
| --- | --- | --- |
| Top-level spawn | queue name | Resolve queue; pre-generate `id = root_id`; use `i_*` for partitioned idempotency. |
| Child spawn | caller queue + parent ID | Accept parent route; derive queue/root; memoize through the parent `p_*` row. |
| Claim and sweeps | queue name | Operate directly on local `x_*`/`p_*`; return queue/root; preserve bounded `SKIP LOCKED`. |
| Replay reads | execution ID | Query `p_*` by `(root_id, execution_id)` using the execution route. |
| History writes | execution ID + worker | Route-aware `_assert_owner`; insert root on every promise; preserve write-once rows. |
| Suspend | execution ID | Lock routed execution, then perform non-locking blocker checks in routed `p_*`. |
| Promise resolvers | promise ID | Lock routed promise first, then owner executions in UUID order. |
| Events | queue name | Resolve local `e_*`/`p_*`; retain advisory race lock keyed by queue UUID and event name. |
| Complete/fail | execution ID + worker | Preserve exact worker ownership, SQL retry calculation, parent promise settlement, and forensic `claimed_by`. |
| Cancellation/kill | execution ID | Accept execution route; traverse and lock one routed root tree in UUID order. |
| Inspection/result | execution ID | Accept `ExecutionRef`; no partition scan or global routing registry. |
| Cleanup | global TTL | Read queue policy; delete bounded eligible roots, `i_*` mappings, and old `e_*` facts. |

## Lock Protocol

Queue routing adds one outer lock without changing otra's proven inner
protocols:

```text
queue metadata barrier
  -> event advisory lock, when applicable
  -> existing execution/promise row protocol
```

Load-bearing row orders remain:

- history creation: owner execution row, then promise insertion
- resolvers: promise row, then owner execution rows in UUID order
- suspension: owner execution row, then non-locking blocker status checks
- tree cancellation/kill: all selected execution rows in UUID order

Expired claims continue through the ordinary failure transition rather than
silently resetting ownership. All timer, timeout, claim-expiry, and runnable
claim sweeps remain bounded and use deterministic tie-breaking. Forced lock
tests must observe blocking through `pg_stat_activity` before releasing gates.

The most relevant absurd lessons are:

- `47e6710`: claim expiry is an attempt failure
- `bcde0df`: event wait registration must serialize against emit
- `300a5c2`: competing terminal transitions share one lock order
- `7b63b7a`: events are immutable first-write-wins facts
- `9c5388e`: idempotency conflict lookup locks against concurrent cleanup and
  raises `40001` if the canonical execution disappears
- `866480d`: retry policies are validated at spawn and poison rows cannot wedge
  claim maintenance

## Cutover Tracer

The smallest functioning end-to-end tracer is an effect-free task:

```text
app.spawn
  -> queue-local root insert
worker.claim
  -> queue-local claim returning ExecutionRoute
driver.loadHistory
  -> empty queue-local promise history
driver.complete
  -> queue-local terminal execution
app.getResult
  -> routed terminal inspection
```

This tracer deliberately includes more than spawn and claim. It proves the
route can cross the SDK/worker/driver seam and that partitioned and
unpartitioned queues present one coordination interface.

The tracer requires:

1. Queue routing helpers and stable queue-not-found errors.
2. `ExecutionRef`/`ExecutionRoute` and route-aware SDK database methods.
3. Top-level spawn with direct `id = root_id` insertion.
4. Partitioned idempotency reservation and the cleanup race protocol.
5. Queue-local claim returning `queue_id` and `root_id`.
6. Route-aware `load_history`, `complete`, and `get_execution`.
7. Tests proving weekly routing, ownership, idempotency, and both storage modes.

The shared tables and static coordination paths are absent, so incomplete
routing cannot be masked by a fallback.

## Expected Performance

No otra benchmark has yet compared these layouts. The expected behavior is:

```text
small installation:
  shared static tables may be faster because plans are reusable

large retained history or many active queues:
  queue-local tables should provide more predictable claim and maintenance
  behavior through smaller indexes and isolated vacuum work
```

Queue-local dynamic queries avoid global indexes and partition dispatch, but
PL/pgSQL `EXECUTE` incurs parse and planning work. The dominant production
benefit is expected to come from physical isolation, smaller working sets, and
independent maintenance rather than a microsecond-level query-path advantage.

Before selecting defaults, benchmark:

- 1, 10, and 100 queues
- millions of retained executions and promises
- 8, 16, and 32 concurrent claimers
- claim throughput and p95/p99 latency
- planning time versus execution time
- lock waits, WAL volume, and buffer-cache hit rate
- bounded cleanup throughput and autovacuum activity

## SDK Shape

The target TypeScript interface is:

```typescript
await app.createQueue("orders", {
  storageMode: "partitioned",
});
await app.setQueuePolicy("orders", {
  cleanupTtl: "30 days",
  cleanupLimit: 1_000,
});

const orders = new Otra({ db, queue: "orders" });
const execution = await orders.spawn(orderTask, params);
await orders.getResult(execution);
```

Top-level spawn selects a provisioned queue and returns `ExecutionRef`. Task
registration describes code rather than storage placement. Child spawn options
omit `queue` and `idempotencyKey`; PostgreSQL derives queue and root from the
parent route.

Queue management needs at least:

- `createQueue`
- `getQueue`
- `listQueues`
- `setQueuePolicy`
- `dropQueue`
- queue-specific cleanup operations
- `ensurePartitions`
- `listDetachCandidates`

## External Promise Tokens

External promise settlement currently addresses a promise by UUID alone. With
queue-local promise tables, settlement must also locate the queue table.

The token is a versioned opaque address containing routing data:

```text
queue ID + root_id + promise_id
```

The random promise identifier remains the unforgeable capability. Encoding
routing data avoids an unbounded global token lookup table, which would
reintroduce shared storage and cleanup pressure. Tokens carry the internal
queue ID rather than exposing the user-facing queue name. The recommended wire
shape is `otr1_<base64url(queue UUID || root UUID || promise UUID)>`. Parsing
returns the route needed for one partition-pruned promise update; no global
token lookup table is introduced.

## Implementation Plan

The repository has no migration framework and is not yet published, so this
should land as a deliberate schema rewrite rather than compatibility layers.
Each slice follows red-green TDD against real PostgreSQL.

1. Land the effect-free cutover tracer across spawn, claim, replay, completion,
   and inspection.
2. Route `_assert_owner`, history reads, and write-once history creation.
3. Route child spawn with inherited queue/root and split top-level/child option
   types.
4. Route bounded timer, timeout, claim-expiry, and retry transitions; add plan
   regressions for queue-local indexes.
5. Route suspension, wakeups, child settlement, and external promise tokens;
   rerun every forced-contention test.
6. Route event facts and waits while retaining immutable one-shot and no-lost-
   wakeup semantics.
7. Route cancellation delivery, kill, compensation finalization, heartbeat,
   and defer through one execution route.
8. Replace global cleanup with policy-driven bounded root/event cleanup and
   remove partitioned idempotency mappings in the same transaction.
9. Delete shared runtime tables and static coordination paths; run the complete
   suite against unpartitioned and partitioned queues.
10. Add detach/drop tooling, benchmarks, and operator scheduling after the
    coordination interface stabilizes.

## Open Questions

- Which queue policy operations belong in the SDK versus an operator CLI?
- Should queue labels be immutable now that public execution references route
  by stable queue ID?
