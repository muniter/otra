# Queue Storage Design

Status: **accepted direction; implementation in progress**

otra currently treats a queue as a routing label on shared `executions` and
`events` tables. This document evaluates making queues explicit storage,
retention, and execution-tree boundaries, following the production-hardened
shape used by absurd.

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

## Current Storage

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

## Proposed Invariants

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

## Proposed Physical Layout

Each queue owns three table families:

```text
otra.queues

x_<queue-id>   executions
p_<queue-id>   durable promises
e_<queue-id>   immutable event facts
```

Queue names are user-facing labels. A stable UUID in `otra.queues` owns the
physical table names, so valid queue names cannot collide with generated table,
index, or constraint names. Table and index families use distinct prefixes.

Execution rows carry both `id` and `root_id`. Root executions set
`root_id = id` at insertion; children inherit the parent's `root_id` and queue.
Promise rows also carry `root_id`, allowing execution and promise storage to
use the same partition bounds.

Queue-local keys are:

```text
x_<queue-id> primary key:        (root_id, id)
x_<queue-id> parent reference:   (root_id, parent_id) -> (root_id, id)
p_<queue-id> primary key:        (root_id, id)
p_<queue-id> owner reference:    (root_id, execution_id) -> (root_id, id)
p_<queue-id> deterministic key:  unique (root_id, execution_id, key)
p_<queue-id> child reference:    (root_id, child_execution_id) -> (root_id, id)
e_<queue-id> immutable fact:     unique (name)
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

Partitioned queues can own `i_<queue-id>`, an unpartitioned idempotency-key
registry. PostgreSQL cannot enforce queue-wide uniqueness for an index that
omits the `root_id` partition key.

These choices let coordination target the same queue-local parent relations in
both storage modes. Converting an existing unpartitioned queue in place is a
separate migration and is not part of this design.

An unpartitioned queue uses ordinary queue-local tables. A partitioned queue
uses UUIDv7 range partitions based on `root_id`:

```text
x_<queue-id>
├── x_<queue-id>_634
├── x_<queue-id>_635
└── x_<queue-id>_d

p_<queue-id>
├── p_<queue-id>_634
├── p_<queue-id>_635
└── p_<queue-id>_d
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
Dynamic identifiers are derived from provisioned queue UUIDs and always passed
through `%I` formatting.

The implementation centralizes table-name derivation and tests provisioning
races, relation-family collisions, and queue drop behavior. Every dynamic
coordination path needs tests because PostgreSQL cannot validate generated
table and column references when a function is created.

Queue-local coordination takes a compatible lock on its queue metadata row
before touching physical tables. Policy changes lock that row for update,
forming a maintenance barrier, and acquire partition parents before leaf
partitions.

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

A possible TypeScript interface is:

```typescript
await app.createQueue("orders", {
  storageMode: "partitioned",
});
await app.setQueuePolicy("orders", {
  cleanupTtl: "30 days",
  cleanupLimit: 1_000,
});

const orders = new Otra({ db, queue: "orders" });
await orders.spawn(orderTask, params);
```

Top-level spawn selects a provisioned queue. Child spawn options omit `queue`;
the database derives queue and root from the parent. Task registration should
not silently move a child to another queue.

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

The preferred direction is a versioned opaque token containing routing data:

```text
queue ID + root_id + promise_id
```

The random promise identifier remains the unforgeable capability. Encoding
routing data avoids an unbounded global token lookup table, which would
reintroduce shared storage and cleanup pressure. Tokens carry the internal
queue ID rather than exposing the user-facing queue name. Exact encoding and
versioning remain open design work.

## Implementation Plan

The repository has no migration framework and is not yet published, so this
should land as a deliberate schema rewrite rather than compatibility layers.
Each slice follows red-green TDD against real PostgreSQL.

1. Add queue provisioning and validation, with an unpartitioned queue table
   set and idempotent concurrent creation tests.
2. Route top-level spawn and claim through provisioned queue tables; reject
   unknown queues.
3. Move durable promise creation, replay, suspension, and settlement into the
   queue table set while preserving ownership and lock-order invariants.
4. Make child spawn derive queue and root from its parent; remove the child
   queue option and add database-level cross-queue rejection tests.
5. Adapt cancellation, event facts, external promises, and execution snapshots
   to queue-local storage.
6. Add queue policy and bounded tree-aware cleanup; prove that live descendants
   block root deletion and that promise history cascades with the tree.
7. Add partition default management and empty-partition candidate discovery.
8. Add detach/drop operations and forced-contention tests around active trees.
9. Benchmark unpartitioned and partitioned queue-local layouts.
10. Add operator tooling only after the SQL interface and policies stabilize.

## Open Questions

- What exact table prefixes make operational inspection clearest?
- What routing data and versioning should external promise tokens contain?
- Should task registration retain any queue preference, or should only app and
  top-level spawn configuration select a queue?
- Which queue policy operations belong in the SDK versus an operator CLI?
