import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import pg from "pg";

import { Otra, isNotFound, isPreconditionFailed } from "../src/index.ts";
import { createTestEnv, waitFor, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv("orders", false);
});
afterEach(async () => {
  await env?.close();
});

test("provisions and discovers an unpartitioned queue", async () => {
  const { app, pool } = env;

  assert.deepEqual(await app.listQueues(), []);

  await app.createQueue();

  assert.deepEqual(await app.getQueue(), {
    name: "orders",
    storageMode: "unpartitioned",
  });
  assert.deepEqual(await app.listQueues(), [
    { name: "orders", storageMode: "unpartitioned" },
  ]);

  // Physical names derive from the queue NAME, not its UUID: readable in
  // \dt, pg_stat_user_tables, EXPLAIN and every log line.
  const { rows } = await pool.query(
    `select array_agg(c.relname::text order by c.relname)::text[] as tables
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
      where c.relkind = 'r'
        and c.relname in ('x_orders', 'p_orders', 'e_orders')`,
  );
  assert.deepEqual(rows[0].tables, ["e_orders", "p_orders", "x_orders"]);
});

test("concurrent queue provisioning is idempotent", async () => {
  const { app } = env;

  await Promise.all([
    app.createQueue(),
    app.createQueue(),
    app.createQueue("orders"),
  ]);

  assert.deepEqual(await app.listQueues(), [
    { name: "orders", storageMode: "unpartitioned" },
  ]);
});

test("queue names are quoted identifiers with a bounded byte length", async () => {
  const { app, pool } = env;

  // Nothing about the character set is policed: every generated identifier
  // goes through %I, so spaces, case, non-ASCII and even embedded quotes are
  // just quoted-identifier material.
  await app.createQueue("Queue Name-1");
  await app.createQueue("pedidos-ñ");
  await app.createQueue(`quoted"queue`);
  await app.createQueue("q".repeat(54));

  assert.deepEqual(await app.listQueues(), [
    { name: "Queue Name-1", storageMode: "unpartitioned" },
    { name: "pedidos-ñ", storageMode: "unpartitioned" },
    { name: "q".repeat(54), storageMode: "unpartitioned" },
    { name: `quoted"queue`, storageMode: "unpartitioned" },
  ]);
  const quoted = await pool.query(
    `select count(*)::int as count
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
      where c.relname in ('x_Queue Name-1', 'x_pedidos-ñ', 'x_quoted"queue')`,
  );
  assert.equal(quoted.rows[0].count, 3);

  // The cap is set by the longest identifier we generate: a week partition,
  // 'x_' + name + '_' + a 6-character tag. At the cap that is exactly 63
  // bytes, so a partitioned queue named right at the limit must provision.
  await app.createQueue("p".repeat(54), { storageMode: "partitioned" });
  const longest = await pool.query(
    `select max(octet_length(c.relname))::int as widest,
            (select count(*)::int
               from pg_inherits i
               join pg_class child on child.oid = i.inhrelid
              where i.inhparent = (
                select c2.oid from pg_class c2
                  join pg_namespace n2
                    on n2.oid = c2.relnamespace and n2.nspname = 'otra'
                 where c2.relname = $2
              )) as partitions
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
      where c.relname like $1`,
    [`%${"p".repeat(54)}%`, `x_${"p".repeat(54)}`],
  );
  // Exactly at the limit: nothing was silently truncated (truncation would
  // fold two weeks onto one name and lose a partition).
  assert.equal(longest.rows[0].widest, 63);
  assert.equal(longest.rows[0].partitions, 6);

  await assert.rejects(app.createQueue(""), /Queue name must be provided/);
  await assert.rejects(
    app.createQueue("q".repeat(55)),
    /too long \(max 54 bytes\)/,
  );
  await assert.rejects(
    app.createQueue("ñ".repeat(28)),
    /too long \(max 54 bytes\)/,
  );
});

test("a queue whose name needs quoting still coordinates end to end", async () => {
  const { app, pool } = env;

  // Storage names are now user-controlled text, so every dynamic statement in
  // the engine -- spawn, claim, history writes, events, cleanup, partition
  // maintenance, drop -- has to survive a name that only works quoted.
  const name = `Weird "Name" ñ`;
  await app.createQueue(name, { storageMode: "partitioned" });
  const local = new Otra({ db: pool, queue: name });
  try {
    const task = local.task("quoted-task", function* (_params: null, ctx) {
      yield* ctx.run("step", () => "stepped");
      return "ok";
    });
    const execution = await local.spawn(task, null);
    await local.createWorker({ workerId: "w1" }).drain();
    assert.equal(await local.getResult(execution), "ok");

    await local.emitEvent("a-fact", { v: 1 });
    await local.ensurePartitions(name);
    await local.cleanup(name, { ttl: "30 days" });

    const { rows } = await pool.query(
      `select count(*)::int as count
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
        where c.relname = $1`,
      [`x_${name}`],
    );
    assert.equal(rows[0].count, 1);
  } finally {
    await local.close();
  }

  await app.dropQueue(name);
  assert.equal(await app.getQueue(name), null);
});

test("queue names cannot collide with another queue's physical relations", async () => {
  const { app, pool } = env;

  // Name-derived storage makes one hazard the UUID scheme could not have:
  // a queue whose name reproduces a relation another queue already owns.
  // Anything already sitting on a name we are about to create is refused,
  // loudly, instead of being silently adopted by "create table if not exists".
  await pool.query(`create table otra.x_shipments (probe int)`);
  await assert.rejects(
    app.createQueue("shipments"),
    /physical name collision with existing relation "x_shipments"/,
  );
  assert.deepEqual(await app.listQueues(), []);

  // The guard covers index names too (tables and indexes share one pg_class
  // namespace), but a name that merely looks like an index suffix is fine:
  // "orders_ri" gets the table x_orders_ri, while "orders"'s index is
  // xi_orders_ri -- the 'x_' and 'xi_' prefix families can never meet.
  await app.createQueue("orders");
  await app.createQueue("orders_ri");
  assert.deepEqual(await app.listQueues(), [
    { name: "orders", storageMode: "unpartitioned" },
    { name: "orders_ri", storageMode: "unpartitioned" },
  ]);

  // Re-provisioning an existing queue stays idempotent: its own relations are
  // not a collision with itself.
  await app.createQueue("orders");
  await app.createQueue("orders_ri");
  assert.equal((await app.getQueue("orders"))!.storageMode, "unpartitioned");
});

test("queue names that mimic a generated partition suffix are rejected", async () => {
  const { app, pool } = env;

  await app.createQueue("orders", { storageMode: "partitioned" });
  const { rows } = await pool.query(
    `select c.relname
       from pg_inherits i
       join pg_class c on c.oid = i.inhrelid
       join pg_class p on p.oid = i.inhparent
      where p.relname = 'x_orders'
        and pg_get_expr(c.relpartbound, c.oid) <> 'DEFAULT'
      order by c.relname
      limit 1`,
  );
  const partition: string = rows[0].relname;
  assert.match(partition, /^x_orders_\d{6}$/);

  // "orders_202601" would want the table x_orders_202601 -- which is already
  // one of "orders"'s week partitions. Reject the whole shape up front rather
  // than only when the partition happens to exist yet.
  await assert.rejects(
    app.createQueue(partition.slice("x_".length)),
    /reserved suffix/,
  );
  await assert.rejects(app.createQueue("foo_202601"), /reserved suffix/);
  await assert.rejects(app.createQueue("foo_d"), /reserved suffix/);
  // A partitioned queue's default partition is 'x_<name>_d'.
  await assert.rejects(app.createQueue("orders_d"), /reserved suffix/);

  // Neighbouring shapes are legal: the suffix is exactly six digits, or "d".
  await app.createQueue("foo_20260");
  await app.createQueue("foo_2026011");
  await app.createQueue("foo_d1");
  await app.createQueue("foo_dd");
});

test("a queue name cannot be renamed out from under its storage", async () => {
  const { app, pool } = env;
  await app.createQueue("orders");

  // The name IS the storage identity now, so it is immutable in the same way
  // the id and the storage mode are.
  await assert.rejects(
    pool.query(
      `update otra.queues set name = 'invoices' where name = 'orders'`,
    ),
    /queue storage identity is immutable/,
  );
  assert.equal((await app.getQueue("orders"))!.name, "orders");
  const relations = await pool.query(
    `select count(*)::int as count
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
      where c.relname = 'x_orders'`,
  );
  assert.equal(relations.rows[0].count, 1);

  // A no-op update of an unrelated column still passes the guard.
  await pool.query(
    `update otra.queues set cleanup_limit = 500 where name = 'orders'`,
  );
  assert.equal((await app.getQueuePolicy("orders"))!.cleanupLimit, 500);
});

test("drops a queue's storage and frees the name for a fresh queue", async () => {
  const { app, pool } = env;
  await app.createQueue("orders");

  const before = await pool.query(
    `select id from otra.queues where name = 'orders'`,
  );
  const previousId = before.rows[0].id;

  await app.dropQueue("orders");

  assert.equal(await app.getQueue("orders"), null);
  const relics = await pool.query(
    `select count(*)::int as count
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
      where c.relname like 'x\\_orders%'
         or c.relname like 'p\\_orders%'
         or c.relname like 'e\\_orders%'`,
  );
  assert.equal(relics.rows[0].count, 0);

  // The name is free again -- and because the drop took the physical relations
  // with it, the recreated queue reuses the same names under a new id.
  await app.createQueue("orders");
  const after = await pool.query(
    `select id from otra.queues where name = 'orders'`,
  );
  assert.notEqual(after.rows[0].id, previousId);

  const task = app.task("after-drop", function* () {
    return "ok";
  });
  const execution = await app.spawn(task, null);
  await app.createWorker({ workerId: "w1" }).drain();
  assert.equal(await app.getResult(execution), "ok");
});

test("dropping a partitioned queue removes its partitions and side tables", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });

  await app.dropQueue("archive");

  const relics = await pool.query(
    `select count(*)::int as count
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
      where c.relname like '%archive%'`,
  );
  assert.equal(relics.rows[0].count, 0);
  assert.deepEqual(await app.listQueues(), []);
});

test("refuses to drop a queue with a live execution unless forced", async () => {
  const { app } = env;
  await app.createQueue("orders");
  app.task("parks", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "done";
  });

  const execution = await app.spawn("parks", null);
  await assert.rejects(app.dropQueue("orders"), /non-terminal execution/);

  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");
  await assert.rejects(app.dropQueue("orders"), /non-terminal execution/);

  await app.dropQueue("orders", { force: true });
  assert.equal(await app.getQueue("orders"), null);
});

test("dropping an unknown queue is an error", async () => {
  await assert.rejects(
    env.app.dropQueue("no-such-queue"),
    /Queue "no-such-queue" does not exist/,
  );
});

test("provisions a partitioned queue with its current storage window", async () => {
  const { app, pool } = env;

  await app.createQueue("archive", { storageMode: "partitioned" });

  assert.deepEqual(await app.getQueue("archive"), {
    name: "archive",
    storageMode: "partitioned",
  });

  const { rows } = await pool.query(
    `with relations as (
       select c.oid, c.relname, c.relkind
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
        where c.relname in ('x_archive', 'p_archive', 'e_archive', 'i_archive')
     ), children as (
       select parent.relname, count(*)::int as child_count
         from pg_class parent
         join pg_namespace n on n.oid = parent.relnamespace and n.nspname = 'otra'
         join pg_inherits i on i.inhparent = parent.oid
        where parent.relname in ('x_archive', 'p_archive')
        group by parent.relname
     )
     select
       (select count(*)::int from relations where relkind = 'p') as partitioned_parents,
       (select count(*)::int from relations where relkind = 'r') as ordinary_tables,
       (select bool_and(pg_get_partkeydef(oid) = 'RANGE (root_id)')
          from relations where relkind = 'p') as root_partitioned,
       (select min(child_count) from children) as min_children,
       (select max(child_count) from children) as max_children`,
  );
  assert.deepEqual(rows[0], {
    partitioned_parents: 2,
    ordinary_tables: 2,
    root_partitioned: true,
    min_children: 6,
    max_children: 6,
  });
});

test("runs an effect-free execution from queue-local storage", async () => {
  const { app, pool } = env;

  // (explicit queue: unregistered names without one are rejected client-side)
  await assert.rejects(
    app.spawn("missing-queue-task", null, { queue: "orders" }),
    /Queue "orders" does not exist/,
  );

  for (const storageMode of ["unpartitioned", "partitioned"] as const) {
    const queue = `runtime-${storageMode}`;
    await app.createQueue(queue, { storageMode });
    const local = new Otra({ db: pool, queue });
    const task = local.task(`hello-${storageMode}`, function* () {
      return { storageMode };
    });

    const executions = await Promise.all(
      Array.from({ length: 5 }, () =>
        local.spawn(task, null, { idempotencyKey: "delivery-1" }),
      ),
    );
    const [execution, ...duplicates] = executions as [
      (typeof executions)[number],
      ...(typeof executions)[number][],
    ];
    assert.match(execution.queueId, /^[0-9a-f-]{36}$/);
    assert.equal(execution.rootId, execution.executionId);
    assert.equal(
      duplicates.every(
        (duplicate) =>
          duplicate.queueId === execution.queueId &&
          duplicate.rootId === execution.rootId &&
          duplicate.executionId === execution.executionId,
      ),
      true,
    );

    assert.equal(await local.createWorker().tick(), 1);
    assert.deepEqual(await local.getResult(execution), { storageMode });
    assert.equal((await local.getExecution(execution))!.status, "completed");

    const stored = await pool.query(
      `select
         to_regclass('otra.executions') as shared,
         format('otra.%I', 'x_' || $1::text)::regclass as local_table`,
      [queue],
    );
    assert.equal(stored.rows[0].shared, null);
    const localRows = await pool.query(
      `select count(*)::int as count from ${stored.rows[0].local_table}
        where root_id = $1 and id = $2`,
      [execution.rootId, execution.executionId],
    );
    assert.equal(localRows.rows[0].count, 1);

    await local.close();
  }
});

test("extends a partitioned queue's storage window", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });

  await env.advance(56 * 24 * 60 * 60);
  await Promise.all([
    app.ensurePartitions("archive"),
    app.ensurePartitions("archive"),
    app.ensurePartitions("archive"),
  ]);

  const { rows } = await pool.query(
    `select to_regclass(
       format('otra.%I', 'x_archive_' || otra.partition_week_tag(otra.now()))
     ) is not null as current_week_exists`,
  );
  assert.equal(rows[0].current_week_exists, true);
});

test("updates and reads queue storage policy", async () => {
  const { app } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });

  await app.setQueuePolicy("archive", {
    partitionLookahead: "60 days",
    partitionLookback: "2 days",
    cleanupTtl: "90 days",
    cleanupLimit: 2_000,
    detachMode: "empty",
    detachMinAge: "45 days",
  });

  assert.deepEqual(await app.getQueuePolicy("archive"), {
    name: "archive",
    storageMode: "partitioned",
    defaultPartition: "enabled",
    partitionLookahead: "60 days",
    partitionLookback: "2 days",
    cleanupTtl: "90 days",
    cleanupLimit: 2_000,
    detachMode: "empty",
    detachMinAge: "45 days",
  });
});

test("queue storage mode is immutable and validated", async () => {
  const { app, pool } = env;

  await app.createQueue("archive", { storageMode: "partitioned" });
  await app.createQueue("archive", { storageMode: "partitioned" });
  await assert.rejects(
    app.createQueue("archive"),
    /already exists with storage mode "partitioned"/,
  );

  await app.createQueue("ordinary");
  await assert.rejects(
    app.createQueue("ordinary", { storageMode: "partitioned" }),
    /already exists with storage mode "unpartitioned"/,
  );
  await assert.rejects(
    app.createQueue("invalid", {
      storageMode: "timescale" as "partitioned",
    }),
    /Unsupported queue storage mode "timescale"/,
  );
  await assert.rejects(
    pool.query(
      `update otra.queues set storage_mode = 'partitioned' where name = 'ordinary'`,
    ),
    /queue storage identity is immutable/,
  );
});

test("toggles empty default partitions through queue policy", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });

  const countDefaults = async (): Promise<number> => {
    const { rows } = await pool.query(
      `select count(*)::int as count
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
        where c.relname in ('x_archive_d', 'p_archive_d')`,
    );
    return rows[0].count;
  };

  assert.equal(await countDefaults(), 2);
  await app.setQueuePolicy("archive", { defaultPartition: "disabled" });
  assert.equal(await countDefaults(), 0);
  await app.setQueuePolicy("archive", { defaultPartition: "enabled" });
  assert.equal(await countDefaults(), 2);
});

test("refuses to disable a nonempty default partition", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });
  await env.advance(120 * 24 * 60 * 60);

  const id = (await pool.query(`select otra.uuid_v7() as id`)).rows[0].id;
  await pool.query(
    `insert into otra.x_archive
       (id, root_id, function_name) values ($1, $1, 'out-of-window')`,
    [id],
  );

  await assert.rejects(
    app.setQueuePolicy("archive", { defaultPartition: "disabled" }),
    /default partition .* is not empty/,
  );
  assert.equal(
    (await app.getQueuePolicy("archive"))!.defaultPartition,
    "enabled",
  );
});

test("default maintenance locks parents before default partitions", async () => {
  const { app, pool, connectionString } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });
  await env.advance(120 * 24 * 60 * 60);

  const parent = "x_archive";
  const blocker = new pg.Client({ connectionString });
  const maintainer = new pg.Client({ connectionString });
  await blocker.connect();
  await maintainer.connect();
  const maintainerPid = (
    await maintainer.query<{ pid: number }>(`select pg_backend_pid() as pid`)
  ).rows[0]!.pid;
  try {
    await blocker.query("begin");
    await blocker.query(`lock table otra."${parent}" in row exclusive mode`);

    const maintenance = maintainer.query(
      `select otra.set_queue_policy(
         'archive', '{"default_partition":"disabled"}'::jsonb
       )`,
    );
    await waitFor(
      async () => {
        const waiting = await pool.query(
          `select wait_event_type = 'Lock' as blocked
             from pg_stat_activity where pid = $1`,
          [maintainerPid],
        );
        return waiting.rows[0]?.blocked === true;
      },
      { label: "default maintenance to wait on parent" },
    );

    const id = (await blocker.query(`select otra.uuid_v7() as id`)).rows[0].id;
    await blocker.query(
      `insert into otra."${parent}"
         (id, root_id, function_name) values ($1, $1, 'contended')`,
      [id],
    );
    await blocker.query("commit");

    await assert.rejects(maintenance, /default partition .* is not empty/);
  } finally {
    await blocker.query("rollback").catch(() => undefined);
    await blocker.end();
    await maintainer.end();
  }
});

test("discovers old empty partitions eligible for detach", async () => {
  const { app } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });
  await app.setQueuePolicy("archive", {
    detachMode: "empty",
    detachMinAge: "30 days",
  });
  await env.advance(120 * 24 * 60 * 60);

  const candidates = await app.listDetachCandidates("archive");

  assert.equal(candidates.length, 10);
  assert.equal(
    candidates.every((candidate) => !candidate.partitionTable.endsWith("_d")),
    true,
  );
  assert.deepEqual(
    new Set(candidates.map((candidate) => candidate.queueName)),
    new Set(["archive"]),
  );
});

test("an orphaned promise partition stays visible after its execution sibling is detached", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });
  await app.setQueuePolicy("archive", {
    detachMode: "empty",
    detachMinAge: "30 days",
  });
  await env.advance(120 * 24 * 60 * 60);

  const before = await app.listDetachCandidates("archive");
  assert.equal(before.length, 10);

  const x = "x_archive";
  const p = "p_archive";
  const tag = before
    .map((candidate) => candidate.partitionTable)
    .find((name) => name.startsWith(`${x}_`))!
    .slice(x.length + 1);

  // An operator (or a crash between the pair's two DETACH statements) leaves
  // the promise partition behind. Its x sibling can no longer vouch for it.
  await pool.query(
    `alter table otra."${x}" detach partition otra."${x}_${tag}"`,
  );

  const after = await app.listDetachCandidates("archive");
  assert.equal(
    after.some((candidate) => candidate.partitionTable === `${x}_${tag}`),
    false,
  );
  // The orphan must stay listed -- otherwise it is invisible forever.
  assert.deepEqual(
    after.filter((candidate) => candidate.partitionTable === `${p}_${tag}`),
    [{ queueName: "archive", parentTable: p, partitionTable: `${p}_${tag}` }],
  );
  assert.equal(after.length, 9);
});

test("keeps a promise generation attached while its execution generation is live", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });
  await app.setQueuePolicy("archive", {
    detachMode: "empty",
    detachMinAge: "30 days",
  });

  const id = (await pool.query(`select otra.uuid_v7() as id`)).rows[0].id;
  await pool.query(
    `insert into otra.x_archive
       (id, root_id, function_name) values ($1, $1, 'still-live')`,
    [id],
  );
  await env.advance(120 * 24 * 60 * 60);

  const candidates = await app.listDetachCandidates("archive");

  assert.equal(candidates.length, 8);
  const liveTag = "202601";
  assert.equal(
    candidates.some((candidate) => candidate.partitionTable.endsWith(liveTag)),
    false,
  );
});

test("cleanup expires finished trees and event facts past the TTL", async () => {
  const { app, pool } = env;
  await app.createQueue("orders");
  const task = app.task("receipt", function* (params: { id: number }) {
    return params.id;
  });
  const worker = app.createWorker({ workerId: "w1" });

  const oldA = await app.spawn(task, { id: 1 });
  const oldB = await app.spawn(task, { id: 2 });
  await worker.drain();
  await app.emitEvent("stale-fact", { v: 1 });

  await env.advance(2 * 24 * 60 * 60);

  const young = await app.spawn(task, { id: 3 });
  await worker.drain();
  await app.emitEvent("fresh-fact", { v: 2 });

  const survivors = async (): Promise<number> => {
    const found = await Promise.all(
      [oldA, oldB].map(async (ref) => (await app.getExecution(ref)) !== null),
    );
    return found.filter(Boolean).length;
  };
  const eventNames = async (): Promise<string[]> => {
    const { rows } = await pool.query(
      `select name from otra.e_orders order by name`,
    );
    return rows.map((row: { name: string }) => row.name);
  };

  // No ttl argument: the queue policy's 30 days applies, nothing is due.
  await app.cleanup();
  assert.equal(await survivors(), 2);
  assert.deepEqual(await eventNames(), ["fresh-fact", "stale-fact"]);

  // limit bounds the batch, so only one of the two due trees goes.
  await app.cleanup("orders", { ttl: "1 day", limit: 1 });
  assert.equal(await survivors(), 1);

  await app.cleanup("orders", { ttl: "1 day" });
  assert.equal(await survivors(), 0);
  assert.equal((await app.getExecution(young))!.status, "completed");
  assert.deepEqual(await eventNames(), ["fresh-fact"]);
});

test("queue tables enforce root ownership and cascade complete trees", async () => {
  const { app, pool } = env;

  for (const storageMode of ["unpartitioned", "partitioned"] as const) {
    const name = `tree-${storageMode}`;
    await app.createQueue(name, { storageMode });
    const executions = `x_${name}`;
    const promises = `p_${name}`;
    const ids = (
      await pool.query<{ id: string }>(
        `select otra.uuid_v7() as id from generate_series(1, 4)`,
      )
    ).rows.map((row) => row.id);
    const [root, otherRoot, child, promise] = ids as [
      string,
      string,
      string,
      string,
    ];

    await pool.query(
      `insert into otra."${executions}" (id, root_id, function_name)
       values ($1, $1, 'root'), ($2, $2, 'other-root')`,
      [root, otherRoot],
    );
    await assert.rejects(
      pool.query(
        `insert into otra."${executions}"
           (id, root_id, parent_id, function_name)
         values ($1, $2, $3, 'wrong-root')`,
        [child, root, otherRoot],
      ),
      /foreign key constraint/,
    );
    await assert.rejects(
      pool.query(
        `insert into otra."${promises}"
           (id, root_id, execution_id, key, label, kind)
         values ($1, $2, $3, 'wrong-root', 'wrong-root', 'run')`,
        [promise, root, otherRoot],
      ),
      /foreign key constraint/,
    );

    await pool.query(
      `insert into otra."${executions}"
         (id, root_id, parent_id, function_name)
       values ($1, $2, $2, 'child')`,
      [child, root],
    );
    await pool.query(
      `insert into otra."${promises}"
         (id, root_id, execution_id, key, label, kind)
       values ($1, $2, $3, 'step', 'step', 'run')`,
      [promise, root, child],
    );
    await pool.query(
      `delete from otra."${executions}" where root_id = $1 and id = $1`,
      [root],
    );

    const remaining = await pool.query(
      `select
         (select count(*)::int from otra."${executions}") as executions,
         (select count(*)::int from otra."${promises}") as promises`,
    );
    assert.deepEqual(remaining.rows[0], { executions: 1, promises: 0 });
  }
});

// --- partition maintenance hardening ----------------------------------------

test("rows stranded in the default partition are drained when their week partition arrives", async () => {
  const { app, pool } = env;
  await app.createQueue("orders", { storageMode: "partitioned" });

  // Maintenance lapses: jump far past the 28-day lookahead, so a new root
  // has no week partition and lands in the default partition.
  await env.setNow("2026-04-01T12:00:00Z");
  app.task("late", function* () {
    return "ok";
  });
  const execution = await app.spawn("late", null);

  const x = "x_orders";
  const inDefault = await pool.query(
    `select tableoid::regclass::text as rel from otra.${x} where id = $1`,
    [execution.executionId],
  );
  assert.equal(inDefault.rows[0].rel, `otra.${x}_d`);

  // The old code raised "updated partition constraint for default partition
  // ... would be violated" here, permanently: the queue was wedged.
  await app.ensurePartitions("orders");

  // The row moved into its proper week partition and still runs.
  const after = await pool.query(
    `select tableoid::regclass::text as rel from otra.${x} where id = $1`,
    [execution.executionId],
  );
  assert.notEqual(after.rows[0].rel, `otra.${x}_d`);
  assert.match(after.rows[0].rel, /_\d{6}$/);

  const worker = app.createWorker({ workerId: "w1", queue: "orders" });
  await worker.drain();
  assert.equal((await app.getExecution(execution))!.status, "completed");
});

test("draining the default partition preserves promise history and children", async () => {
  const { app, pool } = env;
  await app.createQueue("orders", { storageMode: "partitioned" });
  await env.setNow("2026-04-01T12:00:00Z");

  const child = app.task("drain-child", function* () {
    return "child-done";
  });
  app.task("drain-parent", function* (_params: null, ctx) {
    yield* ctx.run("step", () => 41);
    const result = yield* ctx.call(child, null);
    return result;
  });
  const execution = await app.spawn("drain-parent", null);
  const worker = app.createWorker({ workerId: "w1", queue: "orders" });
  await worker.drain(); // parent checkpoints, spawns child, child completes

  const before = (await app.getExecution(execution))!;
  assert.equal(before.status, "completed");

  await app.ensurePartitions("orders"); // must move the whole tree intact

  const counts = await pool.query(
    `select
       (select count(*)::int from otra.x_orders where root_id = $1) as executions,
       (select count(*)::int from otra.p_orders where root_id = $1) as promises,
       (select count(*)::int from otra.x_orders_d) as x_default,
       (select count(*)::int from otra.p_orders_d) as p_default`,
    [execution.rootId],
  );
  // Parent + child executions; step + child promise rows; default emptied.
  assert.equal(counts.rows[0].executions, 2);
  assert.equal(counts.rows[0].promises, 2);
  assert.equal(counts.rows[0].x_default, 0);
  assert.equal(counts.rows[0].p_default, 0);
});

test("week stepping is DST-proof: partition bounds are contiguous across spring-forward", async () => {
  const { app, pool, connectionString } = env;
  const client = new pg.Client({ connectionString });
  await client.connect();
  try {
    await client.query("set time zone 'America/New_York'");
    // US DST starts 2026-03-08; the 28-day lookahead spans it.
    await client.query("select otra.set_fake_now('2026-02-25T12:00:00Z')");
    await client.query("select otra.create_queue('dst', 'partitioned')");

    const { rows } = await client.query(`
      select c.relname,
             pg_get_expr(c.relpartbound, c.oid) as bound
        from pg_inherits i
        join pg_class c on c.oid = i.inhrelid
        join pg_class p on p.oid = i.inhparent
       where p.relname = 'x_dst'
         and pg_get_expr(c.relpartbound, c.oid) <> 'DEFAULT'
       order by c.relname`);
    assert.ok(rows.length >= 4, `expected a full window, got ${rows.length}`);
    const bounds = rows.map((r: { bound: string }) => {
      const m = /FROM \('([0-9a-f-]+)'\) TO \('([0-9a-f-]+)'\)/.exec(r.bound);
      assert.ok(m, `unparsable bound: ${r.bound}`);
      return { from: m![1]!, to: m![2]! };
    });
    for (let i = 1; i < bounds.length; i++) {
      // The old session-timezone arithmetic left a 7-day hole here.
      assert.equal(
        bounds[i]!.from,
        bounds[i - 1]!.to,
        `gap between partitions ${i - 1} and ${i}`,
      );
    }
  } finally {
    await client.end();
  }
});

test("cleanup reports what it deleted, so a saturated batch can be re-run", async () => {
  const { app } = env;
  await app.createQueue("orders");
  const task = app.task("receipt", function* (params: { id: number }) {
    return params.id;
  });
  const worker = app.createWorker({ workerId: "w1" });

  await app.spawn(task, { id: 1 });
  await app.spawn(task, { id: 2 });
  await worker.drain();
  await app.emitEvent("stale-fact", { v: 1 });
  await env.advance(2 * 24 * 60 * 60);

  // limit 1 against two due trees: the counts are the only way a caller can
  // tell the batch saturated and another pass is owed.
  assert.deepEqual(await app.cleanup("orders", { ttl: "1 day", limit: 1 }), [
    { queueName: "orders", rootsDeleted: 1, eventsDeleted: 1 },
  ]);
  assert.deepEqual(await app.cleanup("orders", { ttl: "1 day", limit: 1 }), [
    { queueName: "orders", rootsDeleted: 1, eventsDeleted: 0 },
  ]);
  // Drained: zero says stop looping.
  assert.deepEqual(await app.cleanup("orders", { ttl: "1 day", limit: 1 }), [
    { queueName: "orders", rootsDeleted: 0, eventsDeleted: 0 },
  ]);
});

test("cleanup with no queue sweeps every queue under its own TTL policy", async () => {
  const { app } = env;
  await app.createQueue("orders");
  await app.createQueue("archive");
  await app.setQueuePolicy("orders", { cleanupTtl: "1 day" });
  await app.setQueuePolicy("archive", { cleanupTtl: "10 days" });

  const task = app.task("receipt", function* () {
    return 1;
  });
  const inOrders = await app.spawn(task, null, { queue: "orders" });
  const inArchive = await app.spawn(task, null, { queue: "archive" });
  await app.createWorker({ workerId: "w1", queue: "orders" }).drain();
  await app.createWorker({ workerId: "w2", queue: "archive" }).drain();

  await env.advance(2 * 24 * 60 * 60);
  // Each queue is swept under its OWN policy, not one shared TTL: orders is
  // due at 1 day, archive is not due until 10.
  assert.deepEqual(await app.cleanup(), [
    { queueName: "archive", rootsDeleted: 0, eventsDeleted: 0 },
    { queueName: "orders", rootsDeleted: 1, eventsDeleted: 0 },
  ]);
  assert.equal(await app.getExecution(inOrders), null);
  assert.notEqual(await app.getExecution(inArchive), null);

  await env.advance(20 * 24 * 60 * 60);
  assert.deepEqual(await app.cleanup(), [
    { queueName: "archive", rootsDeleted: 1, eventsDeleted: 0 },
    { queueName: "orders", rootsDeleted: 0, eventsDeleted: 0 },
  ]);
  assert.equal(await app.getExecution(inArchive), null);
});

test("drops a detached partition, and refuses everything that is not one", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });
  await app.setQueuePolicy("archive", {
    detachMode: "empty",
    detachMinAge: "30 days",
  });
  await env.advance(120 * 24 * 60 * 60);

  const exists = async (relation: string): Promise<boolean> => {
    const { rows } = await pool.query(
      `select to_regclass(format('otra.%I', $1::text)) is not null as found`,
      [relation],
    );
    return rows[0].found;
  };
  const code = (expected: string) => (err: unknown) => {
    assert.equal((err as { code?: string }).code, expected);
    return true;
  };

  const target = (await app.listDetachCandidates("archive")).find((candidate) =>
    candidate.partitionTable.startsWith("p_archive_"),
  )!;

  // Still attached: dropping it would take live storage out of the parent.
  await assert.rejects(
    app.dropDetachedPartition(target.partitionTable),
    (err: unknown) => {
      code("OT005")(err);
      assert.match((err as Error).message, /still attached/);
      return true;
    },
  );

  await pool.query(
    `alter table otra."${target.parentTable}"
       detach partition otra."${target.partitionTable}"`,
  );
  await app.dropDetachedPartition(target.partitionTable);
  assert.equal(await exists(target.partitionTable), false);

  // Gone now: a second drop is a clear not-found, not a silent success.
  await assert.rejects(
    app.dropDetachedPartition(target.partitionTable),
    code("OT004"),
  );

  // A same-named relation living outside otra is never ours to drop.
  // if-not-exists: the shared-DSN test mode only resets the otra schema, so
  // this decoy survives between runs.
  await pool.query(`create table if not exists public.p_archive_209901 ()`);
  await assert.rejects(
    app.dropDetachedPartition("p_archive_209901"),
    code("OT004"),
  );
  await assert.rejects(
    app.dropDetachedPartition("public.p_archive_209901"),
    (err: unknown) => {
      code("OT003")(err);
      assert.match((err as Error).message, /otra schema/);
      return true;
    },
  );
  assert.equal(
    (
      await pool.query(
        `select to_regclass('public.p_archive_209901') is not null as found`,
      )
    ).rows[0].found,
    true,
  );

  // An otra relation that is not partition-shaped: the queue registry itself,
  // a base table, and a default partition (which set_queue_policy owns).
  for (const name of ["queues", "x_archive", "x_archive_d"]) {
    await assert.rejects(app.dropDetachedPartition(name), (err: unknown) => {
      code("OT003")(err);
      assert.match((err as Error).message, /partition/);
      return true;
    });
  }
  assert.equal(await exists("x_archive"), true);
});

test("the error taxonomy separates not-found from precondition-failed", async () => {
  const { app } = env;
  await app.createQueue("orders");
  const task = app.task("parked", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
  });
  await app.spawn(task, null);

  // OT005: the queue is right there, but its state forbids the operation.
  await assert.rejects(app.dropQueue("orders"), (err: unknown) => {
    assert.equal(isPreconditionFailed(err), true);
    assert.equal(isNotFound(err), false);
    return true;
  });

  // OT004: a different condition entirely -- there is no such queue. Both
  // used to be undifferentiated P0001 (or, for the drop, an overloaded
  // OT003), so no caller could branch on them.
  const missing = (err: unknown): boolean => {
    assert.equal(isNotFound(err), true);
    assert.equal(isPreconditionFailed(err), false);
    return true;
  };
  await assert.rejects(app.dropQueue("no-such-queue"), missing);
  await assert.rejects(app.cleanup("no-such-queue"), missing);
  await assert.rejects(
    app.setQueuePolicy("no-such-queue", { cleanupTtl: "1 day" }),
    missing,
  );
  await assert.rejects(app.ensurePartitions("no-such-queue"), missing);
  // Read-only getters keep answering "nothing", not raising: absence is the
  // answer to the question they were asked.
  assert.equal(await app.getQueuePolicy("no-such-queue"), null);
  assert.equal(await app.getQueue("no-such-queue"), null);

  // Argument validation stays OT003 and is neither of the two.
  await assert.rejects(
    app.setQueuePolicy("orders", { cleanupLimit: 0 }),
    (err: unknown) => {
      assert.equal((err as { code?: string }).code, "OT003");
      assert.equal(isNotFound(err), false);
      assert.equal(isPreconditionFailed(err), false);
      return true;
    },
  );
});
