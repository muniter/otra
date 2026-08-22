import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import pg from "pg";

import { Otra } from "../src/index.ts";
import { createTestEnv, waitFor, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv("orders", false);
});
afterEach(async () => {
  await env.close();
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

  const { rows } = await pool.query(
    `select count(*)::int as table_count
       from otra.queues q
       join pg_class c
         on c.relname in (
           'x_' || replace(q.id::text, '-', ''),
           'p_' || replace(q.id::text, '-', ''),
           'e_' || replace(q.id::text, '-', '')
         )
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
      where q.name = 'orders' and c.relkind = 'r'`,
  );
  assert.equal(rows[0].table_count, 3);
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
  const { app } = env;

  await app.createQueue("Queue Name-1");
  await app.createQueue("pedidos-ñ");
  await app.createQueue(`quoted"queue`);
  await app.createQueue("q".repeat(57));

  assert.deepEqual(await app.listQueues(), [
    { name: "Queue Name-1", storageMode: "unpartitioned" },
    { name: "pedidos-ñ", storageMode: "unpartitioned" },
    { name: "q".repeat(57), storageMode: "unpartitioned" },
    { name: `quoted"queue`, storageMode: "unpartitioned" },
  ]);
  await assert.rejects(app.createQueue(""), /Queue name must be provided/);
  await assert.rejects(
    app.createQueue("q".repeat(58)),
    /too long \(max 57 bytes\)/,
  );
  await assert.rejects(
    app.createQueue("ñ".repeat(29)),
    /too long \(max 57 bytes\)/,
  );
});

test("queue names cannot collide with another queue's physical relations", async () => {
  const { app } = env;

  await app.createQueue("orders");
  await app.createQueue("orders_ri");

  assert.deepEqual(await app.listQueues(), [
    { name: "orders", storageMode: "unpartitioned" },
    { name: "orders_ri", storageMode: "unpartitioned" },
  ]);
});

test("provisions a partitioned queue with its current storage window", async () => {
  const { app, pool } = env;

  await app.createQueue("archive", { storageMode: "partitioned" });

  assert.deepEqual(await app.getQueue("archive"), {
    name: "archive",
    storageMode: "partitioned",
  });

  const { rows } = await pool.query(
    `with queue as (
       select replace(id::text, '-', '') as storage
         from otra.queues
        where name = 'archive'
     ), relations as (
       select c.oid, c.relname, c.relkind
         from queue q
         join pg_class c on c.relname in (
           'x_' || q.storage,
           'p_' || q.storage,
           'e_' || q.storage,
           'i_' || q.storage
         )
         join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
     ), children as (
       select parent.relname, count(*)::int as child_count
         from queue q
         join pg_class parent on parent.relname in (
           'x_' || q.storage,
           'p_' || q.storage
         )
         join pg_namespace n on n.oid = parent.relnamespace and n.nspname = 'otra'
         join pg_inherits i on i.inhparent = parent.oid
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

  await assert.rejects(
    app.spawn("missing-queue-task", null),
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
         format('otra.%I', 'x_' || replace($1::text, '-', ''))::regclass as local_table`,
      [execution.queueId],
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
       format(
         'otra.%I',
         'x_' || replace(q.id::text, '-', '') || '_' ||
         otra.partition_week_tag(otra.now())
       )
     ) is not null as current_week_exists
       from otra.queues q
      where q.name = 'archive'`,
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
         from otra.queues q
         join pg_class c on c.relname in (
           'x_' || replace(q.id::text, '-', '') || '_d',
           'p_' || replace(q.id::text, '-', '') || '_d'
         )
         join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'
        where q.name = 'archive'`,
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

  const queue = await pool.query(
    `select 'x_' || replace(id::text, '-', '') as table_name
       from otra.queues where name = 'archive'`,
  );
  const id = (await pool.query(`select otra.uuid_v7() as id`)).rows[0].id;
  await pool.query(
    `insert into otra."${queue.rows[0].table_name}"
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

  const { rows } = await pool.query(
    `select 'x_' || replace(id::text, '-', '') as parent
       from otra.queues where name = 'archive'`,
  );
  const parent = rows[0].parent;
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
  assert.equal(candidates.every((candidate) => !candidate.partitionTable.endsWith("_d")), true);
  assert.deepEqual(new Set(candidates.map((candidate) => candidate.queueName)), new Set(["archive"]));
});

test("keeps a promise generation attached while its execution generation is live", async () => {
  const { app, pool } = env;
  await app.createQueue("archive", { storageMode: "partitioned" });
  await app.setQueuePolicy("archive", {
    detachMode: "empty",
    detachMinAge: "30 days",
  });

  const queue = await pool.query(
    `select 'x_' || replace(id::text, '-', '') as table_name
       from otra.queues where name = 'archive'`,
  );
  const id = (await pool.query(`select otra.uuid_v7() as id`)).rows[0].id;
  await pool.query(
    `insert into otra."${queue.rows[0].table_name}"
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

test("queue tables enforce root ownership and cascade complete trees", async () => {
  const { app, pool } = env;

  for (const storageMode of ["unpartitioned", "partitioned"] as const) {
    const name = `tree-${storageMode}`;
    await app.createQueue(name, { storageMode });
    const queue = await pool.query(
      `select 'x_' || replace(id::text, '-', '') as executions,
              'p_' || replace(id::text, '-', '') as promises
         from otra.queues where name = $1`,
      [name],
    );
    const { executions, promises } = queue.rows[0];
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
