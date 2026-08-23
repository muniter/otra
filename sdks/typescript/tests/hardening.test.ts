import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

test("a zombie worker cannot fail an execution it does not own", async () => {
  const { app, pool } = env;
  app.task("victim", function* (_params: null, ctx) {
    return yield* ctx.run("noop", () => 1);
  });
  const execution = await app.spawn("victim", null);

  // A live worker claims it...
  const { rows: claimed } = await pool.query(
    "select * from otra.claim_local('default', 'worker-live', 30, 1)",
  );
  assert.equal(claimed.length, 1);

  // ...and a worker whose lease was stolen (or never existed) reports a
  // failure.  This must be a no-op: the live worker owns the execution.
  await pool.query(
    `select * from otra.fail_attempt_local(
       $1, $2, $3, 'worker-zombie', '{"message":"zombie"}'::jsonb, true
     )`,
    [execution.queueId, execution.rootId, execution.executionId],
  );

  const { rows } = await pool.query(
    `select status, claimed_by, attempt
       from otra.x_default
      where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].status, "running");
  assert.equal(rows[0].claimed_by, "worker-live");
  assert.equal(rows[0].attempt, 0);
});

test("cleanup deletes finished execution trees and old events", async () => {
  const { app, pool } = env;

  const child = app.task("tree-child", function* (_params: null, ctx) {
    return yield* ctx.run("c", () => "ok");
  });
  const parent = app.task("tree-parent", function* (_params: null, ctx) {
    return yield* ctx.call(child, null);
  });

  const execution = await app.spawn(parent, null);
  await app.emitEvent("stale-event", { n: 1 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  assert.equal((await app.getExecution(execution))!.status, "completed");

  await env.advance(100 * 86400);
  await pool.query("select otra.cleanup_local('default', interval '30 days')");

  const { rows: executions } = await pool.query(
    `select count(*)::int as n from otra.x_default`,
  );
  const { rows: promises } = await pool.query(
    `select count(*)::int as n from otra.p_default`,
  );
  const { rows: events } = await pool.query(
    `select count(*)::int as n from otra.e_default`,
  );
  assert.equal(executions[0].n, 0);
  assert.equal(promises[0].n, 0);
  assert.equal(events[0].n, 0);
});

test("claiming one queue does not fire another queue's timers", async () => {
  const { app, pool } = env;
  await app.createQueue("queue-a");
  await app.createQueue("queue-b");

  const { rows: spawned } = await pool.query(
    "select queue_id, root_id, execution_id from otra.spawn_local('sleeper', '{}'::jsonb, 'queue-b')",
  );
  const execution = spawned[0];
  await pool.query("select * from otra.claim_local('queue-b', 'w1', 30, 1)");
  await pool.query(
    "select * from otra.create_sleep_local($1, $2, $3, 'w1', 's1', '$sleep', 60)",
    [execution.queue_id, execution.root_id, execution.execution_id],
  );
  await pool.query("select otra.suspend_local($1, $2, $3, 'w1', array['s1'])", [
    execution.queue_id,
    execution.root_id,
    execution.execution_id,
  ]);

  await env.advance(61);

  // A worker on queue-a sweeps; queue-b's timer is not its business.
  await pool.query("select * from otra.claim_local('queue-a', 'w2', 30, 5)");
  const { rows: after } = await pool.query(
    `select status from otra."p_queue-b"
      where root_id = $1 and execution_id = $2 and key = 's1'`,
    [execution.root_id, execution.execution_id],
  );
  assert.equal(after[0].status, "pending");

  // A worker on queue-b fires it and claims the woken execution.
  const { rows: claimed } = await pool.query(
    "select * from otra.claim_local('queue-b', 'w1', 30, 5)",
  );
  assert.equal(claimed.length, 1);
  assert.equal(claimed[0].execution_id, execution.execution_id);
});

test("exponential backoff saturates instead of overflowing at high attempts", async () => {
  const { pool } = env;
  const { rows } = await pool.query(
    `select extract(epoch from otra._backoff(
       '{"kind": "exponential", "base_s": 1, "factor": 2, "max_s": 300}'::jsonb,
       100000
     ))::int as seconds`,
  );
  assert.equal(rows[0].seconds, 300);
});

test("top-level spawns with an idempotency key never duplicate", async () => {
  const { app, pool } = env;
  const task = app.task("webhook-handler", function* (_params: null, ctx) {
    return yield* ctx.run("handle", () => "handled");
  });

  // A webhook redelivery storm: same key, concurrent spawns.
  const spawns = await Promise.all(
    Array.from({ length: 8 }, () =>
      app.spawn(task, null, { idempotencyKey: "delivery-123" }),
    ),
  );
  const ids = new Set(spawns.map((s) => s.executionId));
  assert.equal(ids.size, 1);

  const { rows } = await pool.query(
    `select count(*)::int as n
       from otra.x_default
      where function_name = 'webhook-handler'`,
  );
  assert.equal(rows[0].n, 1);

  // A different key spawns a fresh execution.
  const other = await app.spawn(task, null, { idempotencyKey: "delivery-456" });
  assert.ok(!ids.has(other.executionId));
});

test("two workers racing over one queue never double-execute", async () => {
  const { app } = env;
  const executed = new Map<number, number>();

  const task = app.task("stress", function* (params: { i: number }, ctx) {
    return yield* ctx.run("mark", () => {
      executed.set(params.i, (executed.get(params.i) ?? 0) + 1);
      return params.i;
    });
  });

  const count = 30;
  const spawned = await Promise.all(
    Array.from({ length: count }, (_, i) => app.spawn(task, { i })),
  );

  const w1 = app.createWorker({ workerId: "wa", batchSize: 3 });
  const w2 = app.createWorker({ workerId: "wb", batchSize: 3 });
  await Promise.all([w1.drain(), w2.drain()]);

  for (const execution of spawned) {
    assert.equal((await app.getExecution(execution))!.status, "completed");
  }
  assert.equal(executed.size, count);
  for (const [i, times] of executed) {
    assert.equal(times, 1, `task ${i} executed ${times} times`);
  }
});
