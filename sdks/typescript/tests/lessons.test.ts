import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import pg from "pg";

import type { ExecutionRef, TaskHandler } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// Regression tests for bug classes mined from absurd's commit history.
// Each comment cites the absurd commit that motivated the test.

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

/** Spawn + claim one execution for worker w1 and return its route. */
async function claimedExecution(
  pool: pg.Pool,
  worker = "w1",
): Promise<ExecutionRef> {
  const { rows: spawned } = await pool.query(
    "select queue_id, root_id, execution_id from otra.spawn_local('subject', '{}'::jsonb, 'default')",
  );
  const { rows: claimed } = await pool.query(
    "select execution_id from otra.claim_local('default', $1, 30, 1)",
    [worker],
  );
  assert.equal(claimed[0].execution_id, spawned[0].execution_id);
  return {
    queueId: spawned[0].queue_id,
    rootId: spawned[0].root_id,
    executionId: spawned[0].execution_id,
  };
}

/** Steal w1's claim: expire it, sweep, wait out the backoff, claim as w2. */
async function stealClaim(
  env: TestEnv,
  execution: ExecutionRef,
): Promise<void> {
  await env.advance(31);
  await env.pool.query(
    "select * from otra.claim_local('default', 'w2', 30, 5)",
  );
  await env.advance(2);
  const { rows } = await env.pool.query(
    "select execution_id from otra.claim_local('default', 'w2', 30, 5)",
  );
  assert.equal(rows[0]?.execution_id, execution.executionId);
}

test("await/emit race cannot lose the wakeup (absurd bcde0df, #61)", async () => {
  const { pool } = env;
  const execution = await claimedExecution(pool);

  const a = new pg.Client({ connectionString: env.connectionString });
  const b = new pg.Client({ connectionString: env.connectionString });
  await a.connect();
  await b.connect();
  try {
    // Interleaving that loses the wakeup without a serialization point:
    // the awaiter has read the event cache (empty) but not yet committed
    // its pending promise when the emit lands.
    await a.query("begin");
    await a.query(
      "select * from otra.create_event_wait_local($1, $2, $3, 'w1', 'e1', '$event', 'race-evt', null)",
      [execution.queueId, execution.rootId, execution.executionId],
    );
    const emit = b.query(
      `select otra.emit_event_local('default', 'race-evt', '{"v": 1}'::jsonb)`,
    );
    await new Promise((resolve) => setTimeout(resolve, 150));
    await a.query("commit");
    await emit;

    const { rows } = await pool.query(
      `select status, value
         from otra.p_${execution.queueId.replaceAll("-", "")}
        where root_id = $1 and execution_id = $2 and key = 'e1'`,
      [execution.rootId, execution.executionId],
    );
    assert.equal(rows[0].status, "resolved");
    assert.deepEqual(rows[0].value, { v: 1 });
  } finally {
    await a.end();
    await b.end();
  }
});

test("a zombie worker cannot write checkpoints into a stolen history (absurd 2ecfbc4)", async () => {
  const { pool } = env;
  const execution = await claimedExecution(pool, "w1");
  await stealClaim(env, execution);

  // w1 is now a zombie; its side-effect result must not enter the history.
  await assert.rejects(
    pool.query(
      `select otra.record_run_local($1, $2, $3, 'w1', 'step', 'step', '"zombie-value"'::jsonb, 30)`,
      [execution.queueId, execution.rootId, execution.executionId],
    ),
    (err: { code?: string }) => err.code === "OT001",
  );
  const { rows } = await pool.query(
    `select count(*)::int as n
       from otra.p_${execution.queueId.replaceAll("-", "")}
      where root_id = $1 and execution_id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].n, 0);
});

test("a zombie worker cannot spawn children for a stolen parent", async () => {
  const { pool } = env;
  const execution = await claimedExecution(pool, "w1");
  await stealClaim(env, execution);

  await assert.rejects(
    pool.query(
      "select * from otra.spawn_child_local($1, $2, $3, 'w1', 'child-1', 'child-fn', 'child-fn', '{}'::jsonb)",
      [execution.queueId, execution.rootId, execution.executionId],
    ),
    (err: { code?: string }) => err.code === "OT001",
  );
  const { rows } = await pool.query(
    `select count(*)::int as n
       from otra.x_${execution.queueId.replaceAll("-", "")}
      where function_name = 'child-fn'`,
  );
  assert.equal(rows[0].n, 0);
});

test("retry strategies are validated at spawn time (absurd 866480d)", async () => {
  const { pool } = env;
  // Negative base: would schedule retries in the past (zero-backoff loop).
  await assert.rejects(
    pool.query(
      `select * from otra.spawn_local('t', null, 'default',
         '{"retry_strategy": {"kind": "fixed", "base_s": -60}}'::jsonb)`,
    ),
    (err: { code?: string }) => err.code === "OT003",
  );
  // Unknown kind.
  await assert.rejects(
    pool.query(
      `select * from otra.spawn_local('t', null, 'default',
         '{"retry_strategy": {"kind": "fibonacci"}}'::jsonb)`,
    ),
    (err: { code?: string }) => err.code === "OT003",
  );
  // Not an object at all.
  await assert.rejects(
    pool.query(
      `select * from otra.spawn_local('t', null, 'default',
         '{"retry_strategy": "garbage"}'::jsonb)`,
    ),
    (err: { code?: string }) => err.code === "OT003",
  );
});

test("backoff is hard-capped at one day regardless of max_s", async () => {
  const { pool } = env;
  const { rows } = await pool.query(
    `select extract(epoch from otra._backoff(
       '{"kind": "exponential", "base_s": 1, "factor": 2, "max_s": 1000000}'::jsonb,
       100
     ))::int as seconds`,
  );
  assert.equal(rows[0].seconds, 86400);
});

test("a poisoned legacy retry strategy fails its task without wedging claim()", async () => {
  const { pool } = env;
  const execution = await claimedExecution(pool, "w1");
  // Simulate a legacy/corrupt row that predates spawn-time validation.
  await pool.query(
    `update otra.x_${execution.queueId.replaceAll("-", "")}
        set retry_strategy = '{"kind": "exponential", "base_s": "abc"}'::jsonb
      where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  await env.advance(31);

  // The sweep hits the poisoned row; claim() must survive (absurd's fix:
  // a bad strategy fails that task permanently, never blocks the queue).
  await pool.query("select * from otra.claim_local('default', 'w2', 30, 5)");
  const { rows } = await pool.query(
    `select status from otra.x_${execution.queueId.replaceAll("-", "")}
      where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].status, "failed");
});

test("cleanup is bounded by a batch limit", async () => {
  const { app, pool } = env;
  const task = app.task("quick", function* (_params: null, ctx) {
    return yield* ctx.run("r", () => 1);
  });
  const executions = [];
  for (let i = 0; i < 5; i++) executions.push(await app.spawn(task, null));
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();

  await env.advance(100 * 86400);
  await pool.query(
    "select otra.cleanup_local('default', interval '30 days', 2)",
  );
  const { rows } = await pool.query(
    `select count(*)::int as n
       from otra.x_${executions[0]!.queueId.replaceAll("-", "")}`,
  );
  assert.equal(rows[0].n, 3);
});

test("cleanup never deletes a tree with live descendants (fire-and-forget child)", async () => {
  const { app, pool } = env;
  const child = app.task("lingering-child", function* (_params: null, ctx) {
    yield* ctx.sleep("200d");
    return "eventually";
  });
  const parent = app.task("forgetful-parent", function* (_params: null, ctx) {
    yield* ctx.spawn(child, null); // fire and forget
    return "done";
  });

  const execution = await app.spawn(parent, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  assert.equal((await app.getExecution(execution))!.status, "completed");

  await env.advance(100 * 86400);
  await pool.query(
    "select otra.cleanup_local('default', interval '30 days', 100)",
  );

  // Parent finished 100 days ago, but its child is still suspended: the
  // whole tree must survive until the subtree is terminal.
  const { rows } = await pool.query(
    `select count(*)::int as n
       from otra.x_${execution.queueId.replaceAll("-", "")}`,
  );
  assert.equal(rows[0].n, 2);
});

test("claim rejects a non-positive lease", async () => {
  const { pool } = env;
  await assert.rejects(
    pool.query("select * from otra.claim_local('default', 'w1', 0, 1)"),
    (err: { code?: string }) => err.code === "OT003",
  );
});

test("an error escaping the driver fails the attempt instead of stranding the claim (absurd 4aec33e)", async () => {
  const { app } = env;
  // A handler that is not a generator: calling it throws synchronously,
  // which escapes driveOnce entirely (infrastructure-error path).
  app.task("broken", (() => {
    throw new Error("sync boom");
  }) as unknown as TaskHandler<null, never>);

  const execution = await app.spawn("broken", null);
  const worker = app.createWorker({
    workerId: "w1",
    onError: () => {}, // keep the test log quiet
  });
  await worker.tick();

  const snapshot = (await app.getExecution(execution))!;
  // Pre-fix behavior: status stays "running" with a live claim, and recovery
  // costs a full lease timeout. It must instead be a recorded failed attempt.
  assert.equal(snapshot.status, "pending");
  assert.equal(snapshot.attempt, 1);
  assert.equal(snapshot.error?.message, "sync boom");
});
