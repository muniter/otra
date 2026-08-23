import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { createTestEnv, type TestEnv } from "./helpers.ts";
import type { ExecutionRef } from "../src/index.ts";

// Operator retry of a permanently-failed execution (adapted from absurd's
// retry_task, sql/absurd.sql:1335-1455).  Two deliberate differences:
//
//   * ROOT-ONLY.  absurd retries any task; here un-failing a child would
//     contradict the write-once child promise its parent already observed.
//   * The journal is KEPT.  absurd reuses checkpoints only; otra's replay
//     fast-forwards through the whole history, so the execution resumes from
//     the failure point instead of re-running settled work.

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

async function childRef(env: TestEnv, functionName: string): Promise<ExecutionRef> {
  const { rows } = await env.pool.query(
    `select q.id as queue_id, x.root_id, x.id
       from otra.x_default x, otra.queues q
      where q.name = 'default' and x.function_name = $1`,
    [functionName],
  );
  assert.equal(rows.length, 1);
  return {
    queueId: rows[0].queue_id,
    rootId: rows[0].root_id,
    executionId: rows[0].id,
  };
}

test("app.retry resumes a failed root in place and keeps its journal", async () => {
  const { app } = env;
  const calls = { a: 0, b: 0 };
  let downstreamDown = true;

  const task = app.task("two-steps", function* (_params: null, ctx) {
    const a = yield* ctx.run("step-a", () => {
      calls.a += 1;
      return "a";
    });
    const b = yield* ctx.run("step-b", () => {
      calls.b += 1;
      if (downstreamDown) throw new Error("step b down");
      return "b";
    });
    return `${a}/${b}`;
  });

  const execution = await app.spawn(task, null, {
    maxAttempts: 2,
    retryStrategy: { kind: "fixed", base_s: 1 },
  });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // attempt 1 fails
  await env.advance(10);
  await worker.tick(); // attempt 2 fails: out of attempts

  let snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "failed");
  assert.equal(snapshot.attempt, 2);
  assert.deepEqual(calls, { a: 1, b: 2 }); // step-a memoized on attempt 1

  // The operator fixes the dependency and asks for one more go.
  downstreamDown = false;
  const retried = await app.retry(execution);
  assert.equal(retried.executionId, execution.executionId);
  assert.equal(retried.attempt, 2);
  assert.equal(retried.maxAttempts, 3); // at least one more attempt exists

  snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "pending");
  assert.equal(snapshot.finishedAt, null);
  // The error is kept, exactly as an ordinary retry leaves it on the pending
  // row: it is the forensic record of why the operator had to intervene.
  assert.equal(snapshot.error?.message, "step b down");

  await worker.drain();
  assert.equal(await app.getResult(execution), "a/b");
  // The journal survived the retry: step-a was never re-executed.
  assert.deepEqual(calls, { a: 1, b: 3 });
});

test("app.retry refuses a child execution: its parent already saw the promise settle", async () => {
  const { app } = env;

  const child = app.task(
    { name: "doomed-child", maxAttempts: 1 },
    function* (_params: null, ctx) {
      yield* ctx.run("boom", () => {
        throw new Error("child down");
      });
    },
  );
  const parent = app.task("parent", function* (_params: null, ctx) {
    return yield* ctx.call(child, null);
  });

  const execution = await app.spawn(parent, null, { maxAttempts: 1 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();

  const ref = await childRef(env, "doomed-child");
  const { rows } = await env.pool.query(
    `select status from otra.x_default where root_id = $1 and id = $2`,
    [ref.rootId, ref.executionId],
  );
  assert.equal(rows[0].status, "failed");

  await assert.rejects(() => app.retry(ref), /root/i);
  assert.equal((await app.getExecution(execution))!.status, "failed");
});

test("app.retry refuses completed and cancelled executions", async () => {
  const { app } = env;

  const ok = app.task("fine", function* (_params: null, ctx) {
    return yield* ctx.run("work", () => "done");
  });
  const sleeper = app.task("sleeper", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "woke";
  });

  const completed = await app.spawn(ok, null);
  const cancelled = await app.spawn(sleeper, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  assert.equal((await app.getExecution(completed))!.status, "completed");

  await app.cancel(cancelled, { reason: "operator" });
  await worker.drain();
  assert.equal((await app.getExecution(cancelled))!.status, "cancelled");

  await assert.rejects(() => app.retry(completed), /completed/);
  // Cancellation owns its outcome: compensation already ran, so 'cancelled'
  // is terminal even for an operator.
  await assert.rejects(() => app.retry(cancelled), /cancelled/);
});

test("app.retry honors a maxAttempts override", async () => {
  const { app, pool } = env;

  const task = app.task("hopeless", function* (_params: null, ctx) {
    yield* ctx.run("boom", () => {
      throw new Error("nope");
    });
  });

  const execution = await app.spawn(task, null, {
    maxAttempts: 2,
    retryStrategy: { kind: "fixed", base_s: 1 },
  });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await env.advance(10);
  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "failed");

  const retried = await app.retry(execution, { maxAttempts: 7 });
  assert.equal(retried.maxAttempts, 7);
  const { rows } = await pool.query(
    `select max_attempts, status from otra.x_default
      where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].max_attempts, 7);
  assert.equal(rows[0].status, "pending");

  // An override that leaves no attempt to make is a refusal, not a no-op.
  for (let i = 0; i < 6; i++) {
    await worker.tick(); // burns attempts 3..7
    await env.advance(10);
  }
  const spent = (await app.getExecution(execution))!;
  assert.equal(spent.status, "failed");
  assert.equal(spent.attempt, 7);
  await assert.rejects(() => app.retry(execution, { maxAttempts: 2 }), /2/);
});
