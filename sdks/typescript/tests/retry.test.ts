import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { TaskError } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

test("a failing step retries the task with backoff; finished steps are skipped", async () => {
  const { app } = env;
  const calls = { stable: 0, flaky: 0 };

  const task = app.task("flaky", function* (_params: null, ctx) {
    const stable = yield* ctx.run("stable", () => {
      calls.stable += 1;
      return "stable-value";
    });
    const flaky = yield* ctx.run("flaky", () => {
      calls.flaky += 1;
      if (calls.flaky < 3) throw new Error(`flake #${calls.flaky}`);
      return "finally";
    });
    return `${stable}/${flaky}`;
  });

  const execution = await app.spawn(task, null, {
    retryStrategy: { kind: "exponential", base_s: 1, factor: 2, max_s: 300 },
  });
  const worker = app.createWorker({ workerId: "w1" });

  await worker.tick(); // attempt 1 fails
  let snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "pending");
  assert.equal(snapshot.attempt, 1);
  assert.equal(await worker.tick(), 0); // backoff not elapsed

  await env.advance(2); // past 1s backoff
  await worker.tick(); // attempt 2 fails
  snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.attempt, 2);

  await env.advance(3); // past 2s backoff
  await worker.tick(); // attempt 3 succeeds
  assert.equal(await app.getResult(execution), "stable-value/finally");
  // The stable step ran exactly once; only the flaky step re-executed.
  assert.deepEqual(calls, { stable: 1, flaky: 3 });
});

test("attempts are bounded by maxAttempts", async () => {
  const { app } = env;
  let calls = 0;

  const task = app.task("hopeless", function* (_params: null, ctx) {
    yield* ctx.run("always-fails", () => {
      calls += 1;
      throw new Error("nope");
    });
  });

  const execution = await app.spawn(task, null, {
    maxAttempts: 3,
    retryStrategy: { kind: "fixed", base_s: 1 },
  });
  const worker = app.createWorker({ workerId: "w1" });

  for (let i = 0; i < 5; i++) {
    await worker.tick();
    await env.advance(2);
  }

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "failed");
  assert.equal(snapshot.attempt, 3);
  assert.equal(calls, 3);
  assert.equal(snapshot.error?.message, "nope");
});

test("TaskError(retryable=false) fails immediately", async () => {
  const { app } = env;
  let calls = 0;

  const task = app.task("fatal", function* (_params: null, ctx) {
    yield* ctx.run("die", () => {
      calls += 1;
      throw new TaskError("unrecoverable input", false);
    });
  });

  const execution = await app.spawn(task, null, { maxAttempts: 5 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  await env.advance(600);
  await worker.drain();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "failed");
  assert.equal(calls, 1);
});

test("a crashed worker's claim expires and another worker resumes the task", async () => {
  const { app, pool } = env;
  const calls = { before: 0, after: 0 };

  const task = app.task("crashy", function* (_params: null, ctx) {
    const a = yield* ctx.run("before-crash", () => {
      calls.before += 1;
      return "checkpointed";
    });
    const b = yield* ctx.run("after-crash", () => {
      calls.after += 1;
      return "recovered";
    });
    return `${a}/${b}`;
  });

  const execution = await app.spawn(task, null);

  // Worker A claims the execution, writes the first checkpoint, then "dies"
  // (we simulate the crash by doing both directly, without ever finishing).
  await pool.query(
    "select * from otra.claim_local('default', 'worker-a', 30, 1)",
  );
  await pool.query(
    `select otra.record_run_local(
       $1, $2, $3, 'worker-a', 'before-crash', 'before-crash',
       '\"checkpointed\"'::jsonb, 30
     )`,
    [execution.queueId, execution.rootId, execution.executionId],
  );

  const workerB = app.createWorker({ workerId: "worker-b" });
  assert.equal(await workerB.tick(), 0); // claim still held

  await env.advance(31); // worker A's claim expires
  await workerB.tick(); // sweep converts the expiry into a failed attempt
  await env.advance(2); // past the retry backoff
  await workerB.tick();

  assert.equal(await app.getResult(execution), "checkpointed/recovered");
  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.attempt, 1); // the crash consumed one attempt
  // Worker B replayed from the top but reused worker A's checkpoint.
  assert.deepEqual(calls, { before: 0, after: 1 });
});
