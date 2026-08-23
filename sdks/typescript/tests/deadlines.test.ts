import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { isCancellation } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// Execution deadlines (absurd's cancellation policy, sql/absurd.sql:947-973
// and :1279-1286).  A blown deadline is a GRACEFUL cancel, never a hard kill:
// the request flag is set exactly like app.cancel would set it, a worker
// delivers CancelledError, and compensation runs.  The sweep lives in
// claim_local next to the timer/expiry sweeps -- there is no scheduler.

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

test("a blown maxDelay cancels an execution that was never claimed", async () => {
  const { app } = env;
  let ran = 0;

  const task = app.task("never-starts", function* (_params: null, ctx) {
    return yield* ctx.run("body", () => {
      ran += 1;
      return "done";
    });
  });

  // Not runnable for an hour, but only allowed to wait a minute for a worker.
  const execution = await app.spawn(task, null, {
    delaySeconds: 3600,
    deadlines: { maxDelaySeconds: 60 },
  });
  const worker = app.createWorker({ workerId: "w1" });

  assert.equal(await worker.tick(), 0);
  assert.equal((await app.getExecution(execution))!.status, "pending");

  await env.advance(61);
  assert.equal(await worker.tick(), 0); // swept, and nothing left to claim

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.match(snapshot.cancelReason ?? "", /exceeded maxDelay/);
  assert.equal(snapshot.error?.name, "CancelledError");
  // Never claimed, so there is no history: request_cancel_local finalizes it
  // in place and no compensation can (or should) run.
  assert.equal(ran, 0);
});

test("a blown maxDuration cancels across a suspension and compensation runs", async () => {
  const { app } = env;
  const events: string[] = [];

  const task = app.task("long-sleeper", function* (_params: null, ctx) {
    try {
      yield* ctx.sleep("1h");
      return "woke";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => {
          events.push("compensated");
          return null;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null, {
    deadlines: { maxDurationSeconds: 1800 },
  });
  const worker = app.createWorker({ workerId: "w1" });

  await worker.tick(); // claims (first_started_at), parks on the 1h sleep
  assert.equal((await app.getExecution(execution))!.status, "suspended");
  assert.deepEqual(events, []);

  await env.advance(1801);
  await worker.drain(); // sweep cancels -> wakes -> delivery -> compensation

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.match(snapshot.cancelReason ?? "", /exceeded maxDuration/);
  assert.deepEqual(events, ["compensated"]);
});

test("a retry that would land past maxDuration cancels with compensation instead", async () => {
  const { app } = env;
  const events: string[] = [];
  let attempts = 0;

  const task = app.task("downstream-outage", function* (_params: null, ctx) {
    try {
      return yield* ctx.run("call-downstream", () => {
        attempts += 1;
        throw new Error("downstream down");
      });
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => {
          events.push("compensated");
          return null;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null, {
    maxAttempts: 20,
    retryStrategy: { kind: "fixed", base_s: 600, max_s: 600 },
    deadlines: { maxDurationSeconds: 900 },
  });
  const worker = app.createWorker({ workerId: "w1" });

  await worker.tick(); // attempt 1 fails; retry at ~t+600 is inside the budget
  let snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "pending");
  assert.equal(snapshot.cancelRequestedAt, null);
  assert.equal(attempts, 1);

  await env.advance(601);
  await worker.drain(); // attempt 2 fails; the next retry would blow the
  // deadline, so the cancel is requested and delivered immediately

  snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.match(snapshot.cancelReason ?? "", /exceeded maxDuration/);
  assert.deepEqual(events, ["compensated"]);
  assert.equal(attempts, 2); // never retried a third time
});

test("an execution with no deadlines is untouched by the deadline sweep", async () => {
  const { app } = env;

  const task = app.task("patient", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "woke";
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");

  await env.advance(7200); // twice the sleep, far past any plausible deadline
  await worker.drain();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "completed");
  assert.equal(snapshot.result, "woke");
  assert.equal(snapshot.cancelRequestedAt, null);
  assert.equal(snapshot.cancelReason, null);
});

test("deadline options are validated at spawn", async () => {
  const { app, pool } = env;
  const task = app.task("guarded", function* (_params: null, ctx) {
    return yield* ctx.run("body", () => "ok");
  });

  await assert.rejects(
    () => app.spawn(task, null, { deadlines: { maxDelaySeconds: 0 } }),
    /max_delay_s/,
  );
  await assert.rejects(
    () => app.spawn(task, null, { deadlines: { maxDurationSeconds: -1 } }),
    /max_duration_s/,
  );
  // Infinity/NaN cannot reach the database through JSON.stringify (they
  // serialize to null), so the finiteness guard is exercised directly.
  await assert.rejects(
    () =>
      pool.query(
        `select * from otra.spawn_local('guarded', 'null'::jsonb, 'default',
           '{"max_duration_s": "Infinity"}'::jsonb)`,
      ),
    /max_duration_s/,
  );

  // A valid pair is persisted verbatim on the execution row.
  const execution = await app.spawn(task, null, {
    deadlines: { maxDelaySeconds: 30, maxDurationSeconds: 90 },
  });
  const { rows } = await pool.query(
    `select max_delay_s, max_duration_s, first_started_at
       from otra.x_default where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].max_delay_s, 30);
  assert.equal(rows[0].max_duration_s, 90);
  assert.equal(rows[0].first_started_at, null);

  // first_started_at is stamped by the FIRST claim only.
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  const { rows: after } = await pool.query(
    `select first_started_at from otra.x_default where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.notEqual(after[0].first_started_at, null);
});
