import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { isCancellation } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// Cancellation v2: suspending compensation. Delivery is journaled as a
// promise row (kind 'cancel', key '$cancel') recording WHERE CancelledError
// was thrown, so compensation may itself suspend -- call durable children,
// sleep, wait -- and every replay re-delivers at the same yield. "Everything
// that determines a replay's path must be journal; now cancellation is."

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

test("compensation can call a durable child task and await it", async () => {
  const { app, pool } = env;
  let refunds = 0;

  const refund = app.task(
    "refund",
    function* (params: { chargeId: string }, ctx) {
      return yield* ctx.run("refund-call", () => {
        refunds += 1;
        return `refunded:${params.chargeId}`;
      });
    },
  );

  const task = app.task("order", function* (_params: null, ctx) {
    const charge = yield* ctx.run("charge", () => "ch_1");
    try {
      yield* ctx.sleep("30d");
      return "shipped";
    } catch (err) {
      if (isCancellation(err)) {
        // The whole point of v2: compensation as a durable child, with its
        // own retries and fault domain, awaited across a real suspension.
        const receipt = yield* ctx.call(refund, { chargeId: charge });
        yield* ctx.run("log-receipt", () => receipt);
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // parked on the 30d sleep
  await app.cancel(execution, { cascade: false });
  await worker.drain(); // deliver -> spawn refund -> park -> refund runs -> resume

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.equal(refunds, 1);

  // The delivery point is journal, and the compensation is checkpointed.
  const storage = execution.queueId.replaceAll("-", "");
  const { rows } = await pool.query(
    `select key, kind, status from otra.p_${storage}
      where root_id = $1 and execution_id = $2 order by key`,
    [execution.rootId, execution.executionId],
  );
  const byKey = new Map(
    rows.map((r: { key: string; kind: string }) => [r.key, r.kind]),
  );
  assert.equal(byKey.get("$cancel"), "cancel");
  assert.ok(byKey.has("log-receipt"));
  const { rows: child } = await pool.query(
    `select status from otra.x_${storage} where function_name = 'refund'`,
  );
  assert.equal(child[0].status, "completed");
});

test("compensation can sleep", async () => {
  const { app } = env;
  const order: string[] = [];

  const task = app.task("backoff-cleanup", function* (_params: null, ctx) {
    try {
      yield* ctx.sleep("30d");
      return "done";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("first-half", () => order.push("first"));
        yield* ctx.sleep("5m"); // settle window before the second half
        yield* ctx.run("second-half", () => order.push("second"));
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await app.cancel(execution);
  await worker.tick(); // delivers; runs first-half; parks on the 5m sleep
  assert.equal((await app.getExecution(execution))!.status, "suspended");
  assert.deepEqual(order, ["first"]);

  await env.advance(301);
  await worker.tick(); // replay re-delivers at the same yield, resumes cleanup

  assert.equal((await app.getExecution(execution))!.status, "cancelled");
  assert.deepEqual(order, ["first", "second"]);
});

test("the delivery point holds even if the forward promise settles later", async () => {
  const { app } = env;
  let forwardResumed = false;
  let token: string | undefined;

  const task = app.task("stable-point", function* (_params: null, ctx) {
    const p = yield* ctx.promise<string>("go-ahead");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    try {
      const v = yield* ctx.await(p);
      forwardResumed = true; // must NEVER run once cancellation was delivered
      return v;
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.sleep("1m"); // suspending compensation forces a replay
        yield* ctx.run("unwound", () => "ok");
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // parked awaiting the external promise
  await app.cancel(execution);
  await worker.tick(); // delivered AT the await; parked on the 1m sleep

  // The forward promise resolves while compensation is parked. Without the
  // journaled delivery point, the replay would see it resolved and take the
  // forward path -- completing an execution that already began unwinding.
  assert.equal(await app.resolvePromise(token!, "too-late"), true);
  await env.advance(61);
  await worker.tick();

  assert.equal((await app.getExecution(execution))!.status, "cancelled");
  assert.equal(forwardResumed, false);
});

test("a failing compensation step retries and resumes compensation", async () => {
  const { app } = env;
  const calls = { done: 0, flaky: 0 };

  const task = app.task("flaky-cleanup", function* (_params: null, ctx) {
    try {
      yield* ctx.sleep("30d");
      return "done";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("cleanup-a", () => {
          calls.done += 1;
        });
        yield* ctx.run("cleanup-flaky", () => {
          calls.flaky += 1;
          if (calls.flaky < 2) throw new Error("transient");
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null, { maxAttempts: 5 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await app.cancel(execution);
  await worker.tick(); // delivers; cleanup-a records; cleanup-flaky fails -> retry
  assert.equal((await app.getExecution(execution))!.status, "pending");

  await env.advance(2); // past the retry backoff
  await worker.tick(); // replay re-delivers; cleanup-a memoized; flaky succeeds

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.equal(snapshot.attempt, 1); // one recorded failed attempt
  assert.deepEqual(calls, { done: 1, flaky: 2 }); // -a never re-executed
});

test("compensation exhausting its attempts still finalizes as cancelled, never failed", async () => {
  const { app } = env;
  let tries = 0;

  const task = app.task("hopeless-cleanup", function* (_params: null, ctx) {
    try {
      yield* ctx.sleep("30d");
      return "done";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("always-fails", () => {
          tries += 1;
          throw new Error("cleanup broken");
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null, {
    maxAttempts: 3,
    retryStrategy: { kind: "fixed", base_s: 1 },
  });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await app.cancel(execution);
  for (let i = 0; i < 5; i++) {
    await worker.tick();
    await env.advance(2);
  }

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled"); // the outcome cancel owns
  assert.equal(snapshot.error?.message, "cleanup broken");
  assert.ok(tries >= 2);
});
