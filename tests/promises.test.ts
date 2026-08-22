import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { TimeoutError, isCancellation } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// External promises (ctx.promise): the sixth promise kind, per the Solfège
// design review. One new ctx method returning a normal handle plus a token;
// outside code settles exactly that promise with app.resolvePromise /
// rejectPromise. No new redemption verbs -- ctx.await / ctx.all redeem the
// handle like any child handle.

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

test("human-in-the-loop: outside code resolves a promise by token", async () => {
  const { app } = env;
  let handedOut: string | undefined;

  const task = app.task("approve-expense", function* (_params: null, ctx) {
    const approval = yield* ctx.promise<{ approvedBy: string }>("approval");
    yield* ctx.run("notify", () => {
      handedOut = approval.token; // the token travels through the real world
    });
    const decision = yield* ctx.await(approval);
    return `approved by ${decision.approvedBy}`;
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  assert.equal((await app.getExecution(executionId))!.status, "suspended");
  assert.ok(handedOut!.startsWith("otr_"), `token looks opaque: ${handedOut}`);

  const settled = await app.resolvePromise(handedOut!, { approvedBy: "hazel" });
  assert.equal(settled, true);
  await worker.tick();
  assert.equal(await app.getResult(executionId), "approved by hazel");
});

test("external promises are write-once", async () => {
  const { app } = env;
  let token: string | undefined;

  const task = app.task("once-only", function* (_params: null, ctx) {
    const p = yield* ctx.promise<{ n: number }>("slot");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    return (yield* ctx.await(p)).n;
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  assert.equal(await app.resolvePromise(token!, { n: 1 }), true);
  // The second settle loses: write-once, first value is canonical.
  assert.equal(await app.resolvePromise(token!, { n: 2 }), false);
  assert.equal(await app.rejectPromise(token!, "too late"), false);

  await worker.tick();
  assert.equal(await app.getResult(executionId), 1);
});

test("rejecting an external promise throws at the await, catchably", async () => {
  const { app } = env;
  let token: string | undefined;

  const task = app.task("deniable", function* (_params: null, ctx) {
    const p = yield* ctx.promise("request");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    try {
      yield* ctx.await(p);
      return "granted";
    } catch (err) {
      return `denied: ${(err as Error).message}`;
    }
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  assert.equal(await app.rejectPromise(token!, "budget exceeded"), true);
  await worker.tick();
  assert.equal(await app.getResult(executionId), "denied: budget exceeded");
});

test("external promises can time out, rejecting with TimeoutError", async () => {
  const { app } = env;

  const task = app.task("impatient", function* (_params: null, ctx) {
    const p = yield* ctx.promise("approval", { timeout: "1h" });
    try {
      yield* ctx.await(p);
      return "arrived";
    } catch (err) {
      if (err instanceof TimeoutError) return "gave-up";
      throw err;
    }
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(executionId))!.status, "suspended");

  await env.advance(3601);
  await worker.tick();
  assert.equal(await app.getResult(executionId), "gave-up");
});

test("resolve-then-await: a promise settled while the task was elsewhere injects without suspending", async () => {
  const { app } = env;
  let token: string | undefined;

  const task = app.task("early-settle", function* (_params: null, ctx) {
    const p = yield* ctx.promise<{ v: string }>("slot");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    yield* ctx.sleep("10s"); // suspends on the timer, not on the promise
    const value = yield* ctx.await(p);
    return value.v;
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // parked on the sleep

  assert.equal(await app.resolvePromise(token!, { v: "early" }), true);
  await env.advance(11);
  await worker.tick(); // timer due; replay finds the promise already resolved
  assert.equal(await app.getResult(executionId), "early");
});

test("tokens are replay-stable and ctx.all mixes external and child handles", async () => {
  const { app } = env;
  const seenTokens: string[] = [];
  let token: string | undefined;

  const child = app.task("mix-child", function* (_params: null, ctx) {
    return yield* ctx.run("c", () => "from-child");
  });

  const task = app.task("mixer", function* (_params: null, ctx) {
    const external = yield* ctx.promise<string>("signal");
    seenTokens.push(external.token); // generator body: runs once per replay
    yield* ctx.run("leak", () => {
      token = external.token;
    });
    const childHandle = yield* ctx.spawn(child, null);
    const [fromOutside, fromChild] = yield* ctx.all([external, childHandle]);
    return `${fromOutside}+${fromChild}`;
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain(); // child completes; parent still blocked on external

  assert.equal((await app.getExecution(executionId))!.status, "suspended");
  await app.resolvePromise(token!, "from-outside");
  await worker.drain();

  assert.equal(await app.getResult(executionId), "from-outside+from-child");
  // The body replayed at least twice; the token never changed.
  assert.ok(seenTokens.length >= 2);
  assert.equal(new Set(seenTokens).size, 1);
});

test("cancelling an execution parked on an external promise unwinds it", async () => {
  const { app } = env;

  const task = app.task("cancellable-waiter", function* (_params: null, ctx) {
    const p = yield* ctx.promise("never");
    try {
      yield* ctx.await(p);
      return "resolved";
    } catch (err) {
      if (isCancellation(err)) return "unwound";
      throw err;
    }
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await app.cancel(executionId);
  await worker.tick();

  const snapshot = (await app.getExecution(executionId))!;
  assert.equal(snapshot.status, "cancelled"); // engine-owned outcome
});

test("only external promises can be settled from outside", async () => {
  const { app, pool } = env;
  const task = app.task("internal-only", function* (_params: null, ctx) {
    yield* ctx.run("step", () => 42);
    yield* ctx.sleep("1h");
    return "done";
  });

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  // Grab the internal run checkpoint's row id and try to settle it.
  const { rows } = await pool.query(
    "select id from otra.promises where execution_id = $1 and key = 'step'",
    [executionId],
  );
  assert.equal(await app.resolvePromise(`otr_${rows[0].id}`, "hijack"), false);

  // And a malformed token is rejected loudly, not treated as a miss.
  await assert.rejects(
    app.resolvePromise("not-a-token", 1),
    /invalid promise token/,
  );
});
