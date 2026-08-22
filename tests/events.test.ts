import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { EventTimeoutError } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

test("await-then-emit: task suspends until the event arrives", async () => {
  const { app } = env;

  const task = app.task("waiter", function* (params: { orderId: string }, ctx) {
    const packed = yield* ctx.waitForEvent<{ tracking: string }>(
      `packed:${params.orderId}`,
    );
    return `shipped via ${packed.tracking}`;
  });

  const { executionId } = await app.spawn(task, { orderId: "42" });
  const worker = app.createWorker({ workerId: "w1" });

  await worker.tick();
  assert.equal((await app.getExecution(executionId))!.status, "suspended");
  assert.equal(await worker.tick(), 0);

  await app.emitEvent("packed:42", { tracking: "TRACK-9" });
  assert.equal(await worker.tick(), 1);
  assert.equal(await app.getResult(executionId), "shipped via TRACK-9");
});

test("emit-then-await: cached events resolve without suspending", async () => {
  const { app } = env;

  const task = app.task("late-waiter", function* (_params: null, ctx) {
    const payload = yield* ctx.waitForEvent<{ n: number }>("already-there");
    return payload.n;
  });

  await app.emitEvent("already-there", { n: 7 });
  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  assert.equal(await worker.tick(), 1);
  assert.equal(await app.getResult(executionId), 7);
});

test("event timeout is thrown into the task and is catchable", async () => {
  const { app } = env;

  const catching = app.task("patient", function* (_params: null, ctx) {
    try {
      yield* ctx.waitForEvent("godot", { timeout: "1h" });
      return "arrived";
    } catch (err) {
      if (err instanceof EventTimeoutError) return "gave-up";
      throw err;
    }
  });

  const { executionId } = await app.spawn(catching, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(executionId))!.status, "suspended");

  await env.advance(3601);
  await worker.tick();
  assert.equal(await app.getResult(executionId), "gave-up");
});

test("uncaught event timeout fails the execution without retries", async () => {
  const { app } = env;

  const task = app.task("impatient", function* (_params: null, ctx) {
    yield* ctx.waitForEvent("godot", { timeout: "1h" });
    return "arrived";
  });

  const { executionId } = await app.spawn(task, null, { maxAttempts: 5 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await env.advance(3601);
  await worker.tick();

  const snapshot = (await app.getExecution(executionId))!;
  assert.equal(snapshot.status, "failed");
  assert.equal(snapshot.attempt, 1); // memoized rejection: retrying is futile
  assert.equal(snapshot.error?.name, "EventTimeoutError");
});

test("an event name is an immutable fact: repeat waits agree, repeat emits are no-ops", async () => {
  const { app, pool } = env;

  // absurd's semantics, adopted deliberately (their commit 7b63b7a moved TO
  // first-write-wins after shipping mutable events): an event name is a
  // one-shot fact per queue. Asking twice gives the same answer twice; a
  // recurring signal derives names (`tick:${i}`) or uses ctx.promise.
  const task = app.task("double-checker", function* (_params: null, ctx) {
    const first = yield* ctx.waitForEvent<{ n: number }>("launched");
    const second = yield* ctx.waitForEvent<{ n: number }>("launched");
    return [first.n, second.n];
  });

  await app.emitEvent("launched", { n: 1 });
  await app.emitEvent("launched", { n: 99 }); // no-op: first write won

  const { executionId } = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  // Both waits resolve immediately with the same immutable fact.
  assert.deepEqual(await app.getResult(executionId), [1, 1]);

  // Exactly one event row exists for the name.
  const { rows } = await pool.query(
    "select count(*)::int as n, min(payload ->> 'n') as v from otra.events where name = 'launched'",
  );
  assert.equal(rows[0].n, 1);
  assert.equal(rows[0].v, "1");
});

test("a no-op re-emit does not disturb waiters already resolved by the fact", async () => {
  const { app } = env;
  const task = app.task("late-arrival", function* (_params: null, ctx) {
    const payload = yield* ctx.waitForEvent<{ v: string }>("sealed");
    return payload.v;
  });

  await app.emitEvent("sealed", { v: "first" });
  const early = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal(await app.getResult(early.executionId), "first");

  await app.emitEvent("sealed", { v: "second" }); // no-op
  const late = await app.spawn(task, null);
  await worker.tick();
  assert.equal(await app.getResult(late.executionId), "first");
});

test("one event wakes every waiter on the queue", async () => {
  const { app } = env;

  const task = app.task(
    "broadcast-waiter",
    function* (params: { id: number }, ctx) {
      const payload = yield* ctx.waitForEvent<{ v: string }>("broadcast");
      return `${params.id}:${payload.v}`;
    },
  );

  const a = await app.spawn(task, { id: 1 });
  const b = await app.spawn(task, { id: 2 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();

  await app.emitEvent("broadcast", { v: "x" });
  await worker.drain();

  assert.equal(await app.getResult(a.executionId), "1:x");
  assert.equal(await app.getResult(b.executionId), "2:x");
});
