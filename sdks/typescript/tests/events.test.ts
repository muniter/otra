import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { EventTimeoutError } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

test("await-then-emit: task suspends until the event arrives", async () => {
  const { app } = env;

  const task = app.task("waiter", function* (params: { orderId: string }, ctx) {
    const packed = yield* ctx.waitForEvent<{ tracking: string }>(
      `packed:${params.orderId}`,
    );
    return `shipped via ${packed.tracking}`;
  });

  const execution = await app.spawn(task, { orderId: "42" });
  const worker = app.createWorker({ workerId: "w1" });

  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");
  assert.equal(await worker.tick(), 0);

  await app.emitEvent("packed:42", { tracking: "TRACK-9" });
  assert.equal(await worker.tick(), 1);
  assert.equal(await app.getResult(execution), "shipped via TRACK-9");
});

test("emit-then-await: cached events resolve without suspending", async () => {
  const { app } = env;

  const task = app.task("late-waiter", function* (_params: null, ctx) {
    const payload = yield* ctx.waitForEvent<{ n: number }>("already-there");
    return payload.n;
  });

  await app.emitEvent("already-there", { n: 7 });
  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  assert.equal(await worker.tick(), 1);
  assert.equal(await app.getResult(execution), 7);
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

  const execution = await app.spawn(catching, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");

  await env.advance(3601);
  await worker.tick();
  assert.equal(await app.getResult(execution), "gave-up");
});

test("uncaught event timeout fails the execution without retries", async () => {
  const { app } = env;

  const task = app.task("impatient", function* (_params: null, ctx) {
    yield* ctx.waitForEvent("godot", { timeout: "1h" });
    return "arrived";
  });

  const execution = await app.spawn(task, null, { maxAttempts: 5 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await env.advance(3601);
  await worker.tick();

  const snapshot = (await app.getExecution(execution))!;
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

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  // Both waits resolve immediately with the same immutable fact.
  assert.deepEqual(await app.getResult(execution), [1, 1]);

  // Exactly one event row exists for the name.
  const { rows } = await pool.query(
    `select count(*)::int as n, min(payload ->> 'n') as v
       from otra.e_default
      where name = 'launched'`,
  );
  assert.equal(rows[0].n, 1);
  assert.equal(rows[0].v, "1");
});

test("emitEvent reports whether this call created the fact", async () => {
  const { app } = env;

  // The return value is the only way a caller can tell "I wrote this fact"
  // from "someone beat me to it" -- a repeat emit with a DIFFERENT payload
  // changes nothing at all, silently.
  assert.equal(await app.emitEvent("shipment:1", { carrier: "first" }), true);
  assert.equal(await app.emitEvent("shipment:1", { carrier: "second" }), false);

  const task = app.task("carrier-reader", function* (_params: null, ctx) {
    const fact = yield* ctx.waitForEvent<{ carrier: string }>("shipment:1");
    return fact.carrier;
  });
  const execution = await app.spawn(task, null);
  await app.createWorker({ workerId: "w1" }).tick();

  // The waiter sees the original payload, not the discarded second one.
  assert.equal(await app.getResult(execution), "first");
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
  assert.equal(await app.getResult(early), "first");

  await app.emitEvent("sealed", { v: "second" }); // no-op
  const late = await app.spawn(task, null);
  await worker.tick();
  assert.equal(await app.getResult(late), "first");
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

  assert.equal(await app.getResult(a), "1:x");
  assert.equal(await app.getResult(b), "2:x");
});
