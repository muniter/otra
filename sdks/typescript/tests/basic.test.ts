import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { TimeoutError } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

test("completes a simple task and returns its result", async () => {
  const { app } = env;
  const hello = app.task("hello", function* (params: { name: string }, ctx) {
    const greeting = yield* ctx.run("greet", () => `Hello, ${params.name}!`);
    return { greeting };
  });

  const execution = await app.spawn(hello, { name: "Lily" });
  const worker = app.createWorker({ workerId: "w1" });
  assert.equal(await worker.tick(), 1);

  const result = await app.getResult<{ greeting: string }>(execution);
  assert.deepEqual(result, { greeting: "Hello, Lily!" });
});

test("steps are memoized across suspension and never re-execute", async () => {
  const { app } = env;
  const calls = { a: 0, b: 0, outside: 0 };

  const task = app.task("sleeper", function* (_params: null, ctx) {
    calls.outside += 1;
    const a = yield* ctx.run("step-a", () => {
      calls.a += 1;
      return "a-result";
    });
    yield* ctx.sleep("1m");
    const b = yield* ctx.run("step-b", () => {
      calls.b += 1;
      return `${a}+b`;
    });
    return b;
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });

  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");
  assert.deepEqual(calls, { a: 1, b: 0, outside: 1 });

  // Not due yet: nothing to claim.
  assert.equal(await worker.tick(), 0);

  await env.advance(61);
  assert.equal(await worker.tick(), 1);
  assert.equal(await app.getResult(execution), "a-result+b");
  // The generator replayed from the top (outside code ran twice) but the
  // checkpointed step did not re-execute.
  assert.deepEqual(calls, { a: 1, b: 1, outside: 2 });
});

test("deterministic helpers are stable across replay", async () => {
  const { app } = env;
  const seen: { first: string[]; second: string[] } = { first: [], second: [] };

  const task = app.task("rng", function* (_params: null, ctx) {
    const id = yield* ctx.uuid();
    const rand = yield* ctx.random();
    const list = ctx.attempt === 0 ? seen.first : seen.second;
    list.push(id, String(rand));
    yield* ctx.sleep("10s");
    return { id, rand };
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await env.advance(11);
  await worker.tick();

  const result = await app.getResult<{ id: string; rand: number }>(execution);
  // Replay after the sleep injected the same memoized values.
  assert.deepEqual(seen.first, [
    result.id,
    String(result.rand),
    result.id,
    String(result.rand),
  ]);
});

test("replay divergence is detected and fails permanently", async () => {
  const { app } = env;
  let shape: "run" | "event" = "run";

  const task = app.task("shape-shifter", function* (_params: null, ctx) {
    if (shape === "run") {
      yield* ctx.run("point", () => 1);
    } else {
      yield* ctx.waitForEvent("never", { label: "point" });
    }
    yield* ctx.sleep("10s");
    return "done";
  });

  const execution = await app.spawn(task, null, { maxAttempts: 5 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");

  // Simulate a bad code change while the execution slept: the promise at
  // key "point" is recorded as a run, but the code now produces an event
  // wait there.
  shape = "event";
  await env.advance(11);
  await worker.tick();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "failed");
  assert.equal(snapshot.error?.name, "DeterminismViolationError");
  // Non-retryable: it failed on the first divergent attempt.
  assert.equal(snapshot.attempt, 1);
});

test("ctx.now() reads the database clock, so the fake clock governs it", async () => {
  const { app } = env;
  const task = app.task("what-time", function* (_params: null, ctx) {
    return yield* ctx.now();
  });
  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  const result = await app.getResult<number>(execution);
  // helpers.ts freezes otra.now() at 2026-01-01T00:00:00Z.
  assert.equal(result, Date.parse("2026-01-01T00:00:00Z"));
});

test("getResult validates its options and times out with a typed error", async () => {
  const { app } = env;
  app.task("never-done", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "later";
  });
  const execution = await app.spawn("never-done", null, { queue: "default" });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // parks

  await assert.rejects(
    app.getResult(execution, { timeoutMs: 50 }),
    TimeoutError,
  );
  await assert.rejects(
    app.getResult(execution, { timeoutMs: Number.NaN }),
    TypeError,
  );
  await assert.rejects(app.getResult(execution, { timeoutMs: -1 }), TypeError);
  await assert.rejects(app.getResult(execution, { pollMs: 0 }), TypeError);
});

test("spawning an unregistered task name requires an explicit queue", async () => {
  const { app } = env;
  // A typo'd name used to spawn silently with SQL defaults and then cycle
  // in the unknown-function defer loop forever (absurd's a339dee lesson).
  await assert.rejects(app.spawn("tpyo-task", null), /not registered.*queue/s);
  // Explicit queue = a deliberate cross-process spawn; allowed.
  const ok = await app.spawn("tpyo-task", null, { queue: "default" });
  assert.ok(ok.executionId);
});

test("a freshly applied schema reports its development version", async () => {
  const { app } = env;
  // The schema file carries its own version marker (absurd's
  // get_schema_version): 'main' while unreleased, stamped with the tag by
  // release automation. There are no migrations yet -- see the comment above
  // otra.schema_version() in sql/schema.sql -- so this is the only version
  // handle an operator has.
  assert.equal(await app.schemaVersion(), "main");
});

test("an idempotent spawn reports whether it created the execution", async () => {
  const { app } = env;
  const task = app.task("charge", function* (params: { slot: string }) {
    return params.slot;
  });

  const first = await app.spawn(
    task,
    { slot: "2026-08-23" },
    {
      idempotencyKey: "cron:charge:2026-08-23",
    },
  );
  const second = await app.spawn(
    task,
    { slot: "2026-08-23" },
    {
      idempotencyKey: "cron:charge:2026-08-23",
    },
  );

  // Same address both times -- that part already worked. What a cron or
  // webhook caller needs on top is to tell a fresh spawn from a dedupe, so
  // it can log "already scheduled" instead of "scheduled".
  assert.equal(second.executionId, first.executionId);
  assert.equal(first.created, true);
  assert.equal(second.created, false);
});
