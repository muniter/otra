import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
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
