import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { ChildFailedError } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

test("parent suspends while awaiting a same-queue child, then resumes", async () => {
  const { app, pool } = env;

  const child = app.task("double", function* (params: { n: number }, ctx) {
    return yield* ctx.run("double", () => params.n * 2);
  });

  const parent = app.task("parent", function* (_params: null, ctx) {
    const handle = yield* ctx.spawn(child, { n: 21 });
    const doubled = yield* ctx.await(handle);
    return { doubled };
  });

  const execution = await app.spawn(parent, null);
  const worker = app.createWorker({ workerId: "w1" });

  // Tick 1 claims only the parent; it spawns the child and parks.  This is
  // the thing absurd cannot do: the parent holds no worker slot, no claim,
  // and same-queue waits cannot deadlock.
  assert.equal(await worker.tick(), 1);
  const parked = (await app.getExecution(execution))!;
  assert.equal(parked.status, "suspended");

  const table = `otra.x_${execution.queueId.replaceAll("-", "")}`;
  const { rows: children } = await pool.query(
    `select id, parent_id, root_id, status from ${table} where function_name = 'double'`,
  );
  assert.equal(children.length, 1);
  assert.equal(children[0].parent_id, execution.executionId);
  assert.equal(children[0].root_id, execution.executionId);

  // Tick 2 runs the child; completion resolves the parent's child-promise
  // and wakes it.  Tick 3 replays the parent to completion.
  assert.equal(await worker.tick(), 1);
  assert.equal(await worker.tick(), 1);
  assert.deepEqual(await app.getResult(execution), { doubled: 42 });
});

test("fan-out: ctx.all awaits several children in order", async () => {
  const { app } = env;

  const square = app.task("square", function* (params: { n: number }, ctx) {
    return yield* ctx.run("sq", () => params.n * params.n);
  });

  const parent = app.task("fan-out", function* (_params: null, ctx) {
    const h1 = yield* ctx.spawn(square, { n: 2 }, { label: "sq-2" });
    const h2 = yield* ctx.spawn(square, { n: 3 }, { label: "sq-3" });
    const h3 = yield* ctx.spawn(square, { n: 4 }, { label: "sq-4" });
    const [a, b, c] = yield* ctx.all([h1, h2, h3]);
    return [a, b, c];
  });

  const execution = await app.spawn(parent, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();

  assert.deepEqual(await app.getResult(execution), [4, 9, 16]);
});

test("replay never duplicates children", async () => {
  const { app, pool } = env;
  let childRuns = 0;

  const child = app.task("once", function* (_params: null, ctx) {
    return yield* ctx.run("count", () => {
      childRuns += 1;
      return childRuns;
    });
  });

  const parent = app.task("replayer", function* (_params: null, ctx) {
    const handle = yield* ctx.spawn(child, null);
    yield* ctx.sleep("30s"); // forces a suspension + full replay of the spawn
    const value = yield* ctx.await(handle);
    yield* ctx.sleep("30s"); // and another one
    return value;
  });

  const execution = await app.spawn(parent, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  await env.advance(31);
  await worker.drain();
  await env.advance(31);
  await worker.drain();

  assert.equal(await app.getResult(execution), 1);
  const { rows } = await pool.query(
    `select count(*)::int as n
       from otra.x_${execution.queueId.replaceAll("-", "")}
      where function_name = 'once'`,
  );
  assert.equal(rows[0].n, 1);
  assert.equal(childRuns, 1);
});

test("a failed child rejects the parent's await with ChildFailedError", async () => {
  const { app } = env;

  app.task("doomed", function* (_params: null, ctx) {
    yield* ctx.run("boom", () => {
      throw new Error("child exploded");
    });
    return "unreachable";
  });

  const catching = app.task("catching-parent", function* (_params: null, ctx) {
    const handle = yield* ctx.spawn("doomed", null, { maxAttempts: 1 });
    try {
      yield* ctx.await(handle);
      return "child-succeeded";
    } catch (err) {
      if (err instanceof ChildFailedError) {
        return `caught: ${err.errorPayload.message}`;
      }
      throw err;
    }
  });

  const uncaught = app.task("uncaught-parent", function* (_params: null, ctx) {
    const handle = yield* ctx.spawn("doomed", null, { maxAttempts: 1 });
    return yield* ctx.await(handle);
  });

  const worker = app.createWorker({ workerId: "w1" });

  const a = await app.spawn(catching, null);
  await worker.drain();
  assert.equal(await app.getResult(a), "caught: child exploded");

  const b = await app.spawn(uncaught, null, { maxAttempts: 5 });
  await worker.drain();
  const snapshot = (await app.getExecution(b))!;
  assert.equal(snapshot.status, "failed");
  // The rejection is memoized, so retrying the parent could never help:
  // it failed on its first attempt despite maxAttempts = 5.
  assert.equal(snapshot.attempt, 1);
  assert.equal(snapshot.error?.name, "ChildFailedError");
});

test("ctx.call is spawn + await", async () => {
  const { app } = env;

  const shout = app.task("shout", function* (params: { word: string }, ctx) {
    return yield* ctx.run("upper", () => params.word.toUpperCase());
  });

  const parent = app.task("caller", function* (_params: null, ctx) {
    return yield* ctx.call(shout, { word: "quiet" });
  });

  const execution = await app.spawn(parent, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  assert.equal(await app.getResult(execution), "QUIET");
});

test("grandchildren: failures are contained to their branch", async () => {
  const { app } = env;

  app.task("leaf-bad", function* (_params: null, ctx) {
    yield* ctx.run("die", () => {
      throw new Error("leaf died");
    });
  });
  const leafGood = app.task("leaf-good", function* (_params: null, ctx) {
    return yield* ctx.run("ok", () => "ok");
  });

  const mid = app.task("mid", function* (_params: null, ctx) {
    const bad = yield* ctx.spawn("leaf-bad", null, { maxAttempts: 1 });
    const good = yield* ctx.spawn(leafGood, null);
    const results: string[] = [];
    try {
      yield* ctx.await(bad);
      results.push("bad-ok");
    } catch {
      results.push("bad-failed");
    }
    results.push((yield* ctx.await(good)) as string);
    return results;
  });

  const root = app.task("root", function* (_params: null, ctx) {
    return yield* ctx.call(mid, null);
  });

  const execution = await app.spawn(root, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  assert.deepEqual(await app.getResult(execution), ["bad-failed", "ok"]);
});

test("ctx.spawn honors the child task's registered retry policy", async () => {
  const { app, pool } = env;
  const child = app.task(
    {
      name: "picky-child",
      maxAttempts: 7,
      retryStrategy: { kind: "fixed", base_s: 9 },
    },
    function* () {
      return "ok";
    },
  );
  app.task("spawning-parent", function* (_params: null, ctx) {
    const handle = yield* ctx.spawn(child, null);
    return yield* ctx.await(handle);
  });
  const execution = await app.spawn("spawning-parent", null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();

  const { rows: q } = await pool.query(
    "select replace(id::text, '-', '') as s from otra.queues where name = 'default'",
  );
  const { rows } = await pool.query(
    `select max_attempts, retry_strategy from otra.x_${q[0].s}
      where root_id = $1 and id <> $2`,
    [execution.rootId, execution.executionId],
  );
  // Previously the child silently got the SQL defaults (5 attempts,
  // exponential) because the driver forwarded only per-spawn options.
  assert.equal(rows[0].max_attempts, 7);
  assert.equal(rows[0].retry_strategy.kind, "fixed");
  assert.equal(rows[0].retry_strategy.base_s, 9);
});
