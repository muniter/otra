import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";
import { afterEach, beforeEach, test } from "node:test";

import { createTestEnv, waitFor, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

// Test coordination follows absurd's TS SDK tests: handlers report through an
// EventEmitter gate and block on once(gate, "release"), so the tests assert
// on explicit rendezvous points instead of sleeping and hoping.

test("a slow step does not block other executions (no head-of-line blocking)", async () => {
  const { app } = env;
  const gate = new EventEmitter();

  const slow = app.task("slow", function* (_params: null, ctx) {
    yield* ctx.run("wait-for-gate", async () => {
      gate.emit("arrived");
      await once(gate, "release");
    });
    return "slow-done";
  });
  const fast = app.task("fast", function* (_params: null, ctx) {
    return yield* ctx.run("quick", () => "fast-done");
  });

  const slowSpawn = await app.spawn(slow, null);
  const worker = app.createWorker({
    workerId: "w1",
    concurrency: 2,
    batchSize: 1,
    pollIntervalMs: 10,
  });
  const arrived = once(gate, "arrived");
  worker.start();
  try {
    // The worker is now stuck inside the slow step...
    await arrived;
    // ...but a free slot must still pick up and finish new work.
    const fastSpawn = await app.spawn(fast, null);
    const fastResult = await app.getResult(fastSpawn, {
      timeoutMs: 3_000,
    });
    assert.equal(fastResult, "fast-done");
    assert.equal(
      (await app.getExecution(slowSpawn))!.status,
      "running",
    );

    gate.emit("release");
    assert.equal(
      await app.getResult(slowSpawn, { timeoutMs: 3_000 }),
      "slow-done",
    );
  } finally {
    gate.emit("release");
    await worker.stop();
  }
});

test("the concurrency cap holds while slots are saturated", async () => {
  const { app, pool } = env;
  const gate = new EventEmitter();
  const atGate = new Set<number>();
  gate.on("arrived", (id: number) => atGate.add(id));
  gate.on("left", (id: number) => atGate.delete(id));

  const task = app.task("gated", function* (params: { id: number }, ctx) {
    yield* ctx.run("hold", async () => {
      gate.emit("arrived", params.id);
      await once(gate, "release");
      gate.emit("left", params.id);
    });
    return params.id;
  });

  const spawns = [];
  for (let i = 1; i <= 5; i++) spawns.push(await app.spawn(task, { id: i }));
  const executions = `otra.x_${spawns[0]!.queueId.replaceAll("-", "")}`;

  const worker = app.createWorker({
    workerId: "w1",
    concurrency: 2,
    pollIntervalMs: 10,
  });
  worker.start();
  try {
    await waitFor(() => atGate.size === 2, { label: "two tasks at gate" });

    // Several poll cycles later, still exactly two: no over-claiming.
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.equal(atGate.size, 2);
    const { rows } = await pool.query(
      `select count(*)::int as n from ${executions} where status = 'running'`,
    );
    assert.equal(rows[0].n, 2);

    // Release repeatedly (each release frees the waiters at the gate) until
    // the whole batch has drained through the two slots.
    await waitFor(
      async () => {
        gate.emit("release");
        const { rows: done } = await pool.query(
          `select count(*)::int as n from ${executions} where status = 'completed'`,
        );
        return done[0].n === 5;
      },
      { label: "all five completed", timeoutMs: 5_000, intervalMs: 20 },
    );
  } finally {
    gate.emit("release");
    await worker.stop();
  }
});

test("a freed slot claims new work immediately, not at the next poll", async () => {
  const { app, pool } = env;
  const gate = new EventEmitter();
  const atGate = new Set<number>();
  gate.on("arrived", (id: number) => atGate.add(id));
  gate.on("left", (id: number) => atGate.delete(id));

  const task = app.task("responsive", function* (params: { id: number }, ctx) {
    yield* ctx.run("hold", async () => {
      gate.emit("arrived", params.id);
      await once(gate, "release");
      gate.emit("left", params.id);
    });
    return params.id;
  });

  const spawns = [];
  for (let i = 1; i <= 3; i++) spawns.push(await app.spawn(task, { id: i }));
  const executions = `otra.x_${spawns[0]!.queueId.replaceAll("-", "")}`;

  // Poll interval of an hour: completion must come from the wake-on-free
  // path, not from polling.
  const worker = app.createWorker({
    workerId: "w1",
    concurrency: 2,
    pollIntervalMs: 3_600_000,
  });
  worker.start();
  try {
    await waitFor(() => atGate.size === 2, { label: "two tasks at gate" });

    await waitFor(
      async () => {
        gate.emit("release");
        const { rows } = await pool.query(
          `select count(*)::int as n from ${executions} where status = 'completed'`,
        );
        return rows[0].n === 3;
      },
      {
        label: "all three completed without polling",
        timeoutMs: 5_000,
        intervalMs: 20,
      },
    );
  } finally {
    gate.emit("release");
    await worker.stop();
  }
});

test("stop() drains in-flight executions gracefully", async () => {
  const { app } = env;
  const gate = new EventEmitter();
  const atGate = new Set<number>();
  gate.on("arrived", (id: number) => atGate.add(id));

  const task = app.task("drainee", function* (params: { id: number }, ctx) {
    yield* ctx.run("hold", async () => {
      gate.emit("arrived", params.id);
      await once(gate, "release");
    });
    return "ok";
  });

  const spawns = await Promise.all([
    app.spawn(task, { id: 1 }),
    app.spawn(task, { id: 2 }),
  ]);
  const worker = app.createWorker({
    workerId: "w1",
    concurrency: 2,
    pollIntervalMs: 10,
  });
  worker.start();
  await waitFor(() => atGate.size === 2, { label: "both tasks in flight" });

  // Stop while both are mid-step: it must not resolve until they finish.
  let stopped = false;
  const stopping = worker.stop().then(() => {
    stopped = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.equal(stopped, false);

  gate.emit("release");
  await stopping;

  for (const execution of spawns) {
    assert.equal((await app.getExecution(execution))!.status, "completed");
  }
});
