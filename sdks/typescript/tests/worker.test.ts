import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";
import { afterEach, beforeEach, test } from "node:test";

import pg from "pg";

import { createTestEnv, waitFor, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
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
    assert.equal((await app.getExecution(slowSpawn))!.status, "running");

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
  const executions = `otra.x_default`;

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
  const executions = `otra.x_default`;

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

test("heartbeat FAILURES trip a watchdog: the drive goes lost and ctx.signal aborts", async () => {
  const { app, pool } = env;
  const gate = new EventEmitter();
  let sawAbort = false;

  const task = app.task("db-goes-away", function* (_params: null, ctx) {
    yield* ctx.run("hang", () => {
      gate.emit("entered");
      return new Promise<string>((resolve) => {
        ctx.signal.addEventListener("abort", () => {
          sawAbort = true;
          resolve("aborted");
        });
      });
    });
    return "unreachable";
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1", claimSeconds: 1 });

  const entered = once(gate, "entered");
  const inflight = worker.tick();
  await entered;
  // The database "goes away" for heartbeats only: every extendClaim rejects.
  // Previously .catch(() => {}) swallowed this forever -- the lease silently
  // expired elsewhere while this drive kept running user code, unsignalled.
  const realExtend = app.db.extendClaim.bind(app.db);
  (app.db as { extendClaim: unknown }).extendClaim = () =>
    Promise.reject(new Error("connection terminated"));
  try {
    await inflight; // watchdog: no successful heartbeat for a full lease
  } finally {
    (app.db as { extendClaim: unknown }).extendClaim = realExtend;
  }

  assert.equal(sawAbort, true);
  // The drive abandoned without writing: it could no longer prove ownership.
  const { rows } = await pool.query(
    "select 1 from otra.p_default where root_id = $1 and execution_id = $2 and key = 'hang'",
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows.length, 0);
});

test("unknown-function defers are jittered deterministically per execution", async () => {
  const { app, pool } = env;
  // Two executions of a function this worker does not know (deploy skew).
  const a = await app.spawn("future-fn", null, { queue: "default" });
  const b = await app.spawn("future-fn", null, { queue: "default" });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // defers both

  const { rows } = await pool.query(
    `select id, extract(epoch from run_after - otra.now()) as delay
       from otra.x_default where id in ($1, $2) order by id`,
    [a.executionId, b.executionId],
  );
  const delays = rows.map((r: { delay: string }) => Number(r.delay));
  for (const delay of delays) {
    assert.ok(delay >= 15 && delay < 30, `delay ${delay} outside [15, 30)`);
  }
  // Deterministic per execution: a second defer lands on the same delay...
  await pool.query(
    "update otra.x_default set run_after = otra.now() where id in ($1, $2)",
    [a.executionId, b.executionId],
  );
  await worker.tick();
  const { rows: again } = await pool.query(
    `select id, extract(epoch from run_after - otra.now()) as delay
       from otra.x_default where id in ($1, $2) order by id`,
    [a.executionId, b.executionId],
  );
  assert.deepEqual(
    again.map((r: { delay: string }) => Number(r.delay)),
    delays,
  );
  // ...and (with overwhelming likelihood) two executions spread apart.
  assert.notEqual(delays[0], delays[1]);
});

test("app.close() stops started workers before ending the pool", async () => {
  const { app } = env;
  const gate = new EventEmitter();
  let releaseStep: (() => void) | null = null;

  const task = app.task("closing-time", function* (_params: null, ctx) {
    return yield* ctx.run("slow", async () => {
      gate.emit("entered");
      await new Promise<void>((resolve) => {
        releaseStep = resolve;
      });
      return "done";
    });
  });

  const execution = await app.spawn(task, null);
  app.startWorker({ workerId: "w1", pollIntervalMs: 10 });

  await once(gate, "entered");
  const closing = app.close(); // must stop the worker and DRAIN, not yank the pool
  await new Promise((resolve) => setTimeout(resolve, 20));
  releaseStep!();
  await closing;

  // The in-flight execution finished cleanly instead of dying mid-drive.
  const verify = new pg.Pool({
    connectionString: env.connectionString,
    max: 1,
  });
  try {
    const { rows } = await verify.query(
      "select status from otra.x_default where id = $1",
      [execution.executionId],
    );
    assert.equal(rows[0].status, "completed");
  } finally {
    await verify.end();
  }
});
