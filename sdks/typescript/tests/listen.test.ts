import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { createTestEnv, waitFor, type TestEnv } from "./helpers.ts";

// LISTEN/NOTIFY wakeups. The schema has emitted pg_notify('otra_wake',
// <queue name>) from every wake path since day one; these tests prove the
// SDK actually listens. The technique throughout: start a worker with a
// HUGE pollIntervalMs so the polling fallback cannot explain fast progress
// -- only a delivered notification (or a next_due-bounded timer sleep) can.
const SLOW_POLL = 120_000;

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

test("a spawn wakes an idle listening worker without polling", async () => {
  const { app } = env;
  app.task("prompt-hello", function* () {
    return "hola";
  });
  const worker = app.startWorker({
    workerId: "w1",
    pollIntervalMs: SLOW_POLL,
  });
  try {
    // Let the worker go idle first (claim finds nothing, parks on LISTEN).
    await new Promise((resolve) => setTimeout(resolve, 300));
    const started = Date.now();
    const execution = await app.spawn("prompt-hello", null);
    const result = await app.getResult<string>(execution, {
      timeoutMs: 10_000,
    });
    assert.equal(result, "hola");
    // Poll fallback is 2 minutes; only a notification explains this.
    assert.ok(
      Date.now() - started < 5_000,
      `took ${Date.now() - started}ms; the notification did not arrive`,
    );
  } finally {
    await worker.stop();
  }
});

test("an event emit wakes a suspended execution through LISTEN", async () => {
  const { app } = env;
  app.task("event-waiter", function* (_params: null, ctx) {
    const payload = yield* ctx.waitForEvent<{ n: number }>("go");
    return payload.n;
  });
  const worker = app.startWorker({
    workerId: "w1",
    pollIntervalMs: SLOW_POLL,
  });
  try {
    const execution = await app.spawn("event-waiter", null);
    await waitFor(
      async () => (await app.getExecution(execution))!.status === "suspended",
      { label: "waiter parked" },
    );
    const started = Date.now();
    await app.emitEvent("go", { n: 7 });
    const result = await app.getResult<number>(execution, {
      timeoutMs: 10_000,
    });
    assert.equal(result, 7);
    assert.ok(
      Date.now() - started < 5_000,
      `took ${Date.now() - started}ms; the wake notification did not arrive`,
    );
  } finally {
    await worker.stop();
  }
});

test("next_due bounds the idle sleep so timers fire without notifications", async () => {
  const { app, pool } = env;
  // next_due_local reports the earliest future instant the queue needs a
  // claim for: pending run_after, timer wake_at, lease expiry, deadlines.
  app.task("napper", function* (_params: null, ctx) {
    yield* ctx.sleep("30m");
    return "rested";
  });
  const execution = await app.spawn("napper", null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // parks on the sleep timer

  const { rows } = await pool.query(
    "select extract(epoch from (otra.next_due_local('default') - otra.now()))::float8 as due_in",
  );
  assert.ok(
    Math.abs(rows[0].due_in - 1800) < 1,
    `expected ~1800s until due, got ${rows[0].due_in}`,
  );

  // A retry scheduled sooner takes over as the minimum.
  app.task("failer", function* (_params: null, ctx) {
    yield* ctx.run("boom", () => {
      throw new Error("nope");
    });
  });
  await app.spawn("failer", null, {
    retryStrategy: { kind: "fixed", base_s: 60 },
  });
  await worker.tick(); // fails; retry lands 60-75s out (jitter)
  const { rows: after } = await pool.query(
    "select extract(epoch from (otra.next_due_local('default') - otra.now()))::float8 as due_in",
  );
  assert.ok(
    after[0].due_in >= 59 && after[0].due_in <= 76,
    `expected the 60s retry to be the minimum, got ${after[0].due_in}`,
  );

  // An immediately-runnable execution reports a non-positive due time.
  const ref = await app.spawn("napper", null);
  const { rows: now } = await pool.query(
    "select extract(epoch from (otra.next_due_local('default') - otra.now()))::float8 as due_in",
  );
  assert.ok(now[0].due_in <= 0, `expected due now, got ${now[0].due_in}`);
  void ref;
  void execution;

  // An empty queue reports nothing due.
  await app.createQueue("idle-q");
  const { rows: idle } = await pool.query(
    "select otra.next_due_local('idle-q') as due",
  );
  assert.equal(idle[0].due, null);
});

test("terminal transitions notify, so getResult returns without polling", async () => {
  const { app, pool } = env;
  const gate = { release: null as (() => void) | null };
  app.task("slow-finish", function* (_params: null, ctx) {
    return yield* ctx.run("wait-for-test", async () => {
      await new Promise<void>((resolve) => {
        gate.release = resolve;
      });
      return "finished";
    });
  });
  const worker = app.startWorker({
    workerId: "w1",
    pollIntervalMs: SLOW_POLL,
  });
  try {
    const execution = await app.spawn("slow-finish", null);
    await waitFor(async () => gate.release !== null, {
      label: "step entered",
    });
    // getResult's backoff would otherwise sleep up to 1s between polls; a
    // completion NOTIFY must cut that short. Measure from release to result.
    const pending = app.getResult<string>(execution, {
      timeoutMs: 10_000,
      pollMs: 9_000, // one initial poll, then only a notification can help
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    const started = Date.now();
    gate.release!();
    assert.equal(await pending, "finished");
    assert.ok(
      Date.now() - started < 5_000,
      `took ${Date.now() - started}ms; the terminal notification did not arrive`,
    );
  } finally {
    await worker.stop();
  }
  void pool;
});

test("the listener survives having its connection killed", async () => {
  const { app, pool } = env;
  app.task("resilient", function* () {
    return "back";
  });
  const worker = app.startWorker({
    workerId: "w1",
    pollIntervalMs: SLOW_POLL,
  });
  try {
    // Wait for the LISTEN connection to exist, then kill it server-side.
    await waitFor(
      async () => {
        const { rows } = await pool.query(
          "select count(*)::int as n from pg_stat_activity where application_name = 'otra-listen'",
        );
        return rows[0].n > 0;
      },
      { label: "listener connected" },
    );
    await pool.query(
      "select pg_terminate_backend(pid) from pg_stat_activity where application_name = 'otra-listen'",
    );

    // The hub must reconnect (and poll once on reset, in case anything was
    // missed while down); work spawned afterwards still completes fast.
    const execution = await app.spawn("resilient", null);
    const result = await app.getResult<string>(execution, {
      timeoutMs: 15_000,
    });
    assert.equal(result, "back");
  } finally {
    await worker.stop();
  }
});

test("a worker whose wake source is unhealthy falls back to a fast poll", async () => {
  const { app } = env;
  // A wake source that never connects (exhausted pool, or a proxy like
  // pgbouncer in transaction mode where LISTEN silently cannot work). The
  // worker must NOT keep its long listening fallback in that state, or
  // every event-driven wake silently degrades to worst-case a minute.
  const deadSource = {
    subscribe: () => () => {},
    isConnected: () => false,
  };
  app.task("degraded-hello", function* () {
    return "still-works";
  });
  const worker = app.createWorker({ workerId: "w1", wake: deadSource });
  worker.start();
  try {
    await new Promise((resolve) => setTimeout(resolve, 300)); // go idle
    const started = Date.now();
    const execution = await app.spawn("degraded-hello", null);
    // getResult itself may listen; the assertion is on the WORKER picking
    // the work up, which only a short degraded poll can explain.
    await waitFor(
      async () => (await app.getExecution(execution))!.status === "completed",
      { timeoutMs: 5_000, label: "degraded worker completed the spawn" },
    );
    assert.ok(Date.now() - started < 5_000);
  } finally {
    await worker.stop();
  }
});

test("notifications are an optimization: liveness holds with them disabled", async () => {
  const { app, pool } = env;
  await pool.query("select otra.set_wake_notifications(false)");
  try {
    // Prove nothing is emitted: a raw listener hears silence across a spawn.
    const client = await pool.connect();
    let heard = 0;
    try {
      await client.query("listen otra_wake");
      client.on("notification", () => {
        heard += 1;
      });
      app.task("silent-running", function* () {
        return "done";
      });
      const execution = await app.spawn("silent-running", null);
      // A short-poll worker still completes the work: the engine is a plain
      // polling system when notifications are off, losing only latency.
      const worker = app.startWorker({
        workerId: "w1",
        pollIntervalMs: 100,
      });
      try {
        const result = await app.getResult<string>(execution, {
          timeoutMs: 10_000,
          pollMs: 100,
        });
        assert.equal(result, "done");
      } finally {
        await worker.stop();
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
      assert.equal(heard, 0, `expected silence, heard ${heard} notifications`);
    } finally {
      client.release();
    }
  } finally {
    await pool.query("select otra.set_wake_notifications(true)");
  }
});
