import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";
import { afterEach, beforeEach, test } from "node:test";

import pg from "pg";

import { isCancellation, type Ctx } from "../src/index.ts";
import { createTestEnv, waitFor, type TestEnv } from "./helpers.ts";

// Cancellation v1, per docs/cancellation-design.md: cancel is a request
// against a live execution (cancel_requested_at column, status stays
// 'running'), delivered as a CancelledError thrown into the generator at the
// first effect needing new work. Compensation in catch/finally may run local
// ctx.run steps, which checkpoint normally. The engine owns the terminal
// state. kill() is the escape hatch: immediate, no compensation (OT002).

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

test("cancelling a pending execution with no history finalizes immediately", async () => {
  const { app } = env;
  app.task("never-runs", function* (_params: null, ctx) {
    return yield* ctx.run("nope", () => "unreachable");
  });
  const execution = await app.spawn("never-runs", null);

  await app.cancel(execution, { reason: "changed my mind" });

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.equal(snapshot.cancelReason, "changed my mind");
  // No worker ever claimed it.
  assert.equal(snapshot.attempt, 0);
});

test("cancelling a suspended execution wakes it and delivers at the blocked yield", async () => {
  const { app, pool } = env;
  const calls = { forward: 0, compensate: 0 };

  const task = app.task("sleeper", function* (_params: null, ctx) {
    try {
      yield* ctx.run("forward", () => {
        calls.forward += 1;
        return "did-forward-work";
      });
      yield* ctx.sleep("1h");
      return "done";
    } catch (err) {
      if (isCancellation(err)) {
        // Plain catch, plain step: it must execute AND checkpoint.
        yield* ctx.run("compensate", () => {
          calls.compensate += 1;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");

  const actions = await app.cancel(execution);
  assert.deepEqual(actions, [
    { executionId: execution.executionId, action: "woken" },
  ]);
  assert.equal((await app.getExecution(execution))!.status, "pending");

  await worker.tick();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  // Forward work was memoized on the replay, not re-executed; compensation
  // ran exactly once and was recorded as a promise.
  assert.deepEqual(calls, { forward: 1, compensate: 1 });
  const { rows } = await pool.query(
    `select status from otra.p_${execution.queueId.replaceAll("-", "")}
      where root_id = $1 and execution_id = $2 and key = 'compensate'`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].status, "resolved");
});

test("catching CancelledError and returning normally still ends 'cancelled'", async () => {
  const { app } = env;
  const task = app.task("swallower", function* (_params: null, ctx) {
    try {
      yield* ctx.sleep("1h");
      return "slept";
    } catch {
      return "swallowed-it"; // the Temporal footgun: this must NOT complete
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await app.cancel(execution);
  await worker.tick();

  assert.equal((await app.getExecution(execution))!.status, "cancelled");
});

test("an error thrown during compensation is recorded but never retried", async () => {
  const { app } = env;
  const task = app.task("bad-cleanup", function* (_params: null, ctx) {
    try {
      yield* ctx.sleep("1h");
      return "slept";
    } catch (err) {
      if (isCancellation(err)) throw new Error("cleanup exploded");
      throw err;
    }
  });

  const execution = await app.spawn(task, null, { maxAttempts: 5 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await app.cancel(execution);
  await worker.tick();
  await env.advance(600);
  await worker.drain();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled"); // never 'failed'
  assert.equal(snapshot.error?.message, "cleanup exploded");
  assert.equal(snapshot.attempt, 0); // no retries were consumed
});

// (v1 refused remote effects during compensation; v2 allows them -- the
// suspending-compensation behavior is covered in tests/compensation.test.ts.)

test("a running execution discovers cancellation through the heartbeat", async () => {
  const { app, pool } = env;
  const gate = new EventEmitter();
  const calls = { after: 0, compensate: 0 };
  let seenCtx: Ctx | undefined;

  const task = app.task("long-runner", function* (_params: null, ctx) {
    seenCtx = ctx;
    try {
      yield* ctx.run("hold", async () => {
        gate.emit("arrived");
        await once(gate, "release");
        return "held";
      });
      yield* ctx.run("after", () => {
        calls.after += 1;
      });
      return "finished";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => {
          calls.compensate += 1;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  // claimSeconds 1 => heartbeat every ~500ms carries the flag back.
  const worker = app.createWorker({
    workerId: "w1",
    claimSeconds: 1,
    pollIntervalMs: 10,
  });
  const arrived = once(gate, "arrived");
  worker.start();
  try {
    await arrived;
    await app.cancel(execution, { reason: "operator" });

    // The worker notices without finishing the step: flag + AbortSignal.
    await waitFor(() => seenCtx?.cancelRequested === true, {
      label: "ctx.cancelRequested via heartbeat",
    });
    assert.equal(seenCtx!.signal.aborted, true);

    gate.emit("release");
    await waitFor(
      async () => (await app.getExecution(execution))!.status === "cancelled",
      { label: "execution finalized" },
    );

    // The in-flight step finished and was recorded (nobody preempts a local
    // step by default); the next forward step never ran; compensation did.
    const { rows } = await pool.query(
      `select key, status from otra.p_${execution.queueId.replaceAll("-", "")}
        where root_id = $1 and execution_id = $2 order by key`,
      [execution.rootId, execution.executionId],
    );
    const keys = rows.map((r: { key: string }) => r.key).sort();
    // $cancel is the journaled delivery point (cancellation v2).
    assert.deepEqual(keys, ["$cancel", "compensate", "hold"]);
    assert.deepEqual(calls, { after: 0, compensate: 1 });
  } finally {
    gate.emit("release");
    await worker.stop();
  }
});

test("kill stops a running execution without compensation (OT002, not OT001)", async () => {
  const { app, pool } = env;
  const gate = new EventEmitter();
  let finallyRan = false;

  const task = app.task("killable", function* (_params: null, ctx) {
    try {
      yield* ctx.run("hold", async () => {
        gate.emit("arrived");
        await once(gate, "release");
        return "held";
      });
      return "finished";
    } finally {
      finallyRan = true;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({
    workerId: "w1",
    claimSeconds: 30,
    pollIntervalMs: 10,
  });
  const arrived = once(gate, "arrived");
  worker.start();
  try {
    await arrived;
    await app.kill(execution, { reason: "stuck" });
    assert.equal((await app.getExecution(execution))!.status, "cancelled");

    // Let the in-flight step finish; its checkpoint write gets OT002 and the
    // worker abandons quietly -- no compensation, no history writes.
    gate.emit("release");
    await waitFor(
      async () => {
        const { rows } = await pool.query(
          `select count(*)::int as n
             from otra.x_${execution.queueId.replaceAll("-", "")}
            where status = 'running'`,
        );
        return rows[0].n === 0;
      },
      { label: "worker abandoned the killed execution", timeoutMs: 4_000 },
    );

    const { rows: promises } = await pool.query(
      `select count(*)::int as n
         from otra.p_${execution.queueId.replaceAll("-", "")}
        where root_id = $1 and execution_id = $2`,
      [execution.rootId, execution.executionId],
    );
    assert.equal(promises[0].n, 0);
    assert.equal(finallyRan, false);
  } finally {
    gate.emit("release");
    await worker.stop();
  }
});

test("cancel cascades to children by default; detached children survive", async () => {
  const { app } = env;

  const lingering = app.task("lingering", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "lingered";
  });
  const audit = app.task("audit", function* (_params: null, ctx) {
    yield* ctx.sleep("5s");
    return "audited";
  });
  const parent = app.task("tree-root", function* (_params: null, ctx) {
    const child = yield* ctx.spawn(lingering, null);
    yield* ctx.spawn(audit, null, { onParentCancel: "detach", label: "audit" });
    return yield* ctx.await(child);
  });

  const execution = await app.spawn(parent, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain(); // parent suspended on child; both children suspended

  const actions = await app.cancel(execution);
  const byAction = new Map(actions.map((a) => [a.executionId, a.action]));
  assert.equal(byAction.size, 2); // parent + cascade child, NOT the detached one

  await worker.drain();

  const parentSnap = (await app.getExecution(execution))!;
  assert.equal(parentSnap.status, "cancelled");

  const { app: _, pool } = env;
  const executions = `otra.x_${execution.queueId.replaceAll("-", "")}`;
  const { rows } = await pool.query(
    `select function_name, status from ${executions}
      where root_id = $1 and parent_id = $2`,
    [execution.rootId, execution.executionId],
  );
  const statuses = new Map(
    rows.map((r: { function_name: string; status: string }) => [
      r.function_name,
      r.status,
    ]),
  );
  assert.equal(statuses.get("lingering"), "cancelled");
  assert.equal(statuses.get("audit"), "suspended"); // untouched

  // The detached child finishes later and settles its promise against the
  // already-terminal parent without error.
  await env.advance(6);
  await worker.drain();
  const { rows: after } = await pool.query(
    `select status from ${executions} where function_name = 'audit'`,
  );
  assert.equal(after[0].status, "completed");
});

test("a cancelled child rejects the parent's await as a cancellation", async () => {
  const { app } = env;
  const child = app.task("cancellable-child", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "child-done";
  });
  const parent = app.task("watching-parent", function* (_params: null, ctx) {
    const handle = yield* ctx.spawn(child, null);
    try {
      return yield* ctx.await(handle);
    } catch (err) {
      if (isCancellation(err)) return "child-was-cancelled";
      throw err;
    }
  });

  const execution = await app.spawn(parent, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();

  const { pool } = env;
  const { rows } = await pool.query(
    `select id from otra.x_${execution.queueId.replaceAll("-", "")}
      where root_id = $1 and function_name = 'cancellable-child'`,
    [execution.rootId],
  );
  await app.cancel({
    queueId: execution.queueId,
    rootId: execution.rootId,
    executionId: rows[0].id,
  }); // cancel the child only
  await worker.drain();

  // The parent itself was not cancelled; it handled the child's cancellation.
  assert.equal(await app.getResult(execution), "child-was-cancelled");
});

test("ctx.uninterruptible defers delivery until the critical section exits", async () => {
  const { app, pool } = env;
  const calls = { a: 0, b: 0, outside: 0 };

  const task = app.task("critical", function* (_params: null, ctx) {
    yield* ctx.run("first", () => "first");
    yield* ctx.sleep("1s");
    const result = yield* ctx.uninterruptible(function* () {
      yield* ctx.run("critical-a", () => {
        calls.a += 1;
      });
      yield* ctx.run("critical-b", () => {
        calls.b += 1;
      });
      return "committed";
    });
    yield* ctx.run("outside", () => {
      calls.outside += 1;
    });
    return result;
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // suspends on the sleep

  await app.cancel(execution); // wakes it with the flag set
  await env.advance(2); // sleep timer is due
  await worker.tick();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  // Both critical steps ran to completion and were recorded; delivery
  // happened at the first effect after the shield.
  assert.deepEqual(calls, { a: 1, b: 1, outside: 0 });
  const { rows } = await pool.query(
    `select count(*)::int as n
       from otra.p_${execution.queueId.replaceAll("-", "")}
      where root_id = $1 and execution_id = $2
        and key in ('critical-a', 'critical-b') and status = 'resolved'`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].n, 2);
});

test("cancel-vs-suspend race converges in both interleavings", async () => {
  const { pool } = env;
  const a = new pg.Client({ connectionString: env.connectionString });
  const b = new pg.Client({ connectionString: env.connectionString });
  await a.connect();
  await b.connect();
  const { rows: pid } = await b.query("select pg_backend_pid() as pid");
  const bPid = pid[0].pid;
  const blockedOnLock = () =>
    waitFor(
      async () => {
        const { rows } = await pool.query(
          "select wait_event_type from pg_stat_activity where pid = $1",
          [bPid],
        );
        return rows[0]?.wait_event_type === "Lock";
      },
      { label: "session B blocked on a row lock" },
    );

  try {
    // Interleaving 1: suspend lands first, cancel second -> cancel must see
    // the suspension and wake the execution.
    let { rows: s } = await pool.query(
      "select queue_id, root_id, execution_id from otra.spawn_local('racer', '{}'::jsonb, 'default')",
    );
    const first = s[0];
    await pool.query("select * from otra.claim_local('default', 'w1', 30, 1)");
    await pool.query(
      "select * from otra.create_sleep_local($1, $2, $3, 'w1', 's1', '$sleep', 3600)",
      [first.queue_id, first.root_id, first.execution_id],
    );
    await a.query("begin");
    await a.query(
      "select * from otra.suspend_local($1, $2, $3, 'w1', array['s1'])",
      [first.queue_id, first.root_id, first.execution_id],
    );
    const cancelling = b.query(
      "select * from otra.request_cancel_local($1, $2, $3)",
      [first.queue_id, first.root_id, first.execution_id],
    );
    await blockedOnLock();
    await a.query("commit");
    await cancelling;
    let { rows: after } = await pool.query(
      `select status, cancel_requested_at
         from otra.x_${first.queue_id.replaceAll("-", "")}
        where root_id = $1 and id = $2`,
      [first.root_id, first.execution_id],
    );
    assert.equal(after[0].status, "pending"); // woken, will deliver on claim
    assert.notEqual(after[0].cancel_requested_at, null);

    // Interleaving 2: cancel lands first, suspend second -> suspend must
    // refuse and report the cancel so the driver delivers instead of parking.
    // A separate queue so the leftover woken execution from interleaving 1
    // cannot be the one w2 claims.
    await env.app.createQueue("race-q2");
    ({ rows: s } = await pool.query(
      "select queue_id, root_id, execution_id from otra.spawn_local('racer-2', '{}'::jsonb, 'race-q2')",
    ));
    const second = s[0];
    await pool.query("select * from otra.claim_local('race-q2', 'w2', 30, 1)");
    await pool.query(
      "select * from otra.create_sleep_local($1, $2, $3, 'w2', 's1', '$sleep', 3600)",
      [second.queue_id, second.root_id, second.execution_id],
    );
    await a.query("begin");
    await a.query("select * from otra.request_cancel_local($1, $2, $3)", [
      second.queue_id,
      second.root_id,
      second.execution_id,
    ]);
    const parking = b.query(
      "select * from otra.suspend_local($1, $2, $3, 'w2', array['s1'])",
      [second.queue_id, second.root_id, second.execution_id],
    );
    await blockedOnLock();
    await a.query("commit");
    const { rows: parked } = await parking;
    assert.equal(parked[0].suspended, false);
    assert.equal(parked[0].cancel_requested, true);
    ({ rows: after } = await pool.query(
      `select status from otra.x_${second.queue_id.replaceAll("-", "")}
        where root_id = $1 and id = $2`,
      [second.root_id, second.execution_id],
    ));
    assert.equal(after[0].status, "running"); // never parked
  } finally {
    await a.end();
    await b.end();
  }
});

test("a failed attempt with a pending cancel retries into compensation, then finalizes cancelled", async () => {
  const { app } = env;
  const calls = { explode: 0, compensate: 0 };

  const task = app.task("fails-under-cancel", function* (_params: null, ctx) {
    // A successful first step, so the execution has history: a cancel then
    // flags it for delivery instead of finalizing an empty run in place.
    yield* ctx.run("prep", () => "prepared");
    try {
      yield* ctx.run("explode", () => {
        calls.explode += 1;
        throw new Error("step blew up");
      });
      return "unreachable";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => {
          calls.compensate += 1;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null, { maxAttempts: 5 });
  // Flag the cancel before the first attempt even starts.
  const worker = app.createWorker({ workerId: "w1" });
  await app.cancel(execution); // pending, but with history? none yet...
  // A pending execution with no history finalizes in place -- so instead,
  // run one failing attempt first, then cancel, then let the retry deliver.
  const second = await app.spawn(task, null, { maxAttempts: 5 });
  await worker.tick(); // second's attempt 1 fails (retry scheduled)
  await app.cancel(second);
  await env.advance(2); // past the backoff
  await worker.tick(); // claim sees the flag: deliver, compensate, finalize

  const snapshot = (await app.getExecution(second))!;
  assert.equal(snapshot.status, "cancelled");
  assert.equal(calls.compensate, 1);
  // The failing forward step was NOT re-executed on the cancel attempt:
  // delivery preempts it at the first unrecorded effect.
  assert.equal(calls.explode, 1);
  // And the first execution (cancelled with empty history) never ran at all.
  assert.equal((await app.getExecution(execution))!.status, "cancelled");
});

// --- delivery latency + claim-loss discovery regressions -------------------
// (found reviewing the queue-local rework; the first two predate it)

/** Resolve the physical table names for a queue (tests only). */
async function tablesFor(pool: pg.Pool, queue: string) {
  const { rows } = await pool.query(
    "select replace(id::text, '-', '') as s from otra.queues where name = $1",
    [queue],
  );
  const s = rows[0].s as string;
  return { x: `x_${s}`, p: `p_${s}` };
}

test("cancelling an execution parked on retry backoff delivers without waiting it out", async () => {
  const { app, pool } = env;
  const calls = { compensate: 0 };

  const task = app.task("slow-backoff", function* (_params: null, ctx) {
    try {
      yield* ctx.run("prep", () => "ready");
      yield* ctx.run("explode", () => {
        throw new Error("attempt failed");
      });
      return "unreachable";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => {
          calls.compensate += 1;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null, {
    maxAttempts: 5,
    retryStrategy: { kind: "fixed", base_s: 3600 },
  });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // attempt 1 fails; retry scheduled an hour out

  const { x } = await tablesFor(pool, "default");
  const before = await pool.query(
    `select status, run_after > otra.now() as parked from otra.${x} where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(before.rows[0].status, "pending");
  assert.equal(before.rows[0].parked, true);

  await app.cancel(execution, { reason: "no point retrying" });

  // The whole point: NO clock advance. The cancel must expedite the row.
  await worker.tick();
  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.equal(calls.compensate, 1);
});

test("a step failing with an undelivered cancel pending retries immediately, not after backoff", async () => {
  const { app } = env;
  const gate = new EventEmitter();
  const calls = { compensate: 0 };
  let releaseStep: (() => void) | null = null;

  const task = app.task("fails-under-cancel", function* (_params: null, ctx) {
    try {
      yield* ctx.run("prep", () => "ready");
      yield* ctx.run("blocking-step", async () => {
        gate.emit("entered");
        await new Promise<void>((resolve) => {
          releaseStep = resolve;
        });
        throw new Error("step failed while cancel was pending");
      });
      return "unreachable";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => {
          calls.compensate += 1;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null, {
    maxAttempts: 5,
    retryStrategy: { kind: "fixed", base_s: 3600 },
  });
  const worker = app.createWorker({ workerId: "w1" });

  const entered = once(gate, "entered");
  const inflight = worker.tick();
  await entered;
  await app.cancel(execution); // running: flag only ('requested')
  releaseStep!();
  await inflight; // the step's failure lands as a failed attempt

  // The retry must be immediate (cancel undelivered), not an hour out.
  await worker.tick();
  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  assert.equal(calls.compensate, 1);
});

test("kill() aborts ctx.signal mid-step; the hung step unblocks and the drive abandons", async () => {
  const { app, pool } = env;
  const gate = new EventEmitter();
  let sawAbort = false;

  const task = app.task("hangs-forever", function* (_params: null, ctx) {
    yield* ctx.run("hang", () => {
      gate.emit("entered");
      return new Promise<string>((resolve) => {
        // A well-behaved long step: it only ends when the signal fires.
        ctx.signal.addEventListener(
          "abort",
          () => {
            sawAbort = true;
            resolve("aborted");
          },
          { once: true },
        );
      });
    });
    return "unreachable";
  });

  const execution = await app.spawn(task, null);
  // claimSeconds=1 floors the heartbeat at its 500ms minimum.
  const worker = app.createWorker({ workerId: "w1", claimSeconds: 1 });

  const entered = once(gate, "entered");
  const inflight = worker.tick();
  await entered;
  await app.kill(execution, { reason: "operator kill" });

  // Without heartbeat discovery this hangs forever; the abort must arrive
  // on the next heartbeat (~500ms).
  await inflight;

  assert.equal(sawAbort, true);
  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  // The zombie write was refused: nothing landed in the journal.
  const { p } = await tablesFor(pool, "default");
  const { rows } = await pool.query(
    `select 1 from otra.${p} where root_id = $1 and execution_id = $2 and key = 'hang'`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows.length, 0);
});

test("a stolen claim aborts ctx.signal mid-step and the drive abandons quietly", async () => {
  const { app, pool } = env;
  const gate = new EventEmitter();
  let sawAbort = false;

  const task = app.task("steal-me", function* (_params: null, ctx) {
    yield* ctx.run("hang", () => {
      gate.emit("entered");
      return new Promise<string>((resolve) => {
        ctx.signal.addEventListener(
          "abort",
          () => {
            sawAbort = true;
            resolve("aborted");
          },
          { once: true },
        );
      });
    });
    return "unreachable";
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1", claimSeconds: 1 });

  const entered = once(gate, "entered");
  const inflight = worker.tick();
  await entered;
  // Simulate another worker taking over after a lease expiry.
  const { x, p } = await tablesFor(pool, "default");
  await pool.query(
    `update otra.${x} set claimed_by = 'thief', claim_expires_at = otra.now() + interval '1h' where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );

  await inflight;

  assert.equal(sawAbort, true);
  // The thief still owns it; w1 wrote nothing.
  const { rows } = await pool.query(
    `select status, claimed_by from otra.${x} where root_id = $1 and id = $2`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(rows[0].status, "running");
  assert.equal(rows[0].claimed_by, "thief");
  const journal = await pool.query(
    `select 1 from otra.${p} where root_id = $1 and execution_id = $2 and key = 'hang'`,
    [execution.rootId, execution.executionId],
  );
  assert.equal(journal.rows.length, 0);
});

test("uninterruptible survives suspension: delivery waits for the shield to exit", async () => {
  const { app } = env;
  const calls = { a: 0, b: 0, outside: 0, compensate: 0 };

  const task = app.task("shielded-sleep", function* (_params: null, ctx) {
    try {
      yield* ctx.run("prep", () => "ready");
      yield* ctx.uninterruptible(function* () {
        yield* ctx.run("critical-a", () => {
          calls.a += 1;
        });
        // The critical section SUSPENDS. A cancel arriving while parked
        // must not split the section in half.
        yield* ctx.sleep("1s");
        yield* ctx.run("critical-b", () => {
          calls.b += 1;
        });
      });
      yield* ctx.run("outside", () => {
        calls.outside += 1;
      });
      return "done";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => {
          calls.compensate += 1;
        });
      }
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // parks on the in-shield sleep
  assert.equal((await app.getExecution(execution))!.status, "suspended");

  await app.cancel(execution, { reason: "mid-shield cancel" });
  await worker.tick(); // woken; must re-park (or finish the shield), never split it
  await env.advance(2); // the in-shield sleep comes due
  await worker.drain();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled");
  // The whole critical section committed; delivery landed after the shield.
  assert.deepEqual(calls, { a: 1, b: 1, outside: 0, compensate: 1 });
});

test("user labels may not use the engine-reserved '$' prefix", async () => {
  const { app } = env;
  // A user step keyed '$cancel' would occupy the cancellation journal slot,
  // making the execution permanently un-cancellable and discarding its
  // result on return. Reject the whole '$' namespace up front.
  const task = app.task("label-squatter", function* (_params: null, ctx) {
    return yield* ctx.run("$cancel", () => "hijack");
  });
  const execution = await app.spawn(task, null, { maxAttempts: 5 });
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "failed"); // permanent: retries can't help
  assert.equal(snapshot.error?.name, "DeterminismViolationError");
  assert.match(snapshot.error?.message ?? "", /reserved/);
  assert.equal(snapshot.attempt, 1);

  // The engine's own defaults still work, including explicit pass-through.
  const ok = app.task("default-labels", function* (_params: null, ctx) {
    yield* ctx.sleep(0, "$sleep");
    return yield* ctx.now();
  });
  const okRef = await app.spawn(ok, null);
  await worker.drain();
  assert.equal((await app.getExecution(okRef))!.status, "completed");
});
