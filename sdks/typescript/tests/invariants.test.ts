import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { isCancellation } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// otra.check_invariants(queue) is the machine-checked oracle for the
// cross-table consistency rules the engine promises: every "paid-for
// lesson" that used to live in prose becomes a row this function returns
// when violated. Chaos and fuzz tests call it after every run; these tests
// prove it (a) stays silent on a genuinely busy healthy queue and (b)
// actually names each class of corruption when one is seeded by hand.

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env?.close();
});

async function violations(env: TestEnv, queue: string): Promise<string[]> {
  const { rows } = await env.pool.query(
    "select violation from otra.check_invariants($1) order by violation",
    [queue],
  );
  return rows.map((r: { violation: string }) => r.violation);
}

test("a busy healthy queue reports zero invariant violations", async () => {
  const { app } = env;

  const child = app.task("inv-child", function* (_params: null, ctx) {
    yield* ctx.run("child-step", () => 1);
    return "child-done";
  });
  app.task("inv-parent", function* (_params: null, ctx) {
    yield* ctx.run("step", () => "x");
    const result = yield* ctx.call(child, null);
    yield* ctx.sleep("1s");
    return result;
  });
  app.task("inv-waiter", function* (_params: null, ctx) {
    return yield* ctx.waitForEvent("inv-go");
  });
  app.task("inv-flaky", function* (_params: null, ctx) {
    yield* ctx.run("boom", () => {
      throw new Error("attempt fails");
    });
  });
  app.task("inv-compensating", function* (_params: null, ctx) {
    try {
      yield* ctx.sleep("1h");
      return "never";
    } catch (err) {
      if (isCancellation(err)) {
        yield* ctx.run("compensate", () => "cleaned");
      }
      throw err;
    }
  });

  const worker = app.createWorker({ workerId: "w1" });
  const parent = await app.spawn("inv-parent", null);
  const waiter = await app.spawn("inv-waiter", null);
  const flaky = await app.spawn("inv-flaky", null, { maxAttempts: 2 });
  const compensating = await app.spawn("inv-compensating", null);
  const killed = await app.spawn("inv-waiter", null);

  await worker.drain(); // parent parks on sleep; waiters park; flaky retries
  await env.advance(2);
  await worker.drain(); // sleep due; flaky retry due -> permanent failure
  await app.emitEvent("inv-go", { ok: true });
  await app.cancel(compensating);
  await app.kill(killed);
  await worker.drain(); // event waiter completes; compensation runs

  // Mixed terminal states: completed, failed, cancelled (both flavors),
  // plus settled child promises and retained event facts.
  assert.equal((await app.getExecution(parent))!.status, "completed");
  assert.equal((await app.getExecution(flaky))!.status, "failed");
  assert.equal((await app.getExecution(compensating))!.status, "cancelled");
  assert.equal((await app.getExecution(killed))!.status, "cancelled");
  assert.equal((await app.getExecution(waiter))!.status, "completed");

  assert.deepEqual(await violations(env, "default"), []);
});

test("each seeded corruption is named by the checker", async () => {
  const { app, pool } = env;

  // A real suspended execution to corrupt (parked on a 1h sleep).
  app.task("inv-sleeper", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "ok";
  });
  const child = app.task("inv-c2", function* () {
    return "c";
  });
  app.task("inv-p2", function* (_params: null, ctx) {
    return yield* ctx.call(child, null);
  });

  const sleeper = await app.spawn("inv-sleeper", null);
  const parent = await app.spawn("inv-p2", null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain();
  assert.equal((await app.getExecution(sleeper))!.status, "suspended");
  assert.equal((await app.getExecution(parent))!.status, "completed");
  assert.deepEqual(await violations(env, "default"), []);

  // 1. Lost wakeup made visible: a suspended execution none of whose
  //    promises are pending can never be woken by anything.
  await pool.query(
    `update otra.p_default set status = 'resolved', settled_at = otra.now()
      where execution_id = $1 and status = 'pending'`,
    [sleeper.executionId],
  );
  // 2. Lost settlement: a child-kind promise still pending although the
  //    child execution reached a terminal state.
  await pool.query(
    `update otra.p_default set status = 'pending', value = null, settled_at = null
      where execution_id = $1 and kind = 'child'`,
    [parent.executionId],
  );
  // 3. Claim-field incoherence: running without a claim.
  const orphan = await app.spawn("inv-sleeper", null);
  await pool.query(
    `update otra.x_default set status = 'running', claimed_by = null,
            claim_expires_at = null
      where id = $1`,
    [orphan.executionId],
  );
  // 4. Attempt accounting: attempt beyond max_attempts.
  const overdrawn = await app.spawn("inv-sleeper", null);
  await pool.query(
    `update otra.x_default set attempt = max_attempts + 1 where id = $1`,
    [overdrawn.executionId],
  );
  // 5. Terminal/finished_at incoherence.
  const unfinished = await app.spawn("inv-sleeper", null);
  await pool.query(
    `update otra.x_default set status = 'completed', finished_at = null
      where id = $1`,
    [unfinished.executionId],
  );
  // 6. Cancelled without a recorded request.
  const unrequested = await app.spawn("inv-sleeper", null);
  await pool.query(
    `update otra.x_default set status = 'cancelled',
            finished_at = otra.now(), cancel_requested_at = null
      where id = $1`,
    [unrequested.executionId],
  );

  const found = await violations(env, "default");
  const expectClasses = [
    "stuck-suspended",
    "lost-settlement",
    "claim-incoherent",
    "attempt-overflow",
    "terminal-incoherent",
    "cancelled-unrequested",
  ];
  for (const cls of expectClasses) {
    assert.ok(
      found.some((v) => v.startsWith(cls)),
      `expected a "${cls}" violation, got: ${JSON.stringify(found)}`,
    );
  }
  assert.equal(found.length, expectClasses.length, JSON.stringify(found));
});
