import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { afterEach, beforeEach, test } from "node:test";

import { TimeoutError, isCancellation } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// External promises (ctx.promise): the sixth promise kind, per the Solfège
// design review. One new ctx method returning a normal handle plus a token;
// outside code settles exactly that promise with app.resolvePromise /
// rejectPromise. No new redemption verbs -- ctx.await / ctx.all redeem the
// handle like any child handle.

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

test("human-in-the-loop: outside code resolves a promise by token", async () => {
  const { app } = env;
  let handedOut: string | undefined;

  const task = app.task("approve-expense", function* (_params: null, ctx) {
    const approval = yield* ctx.promise<{ approvedBy: string }>("approval");
    yield* ctx.run("notify", () => {
      handedOut = approval.token; // the token travels through the real world
    });
    const decision = yield* ctx.await(approval);
    return `approved by ${decision.approvedBy}`;
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  assert.equal((await app.getExecution(execution))!.status, "suspended");
  assert.ok(handedOut!.startsWith("otr1_"), `token looks opaque: ${handedOut}`);

  const settled = await app.resolvePromise(handedOut!, { approvedBy: "hazel" });
  assert.equal(settled, true);
  await worker.tick();
  assert.equal(await app.getResult(execution), "approved by hazel");
});

test("external promises are write-once", async () => {
  const { app } = env;
  let token: string | undefined;

  const task = app.task("once-only", function* (_params: null, ctx) {
    const p = yield* ctx.promise<{ n: number }>("slot");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    return (yield* ctx.await(p)).n;
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  assert.equal(await app.resolvePromise(token!, { n: 1 }), true);
  // The second settle loses: write-once, first value is canonical.
  assert.equal(await app.resolvePromise(token!, { n: 2 }), false);
  assert.equal(await app.rejectPromise(token!, "too late"), false);

  await worker.tick();
  assert.equal(await app.getResult(execution), 1);
});

test("rejecting an external promise throws at the await, catchably", async () => {
  const { app } = env;
  let token: string | undefined;

  const task = app.task("deniable", function* (_params: null, ctx) {
    const p = yield* ctx.promise("request");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    try {
      yield* ctx.await(p);
      return "granted";
    } catch (err) {
      return `denied: ${(err as Error).message}`;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  assert.equal(await app.rejectPromise(token!, "budget exceeded"), true);
  await worker.tick();
  assert.equal(await app.getResult(execution), "denied: budget exceeded");
});

test("external promises can time out, rejecting with TimeoutError", async () => {
  const { app } = env;

  const task = app.task("impatient", function* (_params: null, ctx) {
    const p = yield* ctx.promise("approval", { timeout: "1h" });
    try {
      yield* ctx.await(p);
      return "arrived";
    } catch (err) {
      if (err instanceof TimeoutError) return "gave-up";
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  assert.equal((await app.getExecution(execution))!.status, "suspended");

  await env.advance(3601);
  await worker.tick();
  assert.equal(await app.getResult(execution), "gave-up");
});

test("resolve-then-await: a promise settled while the task was elsewhere injects without suspending", async () => {
  const { app } = env;
  let token: string | undefined;

  const task = app.task("early-settle", function* (_params: null, ctx) {
    const p = yield* ctx.promise<{ v: string }>("slot");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    yield* ctx.sleep("10s"); // suspends on the timer, not on the promise
    const value = yield* ctx.await(p);
    return value.v;
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick(); // parked on the sleep

  assert.equal(await app.resolvePromise(token!, { v: "early" }), true);
  await env.advance(11);
  await worker.tick(); // timer due; replay finds the promise already resolved
  assert.equal(await app.getResult(execution), "early");
});

test("tokens are replay-stable and ctx.all mixes external and child handles", async () => {
  const { app } = env;
  const seenTokens: string[] = [];
  let token: string | undefined;

  const child = app.task("mix-child", function* (_params: null, ctx) {
    return yield* ctx.run("c", () => "from-child");
  });

  const task = app.task("mixer", function* (_params: null, ctx) {
    const external = yield* ctx.promise<string>("signal");
    seenTokens.push(external.token); // generator body: runs once per replay
    yield* ctx.run("leak", () => {
      token = external.token;
    });
    const childHandle = yield* ctx.spawn(child, null);
    const [fromOutside, fromChild] = yield* ctx.all([external, childHandle]);
    return `${fromOutside}+${fromChild}`;
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.drain(); // child completes; parent still blocked on external

  assert.equal((await app.getExecution(execution))!.status, "suspended");
  await app.resolvePromise(token!, "from-outside");
  await worker.drain();

  assert.equal(await app.getResult(execution), "from-outside+from-child");
  // The body replayed at least twice; the token never changed.
  assert.ok(seenTokens.length >= 2);
  assert.equal(new Set(seenTokens).size, 1);
});

test("cancelling an execution parked on an external promise unwinds it", async () => {
  const { app } = env;

  const task = app.task("cancellable-waiter", function* (_params: null, ctx) {
    const p = yield* ctx.promise("never");
    try {
      yield* ctx.await(p);
      return "resolved";
    } catch (err) {
      if (isCancellation(err)) return "unwound";
      throw err;
    }
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();
  await app.cancel(execution);
  await worker.tick();

  const snapshot = (await app.getExecution(execution))!;
  assert.equal(snapshot.status, "cancelled"); // engine-owned outcome
});

test("only external promises can be settled from outside", async () => {
  const { app, pool } = env;
  const task = app.task("internal-only", function* (_params: null, ctx) {
    yield* ctx.run("step", () => 42);
    yield* ctx.sleep("1h");
    return "done";
  });

  const execution = await app.spawn(task, null);
  const worker = app.createWorker({ workerId: "w1" });
  await worker.tick();

  // Grab the internal run checkpoint's row id and try to settle it.
  const { rows } = await pool.query(
    `select id from otra.p_${execution.queueId.replaceAll("-", "")}
      where root_id = $1 and execution_id = $2 and key = 'step'`,
    [execution.rootId, execution.executionId],
  );
  const token = `otr1_${Buffer.from(
    `${execution.queueId}:${execution.rootId}:${rows[0].id}`,
  ).toString("base64url")}`;

  // Loud, not a quiet false: "false" means "already settled, you lost the
  // race", and a run checkpoint is not a race anyone can win.
  await assert.rejects(
    app.resolvePromise(token, "hijack"),
    /does not name an external promise/,
  );
  await assert.rejects(
    app.rejectPromise(token, "hijack"),
    /does not name an external promise/,
  );

  // The single-author journal is intact: the run row still holds its value.
  const after = await pool.query(
    `select kind, status, value from otra.p_${execution.queueId.replaceAll("-", "")}
      where root_id = $1 and id = $2`,
    [execution.rootId, rows[0].id],
  );
  assert.deepEqual(after.rows[0], {
    kind: "run",
    status: "resolved",
    value: 42,
  });

  // And a malformed token is rejected loudly, not treated as a miss.
  await assert.rejects(
    app.resolvePromise("not-a-token", 1),
    /invalid promise token/,
  );
});

test("a well-formed token naming nothing throws instead of reporting a miss", async () => {
  const { app } = env;
  const task = app.task("bystander", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "done";
  });
  const execution = await app.spawn(task, null);
  await app.createWorker({ workerId: "w1" }).tick();

  const nowhere = `otr1_${Buffer.from(
    `${execution.queueId}:${randomUUID()}:${randomUUID()}`,
  ).toString("base64url")}`;
  await assert.rejects(
    app.resolvePromise(nowhere, "hello"),
    /does not name an external promise/,
  );
  await assert.rejects(
    app.rejectPromise(nowhere, "nope"),
    /does not name an external promise/,
  );

  // A token whose queue does not exist either is loud too, never a quiet miss.
  const elsewhere = `otr1_${Buffer.from(
    `${randomUUID()}:${randomUUID()}:${randomUUID()}`,
  ).toString("base64url")}`;
  await assert.rejects(
    app.resolvePromise(elsewhere, "hello"),
    /does not exist/,
  );
});

test("spawning a child onto a key held by another promise kind fails loudly", async () => {
  const { app, pool } = env;
  app.task("host", function* (_params: null, ctx) {
    yield* ctx.sleep("1h");
    return "done";
  });
  await app.spawn("host", null);

  // Claim through SQL so the execution is 'running' under a known worker id.
  const claimed = await pool.query(
    `select queue_id, root_id, execution_id
       from otra.claim_local('default', 'w-direct', 30, 1)`,
  );
  const { queue_id, root_id, execution_id } = claimed.rows[0];

  // A run checkpoint already occupies the key the child spawn will want.
  await pool.query(
    `insert into otra.p_${queue_id.replaceAll("-", "")}
       (root_id, execution_id, key, label, kind, status, value, settled_at)
     values ($1, $2, 'audit', 'audit', 'run', 'resolved', '1'::jsonb, otra.now())`,
    [root_id, execution_id],
  );

  // The old code fell through to the insert and surfaced a bare 23505.
  await assert.rejects(
    pool.query(
      `select * from otra.spawn_child_local(
         $1, $2, $3, 'w-direct', 'audit', 'audit', 'child-fn', 'null'::jsonb
       )`,
      [queue_id, root_id, execution_id],
    ),
    (err: unknown) => {
      const failure = err as { code?: string; message?: string };
      assert.equal(failure.code, "OT003");
      assert.match(failure.message ?? "", /"audit"/);
      assert.match(failure.message ?? "", /run/);
      return true;
    },
  );
});

test("settling an already-settled external promise returns false", async () => {
  const { app } = env;
  let token: string | undefined;

  const task = app.task("second-place", function* (_params: null, ctx) {
    const p = yield* ctx.promise<string>("slot");
    yield* ctx.run("leak", () => {
      token = p.token;
    });
    return yield* ctx.await(p);
  });

  await app.spawn(task, null);
  await app.createWorker({ workerId: "w1" }).tick();

  assert.equal(await app.resolvePromise(token!, "winner"), true);
  // Losing a settled race is an ordinary outcome, reported as false -- it is
  // NOT the same condition as "that token names no external promise".
  assert.equal(await app.resolvePromise(token!, "loser"), false);
  assert.equal(await app.rejectPromise(token!, "loser"), false);
});
