import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import pg from "pg";

import { createTestEnv, waitFor, type TestEnv } from "./helpers.ts";

// Deterministic lock-contention tests, using the technique from absurd's
// tests/test_lock_ordering.py: session A takes a row lock and holds its
// transaction open, session B runs the operation under test and blocks, the
// test *observes the block* via pg_stat_activity (wait_event_type = 'Lock')
// instead of sleeping, then A commits and we assert B converged correctly.
//
// This turns the suspend-vs-resolve race -- the crux of otra's "no lost
// wakeups" claim, argued in a comment in sql/schema.sql -- into something
// the suite actually forces and verifies, in both interleavings.

const DSN =
  process.env.OTRA_TEST_DB ?? "postgres://postgres@127.0.0.1:5433/postgres";

let env: TestEnv;
let a: pg.Client;
let b: pg.Client;
let bPid: number;

beforeEach(async () => {
  env = await createTestEnv();
  a = new pg.Client({ connectionString: DSN });
  b = new pg.Client({ connectionString: DSN });
  await a.connect();
  await b.connect();
  const { rows } = await b.query("select pg_backend_pid() as pid");
  bPid = rows[0].pid;
});
afterEach(async () => {
  await a.end();
  await b.end();
  await env.close();
});

async function waitUntilBlockedOnLock(pid: number): Promise<void> {
  await waitFor(
    async () => {
      const { rows } = await env.pool.query(
        "select wait_event_type from pg_stat_activity where pid = $1",
        [pid],
      );
      return rows[0]?.wait_event_type === "Lock";
    },
    { label: `backend ${pid} blocked on a row lock` },
  );
}

/** Parent P claimed by w1 with a pending child promise; child C claimed by w2. */
async function parentAndChild(): Promise<{ parent: string; child: string }> {
  const { pool } = env;
  const { rows: p } = await pool.query(
    "select execution_id from otra.spawn('parent-fn', '{}'::jsonb)",
  );
  const parent = p[0].execution_id as string;
  await pool.query("select * from otra.claim('default', 'w1', 30, 1)");
  const { rows: c } = await pool.query(
    "select execution_id from otra.spawn('child-fn', '{}'::jsonb, 'default', '{}', $1, 'child-key', 'child-fn', 'w1')",
    [parent],
  );
  const child = c[0].execution_id as string;
  const { rows: claimed } = await pool.query(
    "select execution_id from otra.claim('default', 'w2', 30, 1)",
  );
  assert.equal(claimed[0].execution_id, child);
  return { parent, child };
}

test("suspend-then-resolve: a child completing against a parking parent still wakes it", async () => {
  const { parent, child } = await parentAndChild();

  // A parks the parent and holds the transaction open: the parent's row lock
  // is held, the suspension is not yet visible.
  await a.query("begin");
  const { rows: suspendResult } = await a.query(
    "select suspended from otra.suspend($1, 'w1', array['child-key'])",
    [parent],
  );
  assert.equal(suspendResult[0].suspended, true);

  // B completes the child; its wake of the parent must block on A's lock.
  const completing = b.query("select otra.complete($1, 'w2', '\"r\"'::jsonb)", [
    child,
  ]);
  await waitUntilBlockedOnLock(bPid);

  await a.query("commit");
  await completing;

  // The suspension landed first, the resolution second: the parent must be
  // woken, not left suspended with a settled blocker (the lost wakeup).
  const snapshot = (await env.app.getExecution(parent))!;
  assert.equal(snapshot.status, "pending");
  const { rows: promise } = await env.pool.query(
    "select status, value from otra.promises where execution_id = $1 and key = 'child-key'",
    [parent],
  );
  assert.equal(promise[0].status, "resolved");
});

test("resolve-then-suspend: a parent parking against a completed child refuses to park", async () => {
  const { parent, child } = await parentAndChild();

  // A completes the child and holds the transaction open: the child promise
  // is settled (invisibly) and the parent's row lock is held by _wake.
  await a.query("begin");
  await a.query("select otra.complete($1, 'w2', '\"r\"'::jsonb)", [child]);

  // B (the parent's worker) tries to park; it must block on the parent row.
  const parking = b.query(
    "select suspended from otra.suspend($1, 'w1', array['child-key'])",
    [parent],
  );
  await waitUntilBlockedOnLock(bPid);

  await a.query("commit");
  const { rows: parked } = await parking;

  // The resolution landed first: suspend must see the settled blocker and
  // refuse, sending the worker back to replay (the redrive path) instead of
  // parking an execution nothing will ever wake.
  assert.equal(parked[0].suspended, false);
  assert.equal((await env.app.getExecution(parent))!.status, "running");
});
