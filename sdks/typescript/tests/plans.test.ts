import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import pg from "pg";

import { createTestEnv, type TestEnv } from "./helpers.ts";

// EXPLAIN-plan regression tests, using the technique from absurd's
// tests/test_indexes.py: turn sequential scans off on a dedicated session,
// EXPLAIN the REAL hot statement out of sql/schema.sql, and assert that the
// index we built for that statement appears in the plan by name.
//
// Nothing else in this suite can catch this class of regression. An index
// that stops matching its query -- a predicate reworded so a partial WHERE no
// longer implies, a column dropped from an ORDER BY, a sweep gaining a
// conjunct the index does not cover -- returns exactly the same rows. Only
// the plan rots, and it rots silently until a production queue is deep enough
// for the sequential scan to matter.
//
// Assertions are on the index NAME only, deliberately: node types, costs,
// Incremental Sort steps, parallel workers and Bitmap wrappers are all free to
// change with the planner and the data. What must not change is WHICH index
// answers the query.
//
// The statements below are transcriptions of the ones inside the stored
// functions -- same relations, same predicates, same ORDER BY -- with literal
// parameters substituted for the plpgsql placeholders. Keep them in sync with
// their sources; each test names the function it came from.

const QUEUE = "plans";
const X = `otra.x_${QUEUE}`;
const P = `otra.p_${QUEUE}`;

let env: TestEnv;
let client: pg.Client;

beforeEach(async () => {
  env = await createTestEnv(QUEUE);
  client = new pg.Client({ connectionString: env.connectionString });
  await client.connect();
  // Sequential scans stay *possible* (this is a cost penalty, not a
  // prohibition), so a query no index can serve still shows a Seq Scan and
  // still fails its assertion. That is the point.
  await client.query("set enable_seqscan = off");
  await seed();
});

afterEach(async () => {
  await client?.end();
  await env?.close();
});

/**
 * A spread of executions and promises across the states these sweeps look
 * for, so the plans are the ones a live queue would get rather than the
 * planner's empty-relation defaults.
 */
async function seed(): Promise<void> {
  await client.query(
    // uuid_v7_floor, not uuid_v7: one distinct id per row without a lateral
    // (an uncorrelated lateral is evaluated once, which collides 90 ways),
    // and time-ordered, which is what a real spawn sequence produces.
    `insert into ${X} (id, root_id, function_name, status, run_after,
                       claimed_by, claim_expires_at, finished_at)
     select g.id, g.id, 'fn',
            case n % 3 when 0 then 'pending'
                       when 1 then 'running'
                       else 'completed' end,
            otra.now() + make_interval(secs => n),
            case when n % 3 = 1 then 'w1' end,
            case when n % 3 = 1 then otra.now() + make_interval(secs => n) end,
            case when n % 3 = 2 then otra.now() - make_interval(days => n) end
       from generate_series(1, 90) as n,
            lateral (select otra.uuid_v7_floor(
              otra.now() + make_interval(secs => n)) as id) g`,
  );
  await client.query(
    `insert into ${P} (root_id, execution_id, key, label, kind, status,
                       wake_at, event_name)
     select t.root_id, t.id, 'k1', 'k1',
            case when t.n % 2 = 0 then 'sleep' else 'event' end,
            'pending',
            case when t.n % 2 = 0
                 then otra.now() + make_interval(secs => t.n) end,
            case when t.n % 2 = 1 then 'evt-' || t.n end
       from (select id, root_id, row_number() over (order by id) as n
               from ${X}) t`,
  );
  await client.query(`analyze ${X}`);
  await client.query(`analyze ${P}`);
}

async function plan(sql: string): Promise<string> {
  const { rows } = await client.query(`explain (format text) ${sql}`);
  return rows
    .map((row: Record<string, string>) => row["QUERY PLAN"])
    .join("\n");
}

/** Assert on the index, and say what we got when it is not the one. */
function assertUses(planText: string, index: string): void {
  assert.ok(
    planText.includes(index),
    `expected index ${index} in plan:\n${planText}`,
  );
}

test("claim_local's claimable scan uses the pending/run_after index", async () => {
  // otra.claim_local, the final `return query`: the inner candidate select.
  const text = await plan(
    `select c.root_id, c.id
       from ${X} c
      where c.status = 'pending' and c.run_after <= otra.now()
      order by c.run_after, c.id
      limit 1
        for update skip locked`,
  );
  assertUses(text, `xi_${QUEUE}_ri`);
});

test("claim_local's expired-claim sweep uses the running/claim_expires_at index", async () => {
  // otra.claim_local, the v_crashed loop.
  const text = await plan(
    `select root_id, id from ${X}
      where status = 'running' and claim_expires_at <= otra.now()
      order by claim_expires_at, id limit 100`,
  );
  assertUses(text, `xi_${QUEUE}_cei`);
});

test("claim_local's due-sleep sweep uses the pending/wake_at promise index", async () => {
  // otra.claim_local, the first v_woken loop ("due" CTE).
  const text = await plan(
    `select root_id, id from ${P}
      where kind = 'sleep' and status = 'pending' and wake_at <= otra.now()
      order by wake_at, id limit 100 for update skip locked`,
  );
  assertUses(text, `pi_${QUEUE}_wi`);
});

test("cleanup's terminal-root candidate scan uses the finished_at index", async () => {
  // otra._cleanup_queue_local, the candidate select.
  const text = await plan(
    `select r.id from ${X} r
      where r.root_id = r.id
        and r.status in ('completed', 'failed', 'cancelled')
        and r.finished_at < otra.now() - interval '30 days'
        and not exists (
          select 1 from ${X} d
           where d.root_id = r.id
             and d.status not in ('completed', 'failed', 'cancelled')
        )
      order by r.finished_at, r.id limit 1000`,
  );
  assertUses(text, `xi_${QUEUE}_fin`);
});

test("the event-wait lookup by name uses the event_name promise index", async () => {
  // otra.emit_event_local, the "settled" CTE that resolves every pending wait
  // for the emitted name.
  const text = await plan(
    `select root_id, execution_id from ${P}
      where kind = 'event' and status = 'pending' and event_name = 'evt-1'`,
  );
  assertUses(text, `pi_${QUEUE}_ei`);
});
