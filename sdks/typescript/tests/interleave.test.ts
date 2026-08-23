import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import pg from "pg";

import { createTestEnv, type TestEnv } from "./helpers.ts";

// Pairwise serializability differential: the machine-checked form of the
// engine's concurrency contract. Every coordination function is one
// transaction, so for ANY two calls fired concurrently the observable end
// state must equal what SOME serial order of the same two calls produces
// (plus: never a deadlock). The lock-ordering tests prove specific
// interleavings; this proves the CONTRACT over the whole op matrix:
//
//   for each unordered pair (op1, op2), including self-pairs:
//     O12 := canonical state after serial op1 -> op2   (fresh fixture)
//     O21 := canonical state after serial op2 -> op1   (fresh fixture)
//     T times: fire both concurrently on a fresh fixture;
//       assert outcome IN {O12, O21}, no 40P01, oracle silent.
//
// The fixture is built through raw SQL calls (no driving workers), giving
// exact control: parent suspended on all([child, external]); child claimed
// by 'wc' but never driven; a second root suspended on event 'ev'. Every
// op targets that shared state, so concurrent pairs genuinely contend on
// the same rows. The frozen fake clock makes timestamps identical across
// queues, which is what lets states compare bytewise.
//
// OTRA_INTERLEAVE_TRIALS overrides the concurrent trials per pair.
const TRIALS = Number(process.env.OTRA_INTERLEAVE_TRIALS ?? 4);

interface Fixture {
  queue: string;
  queueId: string;
  parent: string; // root of its own tree
  child: string;
  waiter: string; // root of its own tree
  waiterRoot: string;
  extId: string;
}

let env: TestEnv;
before(async () => {
  env = await createTestEnv("default", false);
});
after(async () => {
  await env?.close();
});

async function buildFixture(queue: string): Promise<Fixture> {
  const { pool } = env;
  await pool.query("select otra.create_queue($1)", [queue]);

  const { rows: p } = await pool.query(
    `select queue_id, root_id, execution_id
       from otra.spawn_local('il-parent', '{}'::jsonb, $1)`,
    [queue],
  );
  const queueId = p[0].queue_id as string;
  const parent = p[0].execution_id as string;
  const { rows: claimed } = await pool.query(
    "select execution_id from otra.claim_local($1, 'wp', 3600, 1)",
    [queue],
  );
  assert.equal(claimed[0].execution_id, parent);

  const { rows: c } = await pool.query(
    `select execution_id from otra.spawn_child_local(
       $1::uuid, $2::uuid, $2::uuid, 'wp', 'kid', 'kid', 'il-child',
       '{}'::jsonb)`,
    [queueId, parent],
  );
  const child = c[0].execution_id as string;
  const { rows: x } = await pool.query(
    `select id from otra.create_external_local(
       $1::uuid, $2::uuid, $2::uuid, 'wp', 'ext', 'ext', null)`,
    [queueId, parent],
  );
  const extId = x[0].id as string;
  const { rows: parked } = await pool.query(
    `select suspended from otra.suspend_local(
       $1::uuid, $2::uuid, $2::uuid, 'wp', array['kid','ext'], false)`,
    [queueId, parent],
  );
  assert.equal(parked[0].suspended, true);

  // The child is the only pending execution now: claim it as 'wc' and
  // leave it undriven, so terminal transitions on it are legal ops.
  const { rows: cc } = await pool.query(
    "select execution_id from otra.claim_local($1, 'wc', 3600, 1)",
    [queue],
  );
  assert.equal(cc[0].execution_id, child);

  // Second root: parked on event 'ev'.
  const { rows: w } = await pool.query(
    `select root_id, execution_id
       from otra.spawn_local('il-waiter', '{}'::jsonb, $1)`,
    [queue],
  );
  const waiter = w[0].execution_id as string;
  const waiterRoot = w[0].root_id as string;
  const { rows: wclaim } = await pool.query(
    "select execution_id from otra.claim_local($1, 'ww', 3600, 1)",
    [queue],
  );
  assert.equal(wclaim[0].execution_id, waiter);
  await pool.query(
    `select * from otra.create_event_wait_local(
       $1::uuid, $2::uuid, $3::uuid, 'ww', 'evwait', 'evwait', 'ev', null)`,
    [queueId, waiterRoot, waiter],
  );
  const { rows: wparked } = await pool.query(
    `select suspended from otra.suspend_local(
       $1::uuid, $2::uuid, $3::uuid, 'ww', array['evwait'], false)`,
    [queueId, waiterRoot, waiter],
  );
  assert.equal(wparked[0].suspended, true);

  return { queue, queueId, parent, child, waiter, waiterRoot, extId };
}

// Each op runs on its own client (concurrency = two backends), returns the
// Postgres error code it raised, or null. Return VALUES are not compared:
// the contract under test is state equivalence.
type Runner = (client: pg.PoolClient, fx: Fixture) => Promise<string | null>;

async function run(
  client: pg.PoolClient,
  text: string,
  values: unknown[],
): Promise<string | null> {
  try {
    await client.query(text, values);
    return null;
  } catch (err) {
    const code = (err as { code?: string }).code ?? "unknown";
    if (code === "40P01") throw err; // a deadlock is never acceptable
    return code;
  }
}

const OPS: Record<string, Runner> = {
  completeChild: (cl, fx) =>
    run(
      cl,
      `select otra.complete_local($1::uuid, $2::uuid, $3::uuid, 'wc', '"r"'::jsonb)`,
      [fx.queueId, fx.parent, fx.child],
    ),
  failChild: (cl, fx) =>
    run(
      cl,
      `select * from otra.fail_attempt_local(
         $1::uuid, $2::uuid, $3::uuid, 'wc',
         '{"name":"Error","message":"boom"}'::jsonb, false)`,
      [fx.queueId, fx.parent, fx.child],
    ),
  emitEvent: (cl, fx) =>
    run(cl, `select otra.emit_event_local($1, 'ev', '{"v":1}'::jsonb)`, [
      fx.queue,
    ]),
  resolveExternal: (cl, fx) =>
    run(
      cl,
      `select otra.resolve_promise_local($1::uuid, $2::uuid, $3::uuid, '"x"'::jsonb)`,
      [fx.queueId, fx.parent, fx.extId],
    ),
  cancelParent: (cl, fx) =>
    run(
      cl,
      `select * from otra.request_cancel_local($1::uuid, $2::uuid, $2::uuid, true, null)`,
      [fx.queueId, fx.parent],
    ),
  cancelChild: (cl, fx) =>
    run(
      cl,
      `select * from otra.request_cancel_local($1::uuid, $2::uuid, $3::uuid, true, null)`,
      [fx.queueId, fx.parent, fx.child],
    ),
  killParent: (cl, fx) =>
    run(
      cl,
      `select otra.kill_local($1::uuid, $2::uuid, $2::uuid, true, null)`,
      [fx.queueId, fx.parent],
    ),
};

/** Everything observable, normalized to entity roles instead of ids. */
async function canonicalState(fx: Fixture): Promise<string> {
  const { pool } = env;
  const roles = new Map([
    [fx.parent, "parent"],
    [fx.child, "child"],
    [fx.waiter, "waiter"],
  ]);
  const { rows: execs } = await pool.query(
    `select id, status, attempt, claimed_by,
            (cancel_requested_at is not null) as cancel_requested,
            error ->> 'message' as error_message
       from otra."${"x_" + fx.queue}" order by id`,
  );
  const { rows: promises } = await pool.query(
    `select execution_id, key, kind, status, value::text as value,
            error ->> 'name' as error_name
       from otra."${"p_" + fx.queue}" order by execution_id, key`,
  );
  const { rows: events } = await pool.query(
    `select name, payload::text as payload from otra."${"e_" + fx.queue}"
      order by name`,
  );
  // Sort by ROLE, not by id: uuids are fresh per fixture, so id order is
  // meaningless across the queues being compared.
  const byWho = (
    a: { who: string; key?: string },
    b: { who: string; key?: string },
  ) =>
    a.who === b.who
      ? (a.key ?? "").localeCompare(b.key ?? "")
      : a.who.localeCompare(b.who);
  const executions = execs
    .map((e: Record<string, unknown>) => ({
      who: (roles.get(e.id as string) ?? "other") as string,
      status: e.status,
      attempt: e.attempt,
      claimedBy: e.claimed_by,
      cancelRequested: e.cancel_requested,
      error: e.error_message,
    }))
    .sort(byWho);
  const journal = promises
    .map((p: Record<string, unknown>) => ({
      who: (roles.get(p.execution_id as string) ?? "other") as string,
      key: p.key as string,
      kind: p.kind,
      status: p.status,
      value: p.value,
      error: p.error_name,
    }))
    .sort(byWho);
  return JSON.stringify({ executions, promises: journal, events });
}

interface Outcome {
  state: string;
  errors: string[]; // sorted `${op}:${code}` for ops that raised
}

async function serialOutcome(
  queue: string,
  first: string,
  second: string,
): Promise<Outcome> {
  const fx = await buildFixture(queue);
  const client = await env.pool.connect();
  try {
    const e1 = await OPS[first]!(client, fx);
    const e2 = await OPS[second]!(client, fx);
    const errors = [
      ...(e1 ? [`${first}:${e1}`] : []),
      ...(e2 ? [`${second}:${e2}`] : []),
    ].sort();
    return { state: await canonicalState(fx), errors };
  } finally {
    client.release();
  }
}

test("every concurrent op pair is equivalent to one of its serial orders", async () => {
  const names = Object.keys(OPS);
  const pairs: Array<[string, string]> = [];
  for (let i = 0; i < names.length; i++) {
    for (let j = i; j < names.length; j++) {
      pairs.push([names[i]!, names[j]!]);
    }
  }

  for (const [op1, op2] of pairs) {
    const tag = `${op1}__${op2}`.toLowerCase();
    const serial12 = await serialOutcome(`il ${tag} a`, op1, op2);
    const serial21 =
      op1 === op2 ? serial12 : await serialOutcome(`il ${tag} b`, op2, op1);

    for (let trial = 0; trial < TRIALS; trial++) {
      const queue = `il ${tag} t${trial}`;
      const fx = await buildFixture(queue);
      const c1 = await env.pool.connect();
      const c2 = await env.pool.connect();
      let e1: string | null = null;
      let e2: string | null = null;
      try {
        // Two separate backends, fired together: interleaving decided by
        // Postgres lock scheduling; repeated trials diversify it.
        [e1, e2] = await Promise.all([OPS[op1]!(c1, fx), OPS[op2]!(c2, fx)]);
      } finally {
        c1.release();
        c2.release();
      }
      const outcome: Outcome = {
        state: await canonicalState(fx),
        errors: [
          ...(e1 ? [`${op1}:${e1}`] : []),
          ...(e2 ? [`${op2}:${e2}`] : []),
        ].sort(),
      };

      const matches =
        (outcome.state === serial12.state &&
          JSON.stringify(outcome.errors) === JSON.stringify(serial12.errors)) ||
        (outcome.state === serial21.state &&
          JSON.stringify(outcome.errors) === JSON.stringify(serial21.errors));
      assert.ok(
        matches,
        `concurrent ${op1} || ${op2} (trial ${trial}) produced a state ` +
          `matching NEITHER serial order.\n` +
          `concurrent: ${outcome.state} errors=${outcome.errors}\n` +
          `serial12:   ${serial12.state} errors=${serial12.errors}\n` +
          `serial21:   ${serial21.state} errors=${serial21.errors}`,
      );

      const { rows: bad } = await env.pool.query(
        "select violation from otra.check_invariants($1)",
        [queue],
      );
      assert.deepEqual(
        bad.map((r: { violation: string }) => r.violation),
        [],
        `${op1} || ${op2} trial ${trial}`,
      );
    }
  }
});
