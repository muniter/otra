import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import fc from "fast-check";
import pg from "pg";

import type { Ctx, DurableHandle, ExecutionRef, Op } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// Multi-worker chaos under the invariant oracle.
//
// tests/fuzz.test.ts fuzzes ONE worker's replay against injected step
// failures. This file adds the three things a single-worker fuzzer cannot
// see, all at once, on a batch of concurrently-running roots:
//
//   - THREE workers claiming from the same queue on separate pool
//     connections (real `for update skip locked` contention, real
//     cross-root tree walks running against each other -- a deadlock, 40P01,
//     would prove the global (root_id, id) lock order broken),
//   - a chaos actor that CANCELS and KILLS roots mid-flight while those
//     workers are driving them,
//   - FORCE-EXPIRED claims: `claim_expires_at` yanked into the past while a
//     worker is mid-drive, which is what a crashed worker looks like from
//     Postgres's side. The expiry sweep inside claim() must recover the
//     execution, and the ownership guards (`_assert_owner`, OT001) must keep
//     the still-running zombie from writing into the history that was taken
//     away from it.
//
// The verdicts are the same shape as fuzz.test.ts -- reference-equal
// results, exactly one journal row per step, exact-once spawns, a silent
// `otra.check_invariants` -- with two deliberate differences:
//
//   1. Executed-step COUNTS are not asserted. A force-expired claim can lose
//      the race between running a step function and journaling it, so the
//      step legitimately runs again on the replay: `ctx.run` side effects are
//      at-least-once, and that is the documented contract. What must still
//      hold is at-most-once PAST the journal, i.e. exactly one `kind='run'`
//      row per label.
//   2. Roots the chaos actor kills are expected to be `cancelled`, and so are
//      roots whose cancel was actually DELIVERED -- the engine owns the
//      terminal state after delivery (AGENTS.md invariant 8). A cancel that
//      arrived with no suspension point left to land on is a race the engine
//      is allowed to lose; see the (c, cancel) verdict below.
//
// The oracle runs between every round, not only at the end: a chaos test
// manufactures TRANSIENT corruption, and a quiescence-only check sees none of
// it.
//
// Programs are DATA (a JSON op tree interpreted by one generic generator
// task), exactly as in fuzz.test.ts, so every replay walks the same ops and
// fast-check can shrink a counterexample. Identifiers are assigned by a PURE
// post-generation walk; a side-effecting counter inside `.map` would break
// fast-check's determinism contract.
//
// OTRA_CHAOS_RUNS overrides the case count (default 8 -- each case drives a
// whole batch of roots through many rounds, so cases are expensive).
// OTRA_FC_SEED reproduces a reported counterexample. OTRA_CHAOS_DEBUG=1
// prints what each case actually exercised -- chaos that quietly stops
// happening is the failure mode of a test like this.
const NUM_RUNS = Number(process.env.OTRA_CHAOS_RUNS ?? 8);
const FC_SEED =
  process.env.OTRA_FC_SEED === undefined
    ? undefined
    : Number(process.env.OTRA_FC_SEED);

/** Rounds of (chaos + 3 concurrent drains) allowed before we call it wedged. */
const MAX_ROUNDS = 80;

/** The chaos actor only ever acts inside this many opening rounds. */
const CHAOS_WINDOW = 4;

type ChaosOp =
  | { kind: "run"; label: string; value: number }
  | { kind: "sleep"; seconds: number }
  | { kind: "event"; name: string }
  | { kind: "spawn"; label: string; program: ChaosOp[] };

/** What the chaos actor does to one root, and when. */
type Fate = "leave" | "cancel" | "kill";

interface RootPlan {
  program: ChaosOp[];
  fate: Fate;
  /** Round at which the fate is applied (before that round's drains). */
  fateRound: number;
}

interface ChaosCase {
  roots: RootPlan[];
  /** step label -> executions of that step that throw before succeeding. */
  failures: Record<string, number>;
  /** event name -> payload value. */
  eventPayloads: Record<string, number>;
  /** event name -> round whose start emits it. */
  eventRound: Record<string, number>;
  /** rounds during which live claims get force-expired (once or twice). */
  expireRounds: number[];
  /** seconds of fake time to burn between rounds. */
  advanceSeconds: number;
}

// --- pure helpers over the case -------------------------------------------

/** The chaos-free, fault-free expected result of one program. */
function reference(program: ChaosOp[], c: ChaosCase): number {
  let sum = 0;
  for (const op of program) {
    if (op.kind === "run") sum += op.value;
    else if (op.kind === "event") sum += c.eventPayloads[op.name] ?? 0;
    else if (op.kind === "spawn") sum += reference(op.program, c);
  }
  return sum;
}

function labelsOf(program: ChaosOp[]): string[] {
  const out: string[] = [];
  for (const op of program) {
    if (op.kind === "run") out.push(op.label);
    else if (op.kind === "spawn") out.push(...labelsOf(op.program));
  }
  return out;
}

function eventsOf(program: ChaosOp[]): string[] {
  const out: string[] = [];
  for (const op of program) {
    if (op.kind === "event") out.push(op.name);
    else if (op.kind === "spawn") out.push(...eventsOf(op.program));
  }
  return out;
}

function countSpawns(program: ChaosOp[]): number {
  let n = 0;
  for (const op of program) if (op.kind === "spawn") n += 1;
  return n;
}

/** Direct (non-child) injected failures: a program's own attempt budget. */
function ownFailures(
  program: ChaosOp[],
  failures: Record<string, number>,
): number {
  let n = 0;
  for (const op of program) {
    if (op.kind === "run") n += failures[op.label] ?? 0;
  }
  return n;
}

/**
 * Canonical labels/names assigned by a PURE DFS walk over the whole batch
 * after generation, so identifiers are unique across every root of a case
 * (the failure map and the execution ledger are keyed by label).
 */
function assignNames(programs: ChaosOp[][]): ChaosOp[][] {
  let next = 0;
  const walk = (ops: ChaosOp[]): ChaosOp[] =>
    ops.map((op) => {
      const i = next++;
      if (op.kind === "run") return { ...op, label: `l${i}` };
      if (op.kind === "event") return { ...op, name: `e${i}` };
      if (op.kind === "spawn")
        return { ...op, label: `s${i}`, program: walk(op.program) };
      return op;
    });
  return programs.map(walk);
}

// --- generators -----------------------------------------------------------

const arbProgram = (depth: number): fc.Arbitrary<ChaosOp[]> => {
  const run: fc.Arbitrary<ChaosOp> = fc
    .integer({ min: -1000, max: 1000 })
    .map((value) => ({ kind: "run", label: "?", value }) as ChaosOp);
  // Sleeps stay under the per-round clock advance, so no round can be a
  // no-op purely because a timer was not yet due.
  const sleep: fc.Arbitrary<ChaosOp> = fc
    .integer({ min: 1, max: 45 })
    .map((seconds) => ({ kind: "sleep", seconds }) as ChaosOp);
  const event: fc.Arbitrary<ChaosOp> = fc.constant({
    kind: "event",
    name: "?",
  } as ChaosOp);
  const leaves = [
    { arbitrary: run, weight: 4 },
    { arbitrary: sleep, weight: 2 },
    { arbitrary: event, weight: 2 },
  ];
  if (depth <= 0) {
    return fc.array(fc.oneof(...leaves), { minLength: 1, maxLength: 4 });
  }
  const spawn: fc.Arbitrary<ChaosOp> = arbProgram(depth - 1).map(
    (program) => ({ kind: "spawn", label: "?", program }) as ChaosOp,
  );
  return fc.array(fc.oneof(...leaves, { arbitrary: spawn, weight: 3 }), {
    minLength: 1,
    maxLength: 5,
  });
};

/** leave 60% / cancel 25% / kill 15%. */
const arbFate: fc.Arbitrary<Fate> = fc.oneof(
  { arbitrary: fc.constant<Fate>("leave"), weight: 60 },
  { arbitrary: fc.constant<Fate>("cancel"), weight: 25 },
  { arbitrary: fc.constant<Fate>("kill"), weight: 15 },
);

const arbRound = fc.integer({ min: 0, max: CHAOS_WINDOW - 1 });

const arbCase: fc.Arbitrary<ChaosCase> = fc
  // A BATCH of roots: three workers with nothing to contend over would only
  // re-prove the single-worker fuzzer.
  .array(arbProgram(1), { minLength: 3, maxLength: 6 })
  .map(assignNames)
  .chain((programs) => {
    const labels = programs.flatMap(labelsOf);
    const events = programs.flatMap(eventsOf);
    return fc
      .record({
        failureCounts: fc.array(fc.integer({ min: 0, max: 2 }), {
          minLength: labels.length,
          maxLength: labels.length,
        }),
        payloads: fc.array(fc.integer({ min: -1000, max: 1000 }), {
          minLength: events.length,
          maxLength: events.length,
        }),
        eventRounds: fc.array(arbRound, {
          minLength: events.length,
          maxLength: events.length,
        }),
        fates: fc.array(arbFate, {
          minLength: programs.length,
          maxLength: programs.length,
        }),
        fateRounds: fc.array(arbRound, {
          minLength: programs.length,
          maxLength: programs.length,
        }),
        expireRounds: fc.array(arbRound, { minLength: 1, maxLength: 2 }),
        advanceSeconds: fc.integer({ min: 60, max: 120 }),
      })
      .map((extra) => {
        const failures: Record<string, number> = {};
        labels.forEach((label, i) => {
          if (extra.failureCounts[i]! > 0) {
            failures[label] = extra.failureCounts[i]!;
          }
        });
        const eventPayloads: Record<string, number> = {};
        const eventRound: Record<string, number> = {};
        events.forEach((name, i) => {
          eventPayloads[name] = extra.payloads[i]!;
          eventRound[name] = extra.eventRounds[i]!;
        });
        return {
          roots: programs.map((program, i) => ({
            program,
            fate: extra.fates[i]!,
            fateRound: extra.fateRounds[i]!,
          })),
          failures,
          eventPayloads,
          eventRound,
          expireRounds: extra.expireRounds,
          advanceSeconds: extra.advanceSeconds,
        };
      });
  });

// --- the interpreter task -------------------------------------------------

/**
 * Attempts of each step function, keyed `${caseId}:${label}`. Module-level on
 * purpose: replays build fresh generators, and the injected-failure schedule
 * is "the first N real executions of this step throw".
 */
const executed = new Map<string, number>();

interface InterpreterParams {
  caseId: string;
  program: ChaosOp[];
  failures: Record<string, number>;
}

const TERMINAL = new Set(["completed", "failed", "cancelled"]);

let env: TestEnv;
/**
 * The chaos actor gets its OWN pool: createTestEnv's pool is capped at four
 * connections and three draining workers want all of them, so sharing it
 * would make the injected crash wait for the very workers it is racing.
 */
let chaosPool: pg.Pool;
/** Live claims actually caught mid-drive, summed over the whole property. */
let forceExpired = 0;
/** Cancels that lost the race to a root with no suspension point left. */
let cancelsRacedToCompletion = 0;

before(async () => {
  env = await createTestEnv();
  chaosPool = new pg.Pool({ connectionString: env.connectionString, max: 2 });

  function* interpretOps(
    params: InterpreterParams,
    ops: ChaosOp[],
    ctx: Ctx,
    handles: DurableHandle<number>[],
  ): Op<number> {
    let sum = 0;
    for (const op of ops) {
      if (op.kind === "run") {
        sum += yield* ctx.run(op.label, () => {
          const key = `${params.caseId}:${op.label}`;
          const n = (executed.get(key) ?? 0) + 1;
          executed.set(key, n);
          if (n <= (params.failures[op.label] ?? 0)) {
            throw new Error(`injected failure ${n} at ${op.label}`);
          }
          return op.value;
        });
      } else if (op.kind === "sleep") {
        yield* ctx.sleep(op.seconds);
      } else if (op.kind === "event") {
        // Events are immutable facts per (queue, name) and every case shares
        // one queue, so names are namespaced per case.
        const payload = yield* ctx.waitForEvent<{ v: number }>(
          `${params.caseId}:${op.name}`,
        );
        sum += payload.v;
      } else {
        handles.push(
          yield* ctx.spawn<InterpreterParams, number>(
            "chaos-interpreter",
            {
              caseId: params.caseId,
              program: op.program,
              failures: params.failures,
            },
            {
              label: op.label,
              maxAttempts: attemptBudget(op.program, params.failures),
              retryStrategy: { kind: "fixed", base_s: 1 },
            },
          ),
        );
      }
    }
    for (const value of yield* ctx.all(handles.splice(0))) sum += value;
    return sum;
  }

  env.app.task("chaos-interpreter", function* (params: InterpreterParams, ctx) {
    return yield* interpretOps(params, params.program, ctx, []);
  });
});

after(async () => {
  await chaosPool?.end();
  await env?.close();
});

// --- attempt budget ------------------------------------------------------

/**
 * fuzz.test.ts uses `own failures + 2`. Chaos needs headroom on top of that:
 * a force-expired claim is applied as a RETRYABLE FAILED ATTEMPT by the
 * sweep in claim_local (claim-expiry-as-failure, absurd's 47e6710), so every
 * injected crash SPENDS an attempt. Each expiry round expires a given
 * execution at most once, so one extra attempt per expiry round is exactly
 * enough -- and no more, so a genuine attempt leak still shows up as a
 * permanently `failed` root instead of being absorbed by a fat budget.
 *
 * Module-level because the interpreter computes its CHILDREN's budgets from
 * inside a worker; fc.assert runs cases one at a time, and each case sets this
 * before it spawns anything.
 */
let expiryHeadroom = 0;
function attemptBudget(
  program: ChaosOp[],
  failures: Record<string, number>,
): number {
  return ownFailures(program, failures) + 2 + expiryHeadroom;
}

// --- the property ---------------------------------------------------------

let caseNo = 0;

const LIVE_CLAIMS_SQL = `
  select root_id, id from otra.x_default
   where status = 'running'
   order by root_id, id`;

/**
 * Yank one live claim's lease into the past: what Postgres sees the instant a
 * worker's process dies mid-step. The expiry sweep inside claim() must
 * recover the execution, and the ownership guards must refuse the zombie that
 * is still running it.
 *
 * Deliberately ONE ROW PER STATEMENT. A bulk `update ... where status =
 * 'running'` -- even ordered by (root_id, id), the engine's one global lock
 * order, with an explicit `for update` -- holds several execution-row locks
 * inside a single transaction while three workers are taking their own, and
 * an early version of this file was caught in a genuine three-process 40P01
 * cycle as a result. A single-row UPDATE holds nothing while it waits, so it
 * cannot be a member of any cycle: the harness must not manufacture the very
 * failure class it exists to detect, so any deadlock left over is the
 * engine's and fails verdict (d).
 */
const EXPIRE_ONE_SQL = `
  update otra.x_default
     set claim_expires_at = otra.now() - interval '1 second'
   where root_id = $1 and id = $2 and status = 'running'`;

test("multi-worker chaos: cancels, kills and crashed workers keep the journal exact", async () => {
  await fc.assert(
    fc.asyncProperty(arbCase, async (c) => {
      const { app } = env;
      const caseId = `c${caseNo++}`;
      expiryHeadroom = c.expireRounds.length;

      // (d) Nothing may escape the driver into the worker's error handler:
      // not a 40P01 deadlock between two tree walks, not an unmapped
      // ownership refusal. Collected here, asserted empty at the end.
      const errors: unknown[] = [];
      const workers = [1, 2, 3].map((n) =>
        app.createWorker({
          workerId: `${caseId}-w${n}`,
          onError: (err) => errors.push(err),
        }),
      );

      const roots: Array<{
        plan: RootPlan;
        ref: ExecutionRef;
        /** True once the cancel/kill was actually issued (root still live). */
        fateApplied: boolean;
      }> = [];
      for (const plan of c.roots) {
        roots.push({
          plan,
          ref: await app.spawn(
            "chaos-interpreter",
            { caseId, program: plan.program, failures: c.failures },
            {
              maxAttempts: attemptBudget(plan.program, c.failures),
              retryStrategy: { kind: "fixed", base_s: 1 },
            },
          ),
          fateApplied: false,
        });
      }

      const snapshots = async () =>
        Promise.all(roots.map((r) => app.getExecution(r.ref)));
      const violations = async (): Promise<string[]> =>
        (
          await chaosPool.query(
            "select violation from otra.check_invariants('default') order by violation",
          )
        ).rows.map((r: { violation: string }) => r.violation);
      const allTerminal = async () =>
        (await snapshots()).every((s) => s !== null && TERMINAL.has(s.status));

      let round = 0;
      for (; round < MAX_ROUNDS; round++) {
        if (await allTerminal()) break;

        // 1. Emit this round's events. Facts are immutable, so an emit for a
        //    root that has since been killed is simply retained and unread.
        for (const [name, at] of Object.entries(c.eventRound)) {
          if (at === round) {
            await app.emitEvent(`${caseId}:${name}`, {
              v: c.eventPayloads[name],
            });
          }
        }

        // 2. Apply this round's fates. No worker is mid-drive here (every
        //    drain of the previous round has returned), so reading the status
        //    and acting on it is not a race: a root that already reached a
        //    terminal state was never actually cancelled, and stays on the
        //    "must complete with the reference result" side of the verdict.
        for (const root of roots) {
          if (root.plan.fate === "leave" || root.plan.fateRound !== round) {
            continue;
          }
          const before = await app.getExecution(root.ref);
          if (before === null || TERMINAL.has(before.status)) continue;
          if (root.plan.fate === "cancel") await app.cancel(root.ref);
          else await app.kill(root.ref);
          root.fateApplied = true;
        }

        // 3. Three workers drain the same queue CONCURRENTLY, each on its own
        //    pool connection: real `for update skip locked` contention.
        //    Force-expiry runs against those live drains, because that is the
        //    only time a claim is live at all -- drain() returns precisely
        //    when nothing is running.
        let draining = true;
        const drains = Promise.all(workers.map((w) => w.drain())).finally(
          () => {
            draining = false;
          },
        );
        const expiring = c.expireRounds.includes(round)
          ? (async () => {
              // Keep sweeping for live claims until one is actually caught
              // (or the drains finish): a single blind pass usually lands in
              // the gap between two claims and tests nothing. Stopping at the
              // first hit keeps the cost at one attempt per execution per
              // round, which is exactly the headroom attemptBudget grants.
              let hit = 0;
              while (hit === 0 && draining) {
                const { rows: live } = await chaosPool.query(LIVE_CLAIMS_SQL);
                for (const victim of live) {
                  hit +=
                    (
                      await chaosPool.query(EXPIRE_ONE_SQL, [
                        victim.root_id,
                        victim.id,
                      ])
                    ).rowCount ?? 0;
                }
                if (hit === 0) {
                  await new Promise((resolve) => setTimeout(resolve, 1));
                }
              }
              return hit;
            })()
          : Promise.resolve(0);
        const [, expired] = await Promise.all([drains, expiring]);
        forceExpired += expired;

        // 4. The oracle, mid-flight. Running it only at quiescence would miss
        //    every TRANSIENT corruption -- a clobbered claim that the next
        //    tick tidies up, a settlement written in the wrong order -- and
        //    transient is exactly what a chaos test manufactures. This point
        //    in the round is a real quiescent point (every drain returned), so
        //    the cross-table rules must already hold.
        const midflight = await violations();
        assert.deepEqual(midflight, [], `round ${round}: mid-flight oracle`);

        // 5. Burn fake time: every sleep (<= 45s) and every 1s retry backoff
        //    comes due, and the 30s claim leases of anything the sweep still
        //    owes us expire.
        await env.advance(c.advanceSeconds);
      }

      const finalSnapshots = await snapshots();
      assert.ok(
        finalSnapshots.every((s) => s !== null && TERMINAL.has(s.status)),
        `not quiescent after ${round} rounds: ${JSON.stringify(finalSnapshots)}`,
      );

      // (a) The oracle stays silent: no stuck-suspended execution, no lost or
      //     premature child settlement, no incoherent claim left behind by a
      //     force-expired worker.
      assert.deepEqual(await violations(), []);

      for (const [i, root] of roots.entries()) {
        const snapshot = finalSnapshots[i]!;
        const where = `root ${i} (${root.plan.fate}@${root.plan.fateRound})`;

        // (c) kill is the hard escape hatch: the SQL flips the row itself and
        //     a worker mid-drive discovers it at its next history write, so
        //     there is no race to lose -- a killed root is `cancelled`, full
        //     stop.
        if (root.fateApplied && root.plan.fate === "kill") {
          assert.equal(
            snapshot.status,
            "cancelled",
            `${where}: ${JSON.stringify(snapshot)}`,
          );
          continue;
        }

        // (c, cancel) `cancel` is a REQUEST, and what binds the terminal
        //     state is DELIVERY (AGENTS.md invariant 8): CancelledError is
        //     thrown at the next yield that is not already settled in the
        //     journal, because a replay may not take a different path through
        //     history it has already recorded. A cancel requested while the
        //     execution is parked on its LAST suspension point therefore has
        //     nowhere left to land once that promise resolves, and the
        //     execution finishes normally -- complete_local refuses only when
        //     a '$cancel' row exists. So: `cancelled`, or `completed` with no
        //     delivery anywhere in the tree and a fully correct result. Not
        //     `failed`: exhausted compensation finalizes cancelled.
        if (root.fateApplied && snapshot.status === "cancelled") continue;
        if (root.fateApplied) {
          assert.equal(
            snapshot.status,
            "completed",
            `${where}: a requested cancel may only race to 'completed': ` +
              JSON.stringify(snapshot),
          );
          const { rows: delivered } = await chaosPool.query(
            `select count(*)::int as n from otra.p_default
              where root_id = $1 and key = '$cancel'`,
            [root.ref.rootId],
          );
          assert.equal(
            delivered[0].n,
            0,
            `${where}: completed with a DELIVERED cancellation in its tree`,
          );
          cancelsRacedToCompletion += 1;
        }

        // (b) Untouched roots -- and roots whose cancel arrived too late --
        //     are unaffected by everything happening around them: crashed
        //     workers, other roots being killed, three workers racing for
        //     their steps.
        assert.equal(
          snapshot.status,
          "completed",
          `${where}: ${JSON.stringify(snapshot)}`,
        );
        assert.equal(
          snapshot.result,
          reference(root.plan.program, c),
          `${where}: result differs from the chaos-free reference`,
        );

        // Journal-exact accounting, scoped by root_id: a force-expired claim
        // may legitimately RE-EXECUTE a step function (at-least-once), but
        // the write-once journal must still hold exactly one row per step.
        const { rows: journal } = await chaosPool.query(
          `select label, count(*)::int as n from otra.p_default
            where root_id = $1 and kind = 'run' group by label`,
          [root.ref.rootId],
        );
        const counts = new Map<string, number>(
          journal.map((r: { label: string; n: number }) => [r.label, r.n]),
        );
        const labels = labelsOf(root.plan.program);
        for (const label of labels) {
          assert.equal(counts.get(label), 1, `${where}: journal rows ${label}`);
        }
        assert.equal(counts.size, labels.length, `${where}: extra run rows`);

        // Exact-once spawn under replay, tree-wide.
        const { rows: kids } = await chaosPool.query(
          `select count(*)::int as n from otra.x_default
            where root_id = $1 and id <> $2`,
          [root.ref.rootId, root.ref.executionId],
        );
        assert.equal(
          kids[0].n,
          countSpawns(root.plan.program),
          `${where}: child execution count`,
        );
      }

      // (d) A deadlock (40P01) or any other error escaping the driver fails
      //     the property.  This once carved out a known zombie-redrive spin
      //     (a swept claim redriven until the worker's replay cap threw);
      //     suspend_local now raises OT001 for a stolen claim, so every
      //     reported error is real again.
      const reported = errors.map((err) => String(err));
      // OTRA_CHAOS_DEBUG prints what each case actually exercised: chaos that
      // silently stops happening is the failure mode of a test like this.
      if (process.env.OTRA_CHAOS_DEBUG) {
        console.error(
          `[${caseId}] roots=${roots.length} rounds=${round} expired=${forceExpired} ` +
            `fates=${roots.map((r) => (r.fateApplied ? r.plan.fate[0] : "-")).join("")} ` +
            `ops=${roots.reduce((n, r) => n + r.plan.program.length, 0)} ` +
            `spawns=${roots.reduce((n, r) => n + countSpawns(r.plan.program), 0)} ` +
            `expireRounds=${c.expireRounds} lateCancels=${cancelsRacedToCompletion}`,
        );
      }
      assert.deepEqual(reported, [], "workers reported errors");
    }),
    { numRuns: NUM_RUNS, seed: FC_SEED },
  );

  // The chaos has to have actually happened: if no injected expiry ever
  // caught a live claim, this file silently stopped testing crash recovery.
  assert.ok(
    forceExpired > 0,
    "no live claim was ever force-expired: the crashed-worker path went untested",
  );
});
