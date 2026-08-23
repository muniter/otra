import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import fc from "fast-check";

import type { Ctx, DurableHandle, Op } from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// Crash/replay fuzzing: the property that DEFINES a durable execution
// engine. For any task program and any schedule of injected step failures,
//
//   - the final result equals the fault-free reference,
//   - every step lands in the journal exactly once,
//   - every step FUNCTION executes exactly (injected failures + 1) times
//     (the memoization proof: replays never re-run recorded work),
//   - children are spawned exactly once each (idempotent spawn under replay),
//   - and otra.check_invariants stays silent.
//
// The trick that makes generated programs replay-safe: a program is DATA
// (a JSON op tree carried in params) interpreted by ONE generic generator
// task, so every replay deterministically walks the same ops, and children
// receive their sub-programs the same way. fast-check shrinks a failure to
// a minimal program + fault schedule.
//
// OTRA_FUZZ_RUNS overrides the case count for deep local runs.
const NUM_RUNS = Number(process.env.OTRA_FUZZ_RUNS ?? 15);

type FuzzOp =
  | { kind: "run"; label: string; value: number }
  | { kind: "sleep"; seconds: number }
  | { kind: "event"; name: string }
  | { kind: "spawn"; label: string; program: FuzzOp[] }
  | { kind: "awaitAll" }
  | { kind: "shield"; body: FuzzOp[] };

interface FuzzCase {
  program: FuzzOp[];
  /** step label -> executions of that step that throw before succeeding. */
  failures: Record<string, number>;
  /** event name -> payload value. */
  eventPayloads: Record<string, number>;
  /** event name -> emitted before the first tick, or after the first drain. */
  eventTiming: Record<string, "before" | "after">;
}

// --- pure helpers over the case ------------------------------------------

/** The fault-free expected result, computed purely from the case. */
function reference(program: FuzzOp[], c: FuzzCase): number {
  let sum = 0;
  for (const op of program) {
    if (op.kind === "run") sum += op.value;
    else if (op.kind === "event") sum += c.eventPayloads[op.name] ?? 0;
    else if (op.kind === "spawn") sum += reference(op.program, c);
    else if (op.kind === "shield") sum += reference(op.body, c);
  }
  return sum;
}

function labelsOf(program: FuzzOp[]): string[] {
  const out: string[] = [];
  for (const op of program) {
    if (op.kind === "run") out.push(op.label);
    else if (op.kind === "spawn") out.push(...labelsOf(op.program));
    else if (op.kind === "shield") out.push(...labelsOf(op.body));
  }
  return out;
}

function eventsOf(program: FuzzOp[]): string[] {
  const out: string[] = [];
  for (const op of program) {
    if (op.kind === "event") out.push(op.name);
    else if (op.kind === "spawn") out.push(...eventsOf(op.program));
    else if (op.kind === "shield") out.push(...eventsOf(op.body));
  }
  return out;
}

function countSpawns(program: FuzzOp[]): number {
  let n = 0;
  for (const op of program) {
    if (op.kind === "spawn") n += 1 + countSpawns(op.program);
    else if (op.kind === "shield") n += countSpawns(op.body);
  }
  return n;
}

/** Direct (non-child) injected failures: a program's own attempt budget. */
function ownFailures(
  program: FuzzOp[],
  failures: Record<string, number>,
): number {
  let n = 0;
  for (const op of program) {
    if (op.kind === "run") n += failures[op.label] ?? 0;
    else if (op.kind === "shield") n += ownFailures(op.body, failures);
  }
  return n;
}

/**
 * Canonical labels/names assigned by a PURE DFS walk after generation.
 * Generating identifiers with a side-effecting counter inside .map would
 * violate fast-check's determinism contract and break shrinking.
 */
function assignNames(program: FuzzOp[]): FuzzOp[] {
  let next = 0;
  const walk = (ops: FuzzOp[]): FuzzOp[] =>
    ops.map((op) => {
      const i = next++;
      if (op.kind === "run") return { ...op, label: `l${i}` };
      if (op.kind === "event") return { ...op, name: `e${i}` };
      if (op.kind === "spawn")
        return { ...op, label: `s${i}`, program: walk(op.program) };
      if (op.kind === "shield") return { ...op, body: walk(op.body) };
      return op;
    });
  return walk(program);
}

// --- generators -----------------------------------------------------------

const arbProgram = (depth: number): fc.Arbitrary<FuzzOp[]> => {
  const run: fc.Arbitrary<FuzzOp> = fc
    .integer({ min: -1000, max: 1000 })
    .map((value) => ({ kind: "run", label: "?", value }) as FuzzOp);
  const sleep: fc.Arbitrary<FuzzOp> = fc
    .integer({ min: 1, max: 60 })
    .map((seconds) => ({ kind: "sleep", seconds }) as FuzzOp);
  const event: fc.Arbitrary<FuzzOp> = fc.constant({
    kind: "event",
    name: "?",
  } as FuzzOp);
  const awaitAll: fc.Arbitrary<FuzzOp> = fc.constant({
    kind: "awaitAll",
  } as FuzzOp);
  const shield: fc.Arbitrary<FuzzOp> = fc
    .array(fc.oneof(run, sleep), { minLength: 1, maxLength: 3 })
    .map((body) => ({ kind: "shield", body }) as FuzzOp);
  const leaves = [
    { arbitrary: run, weight: 4 },
    { arbitrary: sleep, weight: 2 },
    { arbitrary: event, weight: 2 },
    { arbitrary: awaitAll, weight: 1 },
    { arbitrary: shield, weight: 1 },
  ];
  if (depth <= 0) {
    return fc.array(fc.oneof(...leaves), { minLength: 1, maxLength: 6 });
  }
  const spawn: fc.Arbitrary<FuzzOp> = arbProgram(depth - 1).map(
    (program) => ({ kind: "spawn", label: "?", program }) as FuzzOp,
  );
  return fc.array(fc.oneof(...leaves, { arbitrary: spawn, weight: 2 }), {
    minLength: 1,
    maxLength: 8,
  });
};

const arbCase: fc.Arbitrary<FuzzCase> = arbProgram(2)
  .map(assignNames)
  .chain((program) => {
    const labels = labelsOf(program);
    const events = eventsOf(program);
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
        timings: fc.array(
          fc.constantFrom<"before" | "after">("before", "after"),
          {
            minLength: events.length,
            maxLength: events.length,
          },
        ),
      })
      .map(({ failureCounts, payloads, timings }) => {
        const failures: Record<string, number> = {};
        labels.forEach((label, i) => {
          if (failureCounts[i]! > 0) failures[label] = failureCounts[i]!;
        });
        const eventPayloads: Record<string, number> = {};
        const eventTiming: Record<string, "before" | "after"> = {};
        events.forEach((name, i) => {
          eventPayloads[name] = payloads[i]!;
          eventTiming[name] = timings[i]!;
        });
        return { program, failures, eventPayloads, eventTiming };
      });
  });

// --- the interpreter task ---------------------------------------------------

// Execution-count ledger keyed `${caseId}:${label}`: module-level on
// purpose -- replays build fresh generators, and the point is to observe
// how often each step function REALLY ran.
const executed = new Map<string, number>();

interface InterpreterParams {
  caseId: string;
  program: FuzzOp[];
  failures: Record<string, number>;
}

let env: TestEnv;
before(async () => {
  env = await createTestEnv();

  function* interpretOps(
    params: InterpreterParams,
    ops: FuzzOp[],
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
        // Event names are namespaced per case: facts are immutable per
        // (queue, name), and cases share one queue.
        const payload = yield* ctx.waitForEvent<{ v: number }>(
          `${params.caseId}:${op.name}`,
        );
        sum += payload.v;
      } else if (op.kind === "spawn") {
        handles.push(
          yield* ctx.spawn<InterpreterParams, number>(
            "fuzz-interpreter",
            {
              caseId: params.caseId,
              program: op.program,
              failures: params.failures,
            },
            {
              label: op.label,
              maxAttempts: ownFailures(op.program, params.failures) + 2,
              retryStrategy: { kind: "fixed", base_s: 1 },
            },
          ),
        );
      } else if (op.kind === "awaitAll") {
        for (const value of yield* ctx.all(handles.splice(0))) sum += value;
      } else {
        // Shield bodies contain only run/sleep, so a fresh handle list is
        // exact; the body's sum comes back through uninterruptible's return.
        sum += yield* ctx.uninterruptible(() =>
          interpretOps(params, op.body, ctx, []),
        );
      }
    }
    // Await children never explicitly collected: every child's contribution
    // counts exactly once and completion is well-defined.
    for (const value of yield* ctx.all(handles.splice(0))) sum += value;
    return sum;
  }

  env.app.task("fuzz-interpreter", function* (params: InterpreterParams, ctx) {
    return yield* interpretOps(params, params.program, ctx, []);
  });
});
after(async () => {
  await env?.close();
});

// --- the property ------------------------------------------------------------

let caseNo = 0;

test("replay under injected faults: exact-once journal, reference result, clean invariants", async () => {
  await fc.assert(
    fc.asyncProperty(arbCase, async (c) => {
      const { app, pool } = env;
      const caseId = `case${caseNo++}`;

      for (const [name, timing] of Object.entries(c.eventTiming)) {
        if (timing === "before") {
          await app.emitEvent(`${caseId}:${name}`, {
            v: c.eventPayloads[name],
          });
        }
      }

      const execution = await app.spawn(
        "fuzz-interpreter",
        { caseId, program: c.program, failures: c.failures },
        {
          maxAttempts: ownFailures(c.program, c.failures) + 2,
          retryStrategy: { kind: "fixed", base_s: 1 },
        },
      );

      const worker = app.createWorker({ workerId: "wf" });
      await worker.drain();
      // Late events land after the first drain: already-parked waiters get
      // woken, not-yet-reached waiters find the retained fact.
      for (const [name, timing] of Object.entries(c.eventTiming)) {
        if (timing === "after") {
          await app.emitEvent(`${caseId}:${name}`, {
            v: c.eventPayloads[name],
          });
        }
      }
      // Drive to quiescence, advancing the frozen clock past sleeps (<=60s)
      // and 1s retry backoffs each round. Bounded: a liveness bug must fail
      // loudly, not hang the suite.
      let rounds = 0;
      while ((await app.getExecution(execution))!.status !== "completed") {
        if (++rounds > 80) {
          assert.fail(
            `not completed after ${rounds} rounds: ` +
              JSON.stringify(await app.getExecution(execution)),
          );
        }
        await env.advance(120);
        await worker.drain();
      }

      // 1. The result equals the fault-free reference.
      assert.equal(
        (await app.getExecution(execution))!.result,
        reference(c.program, c),
        "result differs from the fault-free reference",
      );

      // 2 + 3. Exactly one journal row per step, and each step function ran
      // exactly (injected failures + 1) times: at-least-once from the
      // engine, at-most-once past the journal. Scoped to this case's tree.
      const { rows: journal } = await pool.query(
        `select label, count(*)::int as n from otra.p_default
          where root_id = $1 and kind = 'run' group by label`,
        [execution.rootId],
      );
      const journalCounts = new Map<string, number>(
        journal.map((r: { label: string; n: number }) => [r.label, r.n]),
      );
      for (const label of labelsOf(c.program)) {
        assert.equal(journalCounts.get(label), 1, `journal rows for ${label}`);
        assert.equal(
          executed.get(`${caseId}:${label}`),
          (c.failures[label] ?? 0) + 1,
          `real executions of step ${label}`,
        );
      }

      // 4. Children spawned exactly once each, tree-wide.
      const { rows: kids } = await pool.query(
        `select count(*)::int as n from otra.x_default
          where root_id = $1 and id <> $2`,
        [execution.rootId, execution.executionId],
      );
      assert.equal(kids[0].n, countSpawns(c.program), "child execution count");

      // 5. The oracle stays silent.
      const { rows: bad } = await pool.query(
        "select violation from otra.check_invariants('default')",
      );
      assert.deepEqual(
        bad.map((r: { violation: string }) => r.violation),
        [],
      );
    }),
    { numRuns: NUM_RUNS },
  );
});
