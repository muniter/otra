/**
 * Property-based tests (fast-check).
 *
 * The rest of the suite is example-based: it pins the behaviours we reasoned
 * about. This file attacks the surfaces where the interesting inputs are the
 * ones nobody thought to write down -- parsers, encoders, numeric bounds and
 * identifier arithmetic -- by generating thousands of them and asserting an
 * *invariant* instead of an expected value. When one fails, fast-check
 * shrinks the input to something minimal, which is the whole point: the
 * report is "9-nines-then-`s`", not "some 400-character string".
 *
 * Scope discipline: every property here is either pure or a *value* property
 * of one SQL function. Engine-level cross-table invariants live in
 * `invariants.test.ts`, and generated-program crash/replay fuzzing lives in
 * `fuzz.test.ts`.
 *
 * Run counts: pure properties default to 200 runs, database-backed ones to
 * 30 (queue provisioning to 15 -- it creates ~43 relations per run). The env
 * knob `OTRA_PROPERTY_RUNS` multiplies all of them, so
 * `OTRA_PROPERTY_RUNS=5` is a deeper nightly pass over the same file and
 * `OTRA_PROPERTY_RUNS=0.2` a fast smoke pass. A plain run stays well under
 * 30 seconds.
 *
 * One TestEnv is shared by every property in the file: a fast-check "run" is
 * an iteration, not a test, so per-run database provisioning would dominate
 * the runtime. Properties that need isolation take a unique queue or a fresh
 * execution per run instead, and the ones that move the fake clock put it
 * back.
 */
import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import fc from "fast-check";

import {
  OtraError,
  parseDuration,
  parsePromiseToken,
  promiseToken,
  type JsonValue,
  type TaskHandle,
  type Worker,
} from "../src/index.ts";
import { createTestEnv, type TestEnv } from "./helpers.ts";

// ---------------------------------------------------------------------------
// run counts
// ---------------------------------------------------------------------------

const RUN_MULTIPLIER = (() => {
  const raw = process.env.OTRA_PROPERTY_RUNS;
  if (raw === undefined || raw === "") return 1;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new TypeError(`invalid OTRA_PROPERTY_RUNS: ${JSON.stringify(raw)}`);
  }
  return parsed;
})();

const runs = (base: number) => Math.max(1, Math.round(base * RUN_MULTIPLIER));

/** Pure, in-process properties: cheap, so run them hard. */
const PURE_RUNS = runs(200);
/** One or two queries per run. */
const DB_RUNS = runs(30);
/** A whole partitioned queue per run (~43 relations created and dropped). */
const PROVISION_RUNS = runs(15);
/** Spawn + two worker ticks + kills per run. */
const DEFER_RUNS = runs(10);

/** helpers.ts freezes the database clock here; clock movers restore it. */
const FROZEN_NOW = "2026-01-01T00:00:00Z";

// ---------------------------------------------------------------------------
// shared environment
// ---------------------------------------------------------------------------

let env: TestEnv;

/** Property 4's channel between the generated value and the durable task. */
interface JsonRoundtripState {
  value: JsonValue;
  /** Generator entries, i.e. real (non-replayed) attempts. */
  entries: number;
  /** What `ctx.run` handed back on the FIRST, executing attempt. */
  firstSeen: unknown;
  /** What `ctx.run` handed back on the replay, out of the journal. */
  replaySeen: unknown;
}

let jsonState: JsonRoundtripState | null = null;
let jsonTask: TaskHandle<null, JsonValue>;
let jsonWorker: Worker;
let deferWorker: Worker;

before(async () => {
  env = await createTestEnv();

  // Registered once: fast-check runs are iterations, and app.task refuses a
  // duplicate name. The generated value reaches the task through jsonState.
  jsonTask = env.app.task(
    "prop-json-roundtrip",
    function* (_params: null, ctx) {
      const state = jsonState!;
      state.entries += 1;
      const got = yield* ctx.run("value", () => state.value);
      if (ctx.attempt === 0) {
        // Fail AFTER the checkpoint so attempt 2 must read the value back
        // out of the journal rather than re-running the step.
        state.firstSeen = got;
        throw new Error("forced retry after checkpoint");
      }
      state.replaySeen = got;
      return got;
    },
  );

  jsonWorker = env.app.createWorker({ workerId: "prop-json" });
  deferWorker = env.app.createWorker({
    workerId: "prop-defer",
    // One tick must claim every execution the run spawned, or the
    // stability check would compare different executions.
    batchSize: 64,
    concurrency: 64,
  });
});

after(async () => {
  await env?.close();
});

// ---------------------------------------------------------------------------
// 1. parseDuration (src/context.ts)
// ---------------------------------------------------------------------------

/** A local copy of the grammar, used only to *generate* non-matching input. */
const DURATION_GRAMMAR = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/;

const UNIT_SECONDS = {
  ms: 0.001,
  s: 1,
  m: 60,
  h: 3600,
  d: 86400,
} as const;
type DurationUnit = keyof typeof UNIT_SECONDS;

const DIGITS = "0123456789".split("");

/**
 * A decimal literal of at most six significant digits, with the point in
 * every legal position (including "no point at all").
 */
const decimalLiteral = fc
  .tuple(
    fc.array(fc.constantFrom(...DIGITS), { minLength: 1, maxLength: 6 }),
    fc.nat({ max: 5 }),
  )
  .map(([digits, cut]) => {
    const text = digits.join("");
    const at = Math.min(cut, text.length - 1);
    return at === 0 ? text : `${text.slice(0, at)}.${text.slice(at)}`;
  });

const whitespace = fc.constantFrom("", " ", "  ", "\t", "\n", " \n\t ");

// Invariant: a well-formed literal means exactly "n of that unit", in seconds.
test("parseDuration turns <n><unit> into n x the unit's factor", () => {
  fc.assert(
    fc.property(
      decimalLiteral,
      fc.constantFrom<DurationUnit>("ms", "s", "m", "h", "d"),
      whitespace,
      whitespace,
      (literal, unit, lead, trail) => {
        const n = Number(literal);
        const actual = parseDuration(`${lead}${literal}${unit}${trail}`);
        const expected = n * UNIT_SECONDS[unit];
        // Computed independently of the implementation (which divides by
        // 1000 for ms rather than multiplying by 0.001), so compare with a
        // relative tolerance -- still ~4 orders of magnitude tighter than
        // any wrong unit factor could be.
        const tolerance = 1e-12 * Math.max(1, Math.abs(expected));
        assert.ok(
          Math.abs(actual - expected) <= tolerance,
          `${literal}${unit} -> ${actual}, expected ~${expected}`,
        );
        assert.ok(
          Number.isFinite(actual) && actual >= 0,
          `${literal}${unit} -> ${actual}, which is not a finite delay`,
        );
      },
    ),
    { numRuns: PURE_RUNS },
  );
});

// Invariant: a number of seconds passes through untouched.
test("parseDuration is the identity on finite non-negative numbers", () => {
  fc.assert(
    fc.property(
      fc.oneof(
        fc.double({ min: 0, noNaN: true, noDefaultInfinity: true }),
        fc.nat().map((n) => n),
        fc.constantFrom(
          0,
          -0,
          Number.MIN_VALUE,
          Number.MAX_VALUE,
          Number.EPSILON,
        ),
      ),
      (seconds) => {
        assert.strictEqual(parseDuration(seconds), seconds);
      },
    ),
    { numRuns: PURE_RUNS },
  );
});

// Invariant: a number that cannot describe a delay is refused, not coerced.
test("parseDuration rejects negative, NaN and infinite numbers", () => {
  fc.assert(
    fc.property(
      fc.oneof(
        fc.double({
          max: -Number.MIN_VALUE,
          noNaN: true,
          noDefaultInfinity: true,
        }),
        fc.constantFrom(
          Number.NaN,
          Number.POSITIVE_INFINITY,
          Number.NEGATIVE_INFINITY,
          -1,
        ),
      ),
      (seconds) => {
        assert.throws(() => parseDuration(seconds), TypeError);
      },
    ),
    { numRuns: PURE_RUNS },
  );
});

/** Near-misses worth generating on purpose, alongside random strings. */
const DURATION_NEAR_MISSES = [
  "",
  " ",
  "s",
  "ms",
  "5",
  "5x",
  "5 s",
  "-5s",
  "+5s",
  "5.s",
  ".5s",
  "5e3s",
  "1_000s",
  "5S",
  "5sec",
  "5m30s",
  "0x10s",
  "Infinitys",
  "NaNs",
  "5.5.5s",
  "٥s", // ARABIC-INDIC DIGIT FIVE: a digit, but not \d
  "5 s",
  "5d5d",
];

const notADuration = fc
  .oneof(
    { arbitrary: fc.string(), weight: 3 },
    { arbitrary: fc.string({ unit: "grapheme" }), weight: 2 },
    { arbitrary: fc.constantFrom(...DURATION_NEAR_MISSES), weight: 3 },
  )
  // Random strings essentially never hit the grammar, but the near-misses
  // are chosen to sit right next to it -- filter so the property is honest.
  .filter((text) => !DURATION_GRAMMAR.test(text.trim()));

// Invariant: anything outside the grammar is a TypeError, never a silent value.
test("parseDuration throws TypeError on strings outside its grammar", () => {
  fc.assert(
    fc.property(notADuration, (text) => {
      assert.throws(() => parseDuration(text), TypeError);
    }),
    { numRuns: PURE_RUNS },
  );
});

/**
 * FINDING, now FIXED (the property found it; parseDuration validates the
 * parsed result with Number.isFinite).
 *
 * Invariant: parseDuration is total and coherent -- for any
 * input it either throws TypeError or returns a finite, non-negative number
 * of seconds. It does not. A long enough run of digits overflows Number()
 * to Infinity and sails straight through the grammar:
 *
 *   fast-check shrunk counterexample: "9".repeat(309) + "s"  ->  Infinity
 *   (and with unit "d" it takes only 305 nines, since the multiply overflows
 *   before the parse does: "9".repeat(305) + "d" -> Infinity)
 *
 * That is exactly the input the NUMBER branch refuses:
 * `parseDuration(Infinity)` throws `invalid duration: Infinity`. So the two
 * branches disagree about the same value, and an Infinity delay flows on to
 * `create_sleep_local`/`make_interval` instead of being rejected at the edge.
 *
 * Fix: apply the number branch's own guard to the parsed result, e.g.
 * `if (!Number.isFinite(value)) throw new TypeError(...)` after `Number(...)`
 * (or, equivalently, bound the digit count in the grammar). Once that lands,
 * drop the `skip`.
 */
test("parseDuration is total: it throws or returns a finite delay", () => {
  fc.assert(
    fc.property(
      fc.oneof(
        notADuration,
        // The overflow region: long digit runs that DO match the grammar.
        fc
          .tuple(
            fc.array(fc.constantFrom(...DIGITS), {
              minLength: 1,
              maxLength: 400,
            }),
            fc.constantFrom<DurationUnit>("ms", "s", "m", "h", "d"),
          )
          .map(([digits, unit]) => `${digits.join("")}${unit}`),
      ),
      (text) => {
        let result: number;
        try {
          result = parseDuration(text);
        } catch (error) {
          assert.ok(error instanceof TypeError);
          return;
        }
        assert.ok(
          Number.isFinite(result) && result >= 0,
          `parseDuration(${JSON.stringify(text.slice(0, 24))}...) -> ${result}`,
        );
      },
    ),
    { numRuns: PURE_RUNS },
  );
});

// ---------------------------------------------------------------------------
// 2. promise tokens (src/db.ts)
// ---------------------------------------------------------------------------

/** A local copy of db.ts's shape check, used to generate and filter fuzz. */
const UUID_TEXT =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * Any 16 bytes in UUID text form -- a deliberate superset of what otra's
 * uuid_v7 emits, so version/variant nibbles get fuzzed too.
 */
const uuidText = fc
  .uint8Array({ minLength: 16, maxLength: 16 })
  .map((bytes) => {
    const hex = Buffer.from(bytes).toString("hex");
    return (
      `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}` +
      `-${hex.slice(16, 20)}-${hex.slice(20)}`
    );
  });

const encodeToken = (parts: readonly string[]) =>
  `otr1_${Buffer.from(parts.join(":")).toString("base64url")}`;

function isWellFormedToken(token: string): boolean {
  if (!token.startsWith("otr1_")) return false;
  const parts = Buffer.from(token.slice(5), "base64url").toString().split(":");
  return parts.length === 3 && parts.every((part) => UUID_TEXT.test(part));
}

// Invariant: the token is a lossless encoding of (queue, root, promise).
test("a promise token roundtrips its address exactly", () => {
  fc.assert(
    fc.property(
      uuidText,
      uuidText,
      uuidText,
      uuidText,
      (queueId, rootId, executionId, promiseId) => {
        const token = promiseToken({ queueId, rootId, executionId }, promiseId);
        assert.match(token, /^otr1_[A-Za-z0-9_-]+$/);
        assert.deepStrictEqual(parsePromiseToken(token), {
          queueId,
          rootId,
          promiseId,
        });
      },
    ),
    { numRuns: PURE_RUNS },
  );
});

// Invariant: the address is (queue, root, promise) -- the owning execution
// is deliberately NOT in it, so the same promise addresses identically
// whichever execution in the tree created it.
test("a promise token does not depend on the executionId", () => {
  fc.assert(
    fc.property(
      uuidText,
      uuidText,
      uuidText,
      uuidText,
      uuidText,
      (queueId, rootId, executionA, executionB, promiseId) => {
        assert.equal(
          promiseToken({ queueId, rootId, executionId: executionA }, promiseId),
          promiseToken({ queueId, rootId, executionId: executionB }, promiseId),
        );
      },
    ),
    { numRuns: PURE_RUNS },
  );
});

/** A UUID with exactly one thing wrong with it. */
const brokenUuid = fc
  .tuple(
    uuidText,
    fc.nat({ max: 35 }),
    fc.constantFrom("drop", "nonhex", "upper", "widen", "empty"),
  )
  .map(([uuid, index, how]) => {
    switch (how) {
      case "drop":
        return uuid.slice(0, index) + uuid.slice(index + 1);
      case "nonhex":
        return uuid.slice(0, index) + "g" + uuid.slice(index + 1);
      case "upper":
        return uuid.toUpperCase();
      case "widen":
        return `${uuid}0`;
      default:
        return "";
    }
  })
  .filter((text) => !UUID_TEXT.test(text));

const notAToken = fc
  .oneof(
    // No magic prefix at all.
    fc.string({ unit: "grapheme" }).filter((s) => !s.startsWith("otr1_")),
    // Right prefix, arbitrary payload.
    fc.string().map((s) => `otr1_${s}`),
    fc
      .uint8Array({ maxLength: 64 })
      .map((b) => `otr1_${Buffer.from(b).toString("base64url")}`),
    // Right prefix, well-formed UUIDs, wrong arity.
    fc.array(uuidText, { maxLength: 2 }).map(encodeToken),
    fc.array(uuidText, { minLength: 4, maxLength: 6 }).map(encodeToken),
    // Right prefix, right arity, exactly one UUID mangled.
    fc
      .tuple(uuidText, uuidText, uuidText, brokenUuid, fc.nat({ max: 2 }))
      .map(([a, b, c, bad, at]) => {
        const parts = [a, b, c];
        parts[at] = bad;
        return encodeToken(parts);
      }),
  )
  .filter((token) => !isWellFormedToken(token));

// Invariant: a token that is not one of ours is refused, never half-parsed.
test("a malformed promise token raises OtraError", () => {
  fc.assert(
    fc.property(notAToken, (token) => {
      assert.throws(() => parsePromiseToken(token), OtraError);
    }),
    { numRuns: PURE_RUNS },
  );
});

// ---------------------------------------------------------------------------
// 5. otra._backoff bounds (sql/schema.sql)
// ---------------------------------------------------------------------------

const HARD_CAP_SECONDS = 86400;

interface Strategy {
  kind: "fixed" | "exponential";
  base_s: number;
  factor: number;
  max_s: number;
}

const validStrategy: fc.Arbitrary<Strategy> = fc.record({
  kind: fc.constantFrom<Strategy["kind"]>("fixed", "exponential"),
  base_s: fc.double({ min: 0, max: HARD_CAP_SECONDS, noNaN: true }),
  factor: fc.double({ min: 1, max: 1000, noNaN: true }),
  max_s: fc.double({ min: 0, max: 1e9, noNaN: true }),
});

const attemptNumber = fc.integer({ min: 1, max: 10000 });

// Invariant: every legal strategy yields a non-negative delay inside the cap,
// for every attempt number -- no overflow, no error, jitter included.
test("otra._backoff stays inside least(max_s, 1 day) for any legal strategy", async () => {
  await fc.assert(
    fc.asyncProperty(
      validStrategy,
      attemptNumber,
      async (strategy, attempt) => {
        const { rows } = await env.pool.query<{ seconds: number }>(
          `select extract(epoch from otra._backoff($1::jsonb, $2::int))::float8
                  as seconds`,
          [JSON.stringify(strategy), attempt],
        );
        const seconds = rows[0]!.seconds;
        const cap = Math.min(strategy.max_s, HARD_CAP_SECONDS);
        const detail = `${JSON.stringify(strategy)} @ attempt ${attempt} -> ${seconds}s`;
        assert.ok(Number.isFinite(seconds), detail);
        assert.ok(seconds >= 0, detail);
        // The stated contract: jitter may add up to 25% on top of the delay.
        assert.ok(seconds <= cap * 1.25 + 1e-6, detail);
        // And the stronger fact the implementation actually guarantees: the
        // caps are applied AFTER jitter, so jitter can never escape them.
        // (1e-6 = make_interval's microsecond resolution.)
        assert.ok(seconds <= cap + 1e-6, detail);
      },
    ),
    { numRuns: DB_RUNS },
  );
});

/**
 * Strategies that are malformed on their face. Each case is constructed so
 * that it CANNOT accidentally be valid: JSON null fields are excluded (they
 * mean "use the default"), and so is `{}`.
 */
const invalidStrategy: fc.Arbitrary<unknown> = fc.oneof(
  // Not a JSON object at all.
  fc.constantFrom<unknown>(null, 0, 1, -1, "fixed", "", true, false),
  fc.array(fc.integer(), { maxLength: 3 }),
  // Unknown kind (case-sensitive, no trimming).
  fc
    .oneof(
      fc.string(),
      fc.constantFrom("linear", "FIXED", "Exponential", "", " fixed", "expo"),
    )
    .filter((kind) => kind !== "fixed" && kind !== "exponential")
    .map((kind) => ({ kind })),
  // base_s outside [0, 86400].
  fc
    .oneof(
      fc.double({ min: -1e6, max: -1e-6, noNaN: true }),
      fc.double({ min: HARD_CAP_SECONDS + 1e-3, max: 1e12, noNaN: true }),
      fc.constantFrom("NaN", "Infinity", "-Infinity"),
    )
    .map((base_s) => ({ base_s })),
  // factor outside [1, 1000].
  fc
    .oneof(
      fc.double({ min: -1e6, max: 1 - 1e-6, noNaN: true }),
      fc.double({ min: 1000 + 1e-3, max: 1e12, noNaN: true }),
      fc.constantFrom("NaN", "Infinity", "-Infinity"),
    )
    .map((factor) => ({ factor })),
  // max_s below zero, or NaN. "Infinity" is legal here (it just clamps to
  // the cap); NaN is not -- see the dedicated property below.
  fc
    .oneof(
      fc.double({ min: -1e9, max: -1e-6, noNaN: true }),
      fc.constantFrom("-Infinity", "NaN"),
    )
    .map((max_s) => ({ max_s })),
  // A field that is not a number at all. Note what is NOT in this list:
  // PostgreSQL 16's float8 input accepts hexadecimal literals, so
  // {"base_s": "0x10"} is a legal 16 seconds, and it accepts surrounding
  // whitespace ("  7  "). Both go on to the range checks like any number.
  fc
    .tuple(
      fc.constantFrom("base_s", "factor", "max_s"),
      fc.constantFrom<unknown>(
        "",
        " ",
        "abc",
        "1,5",
        "5s",
        "1_0",
        "1 2",
        "0b11",
        "0o17",
        "1e",
        ".",
        "1..2",
        true,
        false,
        {},
        [],
        [1],
      ),
    )
    .map(([field, junk]) => ({ [field]: junk })),
);

// Invariant: a malformed strategy is rejected at the edge with OT003 --
// the closed taxonomy, not a stray P0001 or a Postgres cast error.
test("otra._backoff rejects a malformed strategy with errcode OT003", async () => {
  await fc.assert(
    fc.asyncProperty(
      invalidStrategy,
      attemptNumber,
      async (strategy, attempt) => {
        try {
          await env.pool.query("select otra._backoff($1::jsonb, $2::int)", [
            JSON.stringify(strategy),
            attempt,
          ]);
        } catch (error) {
          assert.equal(
            (error as { code?: string }).code,
            "OT003",
            `${JSON.stringify(strategy)} raised ${(error as Error).message}`,
          );
          return;
        }
        assert.fail(`${JSON.stringify(strategy)} was accepted; expected OT003`);
      },
    ),
    { numRuns: DB_RUNS },
  );
});

// Invariant: NaN is malformed in EVERY numeric field, max_s included.
// This was a genuine fast-check finding (shrunk to [{"max_s":"NaN"}, 1]):
// PostgreSQL sorts NaN ABOVE every other float8, so `'NaN' >= 0` is TRUE,
// and max_s -- the one field with no upper bound to trip on -- sailed
// through and was silently reinterpreted as the 86400s hard cap. _backoff
// now checks NaN by equality (Postgres: NaN = NaN is true), while
// "Infinity" stays legal and clamps to the cap.
test("otra._backoff rejects a NaN max_s with OT003", async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.constantFrom("NaN", "nan", "NAN"),
      attemptNumber,
      async (max_s, attempt) => {
        try {
          await env.pool.query("select otra._backoff($1::jsonb, $2::int)", [
            JSON.stringify({ max_s }),
            attempt,
          ]);
        } catch (error) {
          assert.equal((error as { code?: string }).code, "OT003");
          return;
        }
        assert.fail(`max_s ${max_s} was accepted; expected OT003`);
      },
    ),
    { numRuns: DB_RUNS },
  );
});

// ---------------------------------------------------------------------------
// 7. uuid_v7_floor / uuid_v7_timestamp coherence (sql/schema.sql)
// ---------------------------------------------------------------------------

const YEAR_2020 = Date.UTC(2020, 0, 1);
const YEAR_2200 = Date.UTC(2200, 0, 1);

/** An ISO instant with microsecond precision, so the floor has work to do. */
const microTimestamp = fc
  .tuple(
    fc.integer({ min: YEAR_2020, max: YEAR_2200 }),
    fc.integer({ min: 0, max: 999 }),
  )
  .map(([ms, micros]) => ({
    ms,
    text: new Date(ms)
      .toISOString()
      .replace("Z", `${String(micros).padStart(3, "0")}Z`),
  }));

// Invariant: floor is the exact millisecond inverse of uuid_v7_timestamp, and
// it is monotonic -- so a floor id is a usable range bound on uuid_v7 ids.
test("uuid_v7_floor is millisecond-exact and monotonic", async () => {
  try {
    await fc.assert(
      fc.asyncProperty(
        microTimestamp,
        microTimestamp,
        async (first, second) => {
          // Order the pair so the monotonicity claim has a direction.
          const [lo, hi] =
            first.ms <= second.ms ? [first, second] : [second, first];
          const { rows } = await env.pool.query<{
            coherent_lo: boolean;
            coherent_hi: boolean;
            monotonic: boolean;
          }>(
            `select otra.uuid_v7_timestamp(otra.uuid_v7_floor($1::timestamptz))
                      = date_trunc('milliseconds', $1::timestamptz)
                        as coherent_lo,
                    otra.uuid_v7_timestamp(otra.uuid_v7_floor($2::timestamptz))
                      = date_trunc('milliseconds', $2::timestamptz)
                        as coherent_hi,
                    otra.uuid_v7_floor($1::timestamptz)
                      <= otra.uuid_v7_floor($2::timestamptz) as monotonic`,
            [lo.text, hi.text],
          );
          const row = rows[0]!;
          assert.ok(row.coherent_lo, `${lo.text} did not round-trip`);
          assert.ok(row.coherent_hi, `${hi.text} did not round-trip`);
          assert.ok(
            row.monotonic,
            `floor(${lo.text}) > floor(${hi.text}) despite lo <= hi`,
          );
        },
      ),
      { numRuns: DB_RUNS },
    );
  } finally {
    await env.setNow(FROZEN_NOW);
  }
});

// Invariant: a floor id really is a lower bound on the ids generated at or
// after that instant -- this is what makes it safe as a partition boundary.
test("uuid_v7_floor(t) is <= any uuid_v7 generated at or after t", async () => {
  try {
    await fc.assert(
      fc.asyncProperty(
        microTimestamp,
        fc.integer({ min: 0, max: 3_600_000 }),
        async (stamp, laterMs) => {
          const later = new Date(stamp.ms + laterMs).toISOString();
          await env.pool.query("select otra.set_fake_now($1::timestamptz)", [
            later,
          ]);
          const { rows } = await env.pool.query<{ bounded: boolean }>(
            `select otra.uuid_v7_floor($1::timestamptz) <= otra.uuid_v7()
                      as bounded`,
            [stamp.text],
          );
          assert.ok(
            rows[0]!.bounded,
            `floor(${stamp.text}) exceeded a uuid_v7 minted at ${later}`,
          );
        },
      ),
      { numRuns: DB_RUNS },
    );
  } finally {
    await env.setNow(FROZEN_NOW);
  }
});

// ---------------------------------------------------------------------------
// 6. queue-name arithmetic (sql/schema.sql)
// ---------------------------------------------------------------------------

/**
 * Deliberately hostile identifier material: every generated relation name
 * goes through format's %I, so spaces, quotes, case and multibyte characters
 * are all legal -- and 2-, 3- and 4-byte characters are what make
 * octet_length (not character length) the quantity that matters.
 */
const QUEUE_NAME_CHARS = [
  ..."abcdefghijklmnopqrstuvwxyz",
  ..."ABCDEFGHIJKLMNOPQRSTUVWXYZ",
  ..."0123456789",
  " ",
  "-",
  "_",
  ".",
  '"',
  "'",
  "%",
  "$",
  "ñ", // 2 bytes
  "é",
  "Ω",
  "日", // 3 bytes
  "𝄞", // 4 bytes
];

const RESERVED_SUFFIX = /_\d{6}$|_d$/;

/** Clip a character sequence to at most `maxBytes` UTF-8 bytes. */
function clipToBytes(chars: readonly string[], maxBytes: number): string {
  let name = "";
  for (const char of chars) {
    if (Buffer.byteLength(name + char, "utf8") > maxBytes) break;
    name += char;
  }
  return name;
}

const legalQueueName = fc
  .array(fc.constantFrom(...QUEUE_NAME_CHARS), {
    minLength: 1,
    maxLength: 54,
  })
  .map((chars) => clipToBytes(chars, 54))
  .filter(
    (name) =>
      Buffer.byteLength(name, "utf8") >= 1 && !RESERVED_SUFFIX.test(name),
  );

async function otraRelationNames(): Promise<Set<string>> {
  const { rows } = await env.pool.query<{ relname: string }>(
    `select c.relname::text as relname
       from pg_class c
       join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'otra'`,
  );
  return new Set(rows.map((row) => row.relname));
}

// Invariant: a name inside the 54-byte budget provisions, and every relation
// it produces fits Postgres's 63-byte identifier limit *without truncation* --
// truncation would fold two ISO weeks onto one partition name and lose data.
test("every relation a legal queue name generates fits in 63 bytes", async () => {
  await fc.assert(
    fc.asyncProperty(legalQueueName, async (name) => {
      const before = await otraRelationNames();
      // Partitioned mode is where the arithmetic binds: the widest
      // identifier otra generates is a week partition, 'x_' + name + '_' +
      // a 6-character ISO tag = 54 + 9 = 63 bytes at the cap.
      await env.app.createQueue(name, { storageMode: "partitioned" });
      try {
        const after = await otraRelationNames();
        const created = [...after].filter((rel) => !before.has(rel));
        assert.ok(
          created.length > 0,
          `createQueue(${JSON.stringify(name)}) created nothing`,
        );
        for (const rel of created) {
          const bytes = Buffer.byteLength(rel, "utf8");
          assert.ok(
            bytes <= 63,
            `relation ${JSON.stringify(rel)} is ${bytes} bytes`,
          );
        }
        // Our own base names survive verbatim: nothing was truncated on the
        // way in, so \dt and pg_locks still read back as the queue's name.
        for (const prefix of ["x_", "p_", "e_", "i_"]) {
          assert.ok(
            after.has(prefix + name),
            `expected relation ${JSON.stringify(prefix + name)}`,
          );
        }
        // Partition children keep their full suffix (_<6-digit ISO week>, or
        // _d for the default): a truncated name would have lost those
        // digits, and two weeks would have collided on one relation.
        const { rows } = await env.pool.query<{ relname: string }>(
          `select child.relname::text as relname
             from pg_inherits i
             join pg_class parent on parent.oid = i.inhparent
             join pg_namespace n
               on n.oid = parent.relnamespace and n.nspname = 'otra'
             join pg_class child on child.oid = i.inhrelid
            where parent.relname = $1`,
          [`x_${name}`],
        );
        assert.ok(rows.length > 0, `x_${name} has no partitions`);
        for (const { relname } of rows) {
          const suffix = relname.slice(`x_${name}_`.length);
          assert.ok(
            relname.startsWith(`x_${name}_`) &&
              (suffix === "d" || /^\d{6}$/.test(suffix)),
            `partition ${JSON.stringify(relname)} does not look intact`,
          );
        }
      } finally {
        // Provisioning is not free and the env is shared: give it all back.
        await env.app.dropQueue(name, { force: true });
      }
    }),
    { numRuns: PROVISION_RUNS },
  );
});

const oversizedQueueName = fc
  .array(fc.constantFrom(...QUEUE_NAME_CHARS), {
    minLength: 20,
    maxLength: 120,
  })
  .map((chars) => clipToBytes(chars, 200))
  .filter((name) => Buffer.byteLength(name, "utf8") > 54);

const reservedQueueName = fc.oneof(
  fc
    .tuple(
      fc.array(fc.constantFrom(...QUEUE_NAME_CHARS), {
        minLength: 1,
        maxLength: 20,
      }),
      fc.integer({ min: 0, max: 999999 }),
    )
    .map(
      ([chars, week]) =>
        `${clipToBytes(chars, 40)}_${String(week).padStart(6, "0")}`,
    ),
  fc
    .array(fc.constantFrom(...QUEUE_NAME_CHARS), {
      minLength: 1,
      maxLength: 20,
    })
    .map((chars) => `${clipToBytes(chars, 40)}_d`),
);

/** Assert `create_queue` refuses this name with errcode OT003. */
async function assertRejected(name: string): Promise<void> {
  try {
    await env.app.createQueue(name, { storageMode: "partitioned" });
  } catch (error) {
    assert.equal(
      (error as { code?: string }).code,
      "OT003",
      `${JSON.stringify(name)} raised ${(error as Error).message}`,
    );
    return;
  }
  await env.app.dropQueue(name, { force: true });
  assert.fail(`${JSON.stringify(name)} was accepted; expected OT003`);
}

// Invariant: names that would blow the identifier budget, or shadow another
// queue's partitions, are refused up front (OT003) rather than provisioned
// into a truncated collision.
test("queue names over 54 bytes or with a reserved suffix are refused", async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.oneof(oversizedQueueName, reservedQueueName),
      async (name) => {
        await assertRejected(name);
      },
    ),
    { numRuns: DB_RUNS },
  );
});

// ---------------------------------------------------------------------------
// 4. JSON replay-value roundtrip (the deep one)
// ---------------------------------------------------------------------------

/**
 * A UTF-16 code unit with no partner: what `String.prototype.isWellFormed`
 * looks for, written out because tsconfig pins `lib` at ES2022.
 */
const LONE_SURROGATE =
  /[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/;

/**
 * Postgres jsonb cannot hold U+0000 or an unpaired surrogate (see the
 * skipped property below), so the generator stays inside what the journal
 * can actually store.
 */
const jsonbSafeString = fc
  .oneof(
    fc.string(),
    fc.string({ unit: "grapheme" }),
    fc.string({
      unit: fc.constantFrom(
        "é",
        "日",
        "𝄞",
        "ß",
        "🙂",
        "\t",
        "\n",
        '"',
        "\\",
        "/",
        "\u007f",
        "\u2028",
        " ",
      ),
    }),
  )
  .filter((text) => !text.includes("\u0000") && !LONE_SURROGATE.test(text));

const jsonNumber = fc.oneof(
  fc.double({ noNaN: true, noDefaultInfinity: true }),
  fc.integer(),
  fc.maxSafeInteger(),
  fc.constantFrom(
    0,
    -0,
    0.1,
    -0.1,
    1e-7,
    1e21,
    Number.MAX_VALUE,
    Number.MIN_VALUE,
    Number.MAX_SAFE_INTEGER,
    -Number.MAX_SAFE_INTEGER,
  ),
);

const jsonLeaf: fc.Arbitrary<JsonValue> = fc.oneof(
  fc.constant(null),
  fc.boolean(),
  jsonNumber,
  jsonbSafeString,
) as fc.Arbitrary<JsonValue>;

const jsonValue: fc.Arbitrary<JsonValue> = fc.letrec<{ node: JsonValue }>(
  (tie) => ({
    node: fc.oneof(
      { maxDepth: 3, depthIdentifier: "json" },
      { arbitrary: jsonLeaf, weight: 4 },
      {
        arbitrary: fc.array(tie("node"), { maxLength: 4 }),
        weight: 1,
      },
      {
        arbitrary: fc.dictionary(jsonbSafeString, tie("node"), {
          maxKeys: 4,
        }),
        weight: 1,
      },
    ) as fc.Arbitrary<JsonValue>,
  }),
).node;

/**
 * JSON has no -0: `JSON.stringify(-0)` is `"0"`. Normalize both sides so the
 * comparison is about the roundtrip and not about that known collapse.
 */
function normalizeMinusZero(value: unknown): unknown {
  if (typeof value === "number") return value === 0 ? 0 : value;
  if (Array.isArray(value)) return value.map(normalizeMinusZero);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, inner]) => [
        key,
        normalizeMinusZero(inner),
      ]),
    );
  }
  return value;
}

// Invariant: what a ctx.run hands back is exactly its value after ONE JSON
// roundtrip -- both on the executing attempt (the driver returns the value
// the journal stored, not the in-memory one) and on every replay after it.
test("a run's value survives the journal identically on first run and replay", async () => {
  await fc.assert(
    fc.asyncProperty(jsonValue, async (value) => {
      const expected = JSON.parse(JSON.stringify(value)) as JsonValue;
      const state: JsonRoundtripState = {
        value,
        entries: 0,
        firstSeen: undefined,
        replaySeen: undefined,
      };
      jsonState = state;

      const ref = await env.app.spawn(jsonTask, null, {
        maxAttempts: 2,
        // base_s 0 means the forced retry is runnable immediately, so one
        // drain() covers both attempts and the fake clock stays put.
        retryStrategy: { kind: "fixed", base_s: 0, max_s: 60 },
      });
      await jsonWorker.drain();

      const snapshot = await env.app.getExecution(ref);
      assert.equal(
        snapshot?.status,
        "completed",
        `status ${snapshot?.status}: ${JSON.stringify(snapshot?.error)}`,
      );
      assert.equal(
        state.entries,
        2,
        "the forced failure did not produce a second attempt",
      );

      // 1. The executing attempt already sees the journal's version -- the
      //    driver hands back what record_run STORED, not the in-memory
      //    return. Compared without normalizing, deliberately: -0 is the one
      //    value that survives in memory but not through JSON, so this is
      //    what gives the claim teeth (deepStrictEqual separates 0 from -0).
      assert.deepStrictEqual(state.firstSeen, expected);
      // 2. The replay reads the identical value back out of the journal --
      //    the memoized step never re-executed.
      assert.deepStrictEqual(
        normalizeMinusZero(state.replaySeen),
        normalizeMinusZero(state.firstSeen),
      );
      // 3. And the execution's recorded result agrees with both.
      assert.deepStrictEqual(
        normalizeMinusZero(snapshot!.result),
        normalizeMinusZero(expected),
      );
    }),
    { numRuns: DB_RUNS },
  );
});

/**
 * FINDING, now FIXED. jsonb cannot hold U+0000 or an unpaired surrogate;
 * these used to make the journal INSERT itself fail outside the step
 * try/catch -- a poison pill that wedged the execution retrying forever.
 * The engine now sanitizes hostile code points in string VALUES to U+FFFD
 * on the way in (a lone surrogate is not a Unicode character at all), and
 * the driver treats any residual Postgres data exception (class 22, e.g. a
 * hostile object KEY) as a permanent, readable failure.
 */
const LONE_SURROGATE_G =
  /[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/g;

function sanitizeHostile(text: string): string {
  return text
    .replace(LONE_SURROGATE_G, "\uFFFD")
    .replaceAll("\u0000", "\uFFFD");
}

test("jsonb-hostile strings checkpoint as U+FFFD instead of wedging", async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.oneof(
        fc.constantFrom("\u0000", "\ud800", "\udfff", "a\u0000b", "x\ud800y"),
        fc.string({ unit: "binary" }),
        fc.string(),
      ),
      async (text) => {
        const expected = JSON.parse(
          JSON.stringify(sanitizeHostile(text)),
        ) as JsonValue;
        const state: JsonRoundtripState = {
          value: text,
          entries: 0,
          firstSeen: undefined,
          replaySeen: undefined,
        };
        jsonState = state;
        const ref = await env.app.spawn(jsonTask, null, {
          maxAttempts: 2,
          retryStrategy: { kind: "fixed", base_s: 0, max_s: 60 },
        });
        await jsonWorker.drain();
        const snapshot = await env.app.getExecution(ref);
        assert.equal(
          snapshot?.status,
          "completed",
          `status ${snapshot?.status}: ${JSON.stringify(snapshot?.error)}`,
        );
        assert.deepStrictEqual(state.firstSeen, expected);
        assert.deepStrictEqual(state.replaySeen, expected);
        assert.deepStrictEqual(snapshot!.result, expected);
      },
    ),
    { numRuns: DB_RUNS },
  );
});

// ---------------------------------------------------------------------------
// 3. deterministic defer jitter (src/worker.ts)
// ---------------------------------------------------------------------------

/**
 * `deterministicJitterSeconds` is deliberately not exported, and its seed --
 * the executionId -- is minted by the database, so there is no user-facing
 * input for fast-check to generate and shrink. What IS observable is its one
 * call site: an unknown function name defers the claim by
 * 15 + FNV-1a(executionId) % 15 seconds. So this property generates the
 * BATCH (a fresh set of engine-minted seeds per run) and asserts the two
 * things the jitter promises: it lands inside its window, and it is stable
 * per execution rather than re-randomised on every defer.
 */
const UNKNOWN_TASK_NAME = "prop-no-such-task-anywhere";
const DEFER_BASE_SECONDS = 15;
const DEFER_WINDOW_SECONDS = 15;

async function deferralSeconds(executionId: string): Promise<number> {
  const { rows } = await env.pool.query<{ delta: number }>(
    `select extract(epoch from (x.run_after - otra.now()))::float8 as delta
       from otra.x_default x
      where x.id = $1::uuid`,
    [executionId],
  );
  return rows[0]!.delta;
}

// Invariant: an unknown task's deferral is jittered inside [15s, 30s) and is
// a function of the executionId alone -- so a fleet spreads out, but each
// execution's delay is the same on every worker and every defer.
test("an unknown task's deferral is jittered in-window and stable per execution", async () => {
  try {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (count) => {
        const refs = [];
        for (let i = 0; i < count; i++) {
          refs.push(
            await env.app.spawn(UNKNOWN_TASK_NAME, null, {
              queue: "default",
            }),
          );
        }
        try {
          await deferWorker.tick();
          const first = await Promise.all(
            refs.map((ref) => deferralSeconds(ref.executionId)),
          );
          for (const [i, delta] of first.entries()) {
            assert.ok(
              delta >= DEFER_BASE_SECONDS &&
                delta < DEFER_BASE_SECONDS + DEFER_WINDOW_SECONDS,
              `execution ${refs[i]!.executionId} deferred ${delta}s, ` +
                `outside [${DEFER_BASE_SECONDS}, ` +
                `${DEFER_BASE_SECONDS + DEFER_WINDOW_SECONDS})`,
            );
          }

          // Defer the same executions again: same seed, same offset.
          await env.advance(DEFER_BASE_SECONDS + DEFER_WINDOW_SECONDS + 1);
          await deferWorker.tick();
          const second = await Promise.all(
            refs.map((ref) => deferralSeconds(ref.executionId)),
          );
          assert.deepStrictEqual(
            second,
            first,
            "the deferral was re-randomised instead of being deterministic",
          );
        } finally {
          // Retire them so later runs (and later properties) do not keep
          // re-claiming a growing backlog of deferred executions.
          for (const ref of refs) {
            await env.app.kill(ref, { reason: "property teardown" });
          }
        }
      }),
      { numRuns: DEFER_RUNS },
    );
  } finally {
    await env.setNow(FROZEN_NOW);
  }
});
