import pg from "pg";

import { Otra } from "../src/index.ts";

/**
 * The global test setup starts Postgres with Testcontainers. Point
 * OTRA_TEST_DB at an existing database to bypass the container, e.g.:
 *   OTRA_TEST_DB=postgres://postgres@127.0.0.1:5433/postgres make test
 * The otra schema in that database is dropped and recreated per test.
 */
const DSN =
  process.env.OTRA_TEST_DB ?? "postgres://postgres@127.0.0.1:5433/postgres";

export interface TestEnv {
  app: Otra;
  pool: pg.Pool;
  /** Freeze database time (all workers see it, across connections). */
  setNow(when: string | Date): Promise<void>;
  /** Advance frozen database time by this many seconds. */
  advance(seconds: number): Promise<void>;
  close(): Promise<void>;
}

/** Poll a condition instead of sleeping (absurd-style test gate support). */
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options: { timeoutMs?: number; intervalMs?: number; label?: string } = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 3_000;
  const intervalMs = options.intervalMs ?? 10;
  const deadline = Date.now() + timeoutMs;
  while (true) {
    if (await condition()) return;
    if (Date.now() > deadline) {
      throw new Error(`waitFor timed out: ${options.label ?? "condition"}`);
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

export async function createTestEnv(queue = "default"): Promise<TestEnv> {
  const pool = new pg.Pool({ connectionString: DSN, max: 4 });
  await pool.query("drop schema if exists otra cascade");
  const app = new Otra({ db: pool, queue });
  await app.applySchema();
  await pool.query("select otra.set_fake_now($1)", ["2026-01-01T00:00:00Z"]);
  return {
    app,
    pool,
    async setNow(when) {
      await pool.query("select otra.set_fake_now($1)", [
        when instanceof Date ? when.toISOString() : when,
      ]);
    },
    async advance(seconds) {
      await pool.query(
        "select otra.advance_fake_now(make_interval(secs => $1))",
        [seconds],
      );
    },
    async close() {
      await pool.end();
    },
  };
}
