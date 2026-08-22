import { randomUUID } from "node:crypto";

import pg from "pg";

import { Otra } from "../src/index.ts";

/**
 * The global test setup prepares a template in a Postgres Testcontainer;
 * each test clones an isolated database from it and drops it on close().
 * Point OTRA_TEST_DB at an existing database to use serial schema resets
 * instead, e.g.:
 *   OTRA_TEST_DB=postgres://postgres@127.0.0.1:5433/postgres make test
 *
 * There is deliberately NO default DSN: without one of the two modes
 * configured, eight-wide test files would silently share one database and
 * fail incomprehensibly instead of naming the real problem.
 */
function databaseUrl(connectionString: string, database: string): string {
  const url = new URL(connectionString);
  url.pathname = `/${database}`;
  return url.href;
}

/** An isolated clone of the template database, plus how to drop it again. */
interface IsolatedDatabase {
  connectionString: string;
  adminUrl: string;
  database: string;
}

async function createIsolatedDatabase(
  adminUrl: string,
  template: string,
): Promise<IsolatedDatabase> {
  const database = `otra_test_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  try {
    await admin.query(`create database "${database}" template "${template}"`);
  } finally {
    await admin.end();
  }
  return {
    connectionString: databaseUrl(adminUrl, database),
    adminUrl,
    database,
  };
}

/** Postgres: "source/target database is being accessed by other users". */
function isDatabaseInUse(error: unknown): boolean {
  return (error as { code?: string } | null)?.code === "55006";
}

/**
 * Give the cloned database back; otherwise a run leaks one database per test
 * (~85 per suite, hundreds of MB).
 *
 * The drop is deliberately NOT `with (force)` on the first attempts: a
 * backend whose client just called end() lingers in pg_stat_activity for a
 * few milliseconds, and force-dropping SIGTERMs it -- the FATAL reaches the
 * dying socket and surfaces in the test process as an unhandled 'error'
 * after the test has ended ("terminating connection due to administrator
 * command"), failing whole files. So wait the stragglers out, and force only
 * as a last resort, when a genuinely leaked connection is holding the
 * database open.
 */
async function dropIsolatedDatabase(isolated: IsolatedDatabase): Promise<void> {
  const admin = new pg.Client({ connectionString: isolated.adminUrl });
  await admin.connect();
  try {
    for (let attempt = 0; attempt < 10; attempt++) {
      try {
        await admin.query(`drop database if exists "${isolated.database}"`);
        return;
      } catch (error) {
        if (!isDatabaseInUse(error)) throw error;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
    await admin.query(
      `drop database if exists "${isolated.database}" with (force)`,
    );
  } finally {
    await admin.end();
  }
}

/** Resolve the database this test runs against, or say why we cannot. */
async function resolveDatabase(): Promise<{
  connectionString: string;
  /** Set only in isolated-clone mode: the clone close() must drop. */
  isolated?: IsolatedDatabase;
}> {
  const adminUrl = process.env.OTRA_TEST_ADMIN_DB;
  const template = process.env.OTRA_TEST_TEMPLATE_DB;
  if (adminUrl && template) {
    const isolated = await createIsolatedDatabase(adminUrl, template);
    return { connectionString: isolated.connectionString, isolated };
  }
  const shared = process.env.OTRA_TEST_DB;
  if (shared) return { connectionString: shared };
  throw new Error(
    "test global setup did not run and OTRA_TEST_DB is not set: run the " +
      "suite with `npm test` (Node >= 24, Docker for the Testcontainer) or " +
      "point OTRA_TEST_DB at an existing Postgres, e.g. " +
      "OTRA_TEST_DB=postgres://postgres@127.0.0.1:5433/postgres npm test",
  );
}

export interface TestEnv {
  app: Otra;
  pool: pg.Pool;
  connectionString: string;
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

export async function createTestEnv(
  queue = "default",
  provisionQueue = true,
): Promise<TestEnv> {
  const { connectionString, isolated } = await resolveDatabase();
  const pool = new pg.Pool({ connectionString, max: 4 });
  const app = new Otra({ db: pool, queue });
  if (!isolated) {
    await pool.query("drop schema if exists otra cascade");
    await app.applySchema();
  }
  if (provisionQueue) await app.createQueue();
  await pool.query("select otra.set_fake_now($1)", ["2026-01-01T00:00:00Z"]);
  return {
    app,
    pool,
    connectionString,
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
      // Shared-DSN mode owns its database; only clones are ours to drop.
      if (isolated) await dropIsolatedDatabase(isolated);
    },
  };
}
