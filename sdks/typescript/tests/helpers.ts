import { randomUUID } from "node:crypto";

import pg from "pg";

import { Otra } from "../src/index.ts";

/**
 * The global test setup prepares a template in a Postgres Testcontainer;
 * each test clones an isolated database from it. Point OTRA_TEST_DB at an
 * existing database to use serial schema resets instead, e.g.:
 *   OTRA_TEST_DB=postgres://postgres@127.0.0.1:5433/postgres make test
 */
const SHARED_DSN =
  process.env.OTRA_TEST_DB ?? "postgres://postgres@127.0.0.1:5433/postgres";

function databaseUrl(connectionString: string, database: string): string {
  const url = new URL(connectionString);
  url.pathname = `/${database}`;
  return url.href;
}

async function isolatedDatabaseUrl(): Promise<string | undefined> {
  const adminUrl = process.env.OTRA_TEST_ADMIN_DB;
  const template = process.env.OTRA_TEST_TEMPLATE_DB;
  if (!adminUrl || !template) return undefined;

  const database = `otra_test_${randomUUID().replaceAll("-", "")}`;
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  try {
    await admin.query(`create database "${database}" template "${template}"`);
  } finally {
    await admin.end();
  }
  return databaseUrl(adminUrl, database);
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

export async function createTestEnv(queue = "default"): Promise<TestEnv> {
  const isolatedUrl = await isolatedDatabaseUrl();
  const connectionString = isolatedUrl ?? SHARED_DSN;
  const pool = new pg.Pool({ connectionString, max: 4 });
  const app = new Otra({ db: pool, queue });
  if (!isolatedUrl) {
    await pool.query("drop schema if exists otra cascade");
    await app.applySchema();
  }
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
    },
  };
}
