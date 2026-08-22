import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import pg from "pg";

import { Otra } from "../src/index.ts";

let container: StartedPostgreSqlContainer | undefined;
const TEMPLATE_DATABASE = "otra_template";

function databaseUrl(connectionString: string, database: string): string {
  const url = new URL(connectionString);
  url.pathname = `/${database}`;
  return url.href;
}

export async function globalSetup(): Promise<void> {
  if (process.env.OTRA_TEST_DB) return;

  container = await new PostgreSqlContainer("postgres:16-alpine").start();
  const adminUrl = container.getConnectionUri();
  const admin = new pg.Pool({ connectionString: adminUrl });
  try {
    await admin.query(`create database ${TEMPLATE_DATABASE}`);
  } finally {
    await admin.end();
  }

  const template = new pg.Pool({
    connectionString: databaseUrl(adminUrl, TEMPLATE_DATABASE),
  });
  try {
    await template.query(Otra.schemaSql());
  } finally {
    await template.end();
  }

  // Lock the template down the way template0 is: with connections barred,
  // autovacuum skips it and nothing can hold it open, so `create database
  // ... template otra_template` never trips over "source database is being
  // accessed by other users". Cloning is unaffected -- the template read
  // path does not open a normal connection (verified against PG 16).
  const lockdown = new pg.Pool({ connectionString: adminUrl });
  try {
    await lockdown.query(
      `alter database ${TEMPLATE_DATABASE} with allow_connections false`,
    );
  } finally {
    await lockdown.end();
  }

  process.env.OTRA_TEST_ADMIN_DB = adminUrl;
  process.env.OTRA_TEST_TEMPLATE_DB = TEMPLATE_DATABASE;
}

export async function globalTeardown(): Promise<void> {
  await container?.stop();
}
