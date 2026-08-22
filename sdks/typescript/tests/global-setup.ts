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

  process.env.OTRA_TEST_ADMIN_DB = adminUrl;
  process.env.OTRA_TEST_TEMPLATE_DB = TEMPLATE_DATABASE;
}

export async function globalTeardown(): Promise<void> {
  await container?.stop();
}
