import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";

let container: StartedPostgreSqlContainer | undefined;

export async function globalSetup(): Promise<void> {
  if (process.env.OTRA_TEST_DB) return;

  container = await new PostgreSqlContainer("postgres:16-alpine").start();
  process.env.OTRA_TEST_DB = container.getConnectionUri();
}

export async function globalTeardown(): Promise<void> {
  await container?.stop();
}
