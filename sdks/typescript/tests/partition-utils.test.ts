import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

import { createTestEnv, type TestEnv } from "./helpers.ts";

let env: TestEnv;
beforeEach(async () => {
  env = await createTestEnv();
});
afterEach(async () => {
  await env.close();
});

test("UUIDv7 generation follows the shared database clock", async () => {
  const { rows } = await env.pool.query(
    `select otra.uuid_v7_timestamp(otra.uuid_v7()) as generated_at,
            otra.uuid_v7_timestamp(gen_random_uuid()) as non_v7`,
  );

  assert.equal(rows[0].generated_at.toISOString(), "2026-01-01T00:00:00.000Z");
  assert.equal(rows[0].non_v7, null);
});

test("UUIDv7 partition bounds and UTC week tags are deterministic", async () => {
  const { rows } = await env.pool.query(
    `select
       otra.uuid_v7_timestamp(
         otra.uuid_v7_floor('2024-04-01T10:20:30.123789Z')
       ) as floored,
       otra.week_bucket_utc('2024-04-03T15:30:00Z') as week_start,
       otra.partition_week_tag('2021-01-01T12:00:00Z') as start_boundary,
       otra.partition_week_tag('2024-12-31T12:00:00Z') as end_boundary`,
  );

  assert.equal(rows[0].floored.toISOString(), "2024-04-01T10:20:30.123Z");
  assert.equal(rows[0].week_start.toISOString(), "2024-04-01T00:00:00.000Z");
  assert.equal(rows[0].start_boundary, "202053");
  assert.equal(rows[0].end_boundary, "202501");
});

test("UUIDv7 generation rejects timestamps outside its 48-bit range", async () => {
  await env.setNow("1960-01-01T00:00:00Z");
  await assert.rejects(
    env.pool.query(`select otra.uuid_v7()`),
    /outside UUIDv7 supported range/,
  );

  await env.setNow("12000-01-01T00:00:00Z");
  await assert.rejects(
    env.pool.query(`select otra.uuid_v7()`),
    /outside UUIDv7 supported range/,
  );
});
