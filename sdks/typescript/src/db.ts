import type * as pg from "pg";

import {
  OtraError,
  type ChildSpawnOptions,
  type ErrorPayload,
  type ExecutionRef,
  type ExecutionSnapshot,
  type JsonValue,
  type SpawnOptions,
} from "./types.ts";

/** The only database interface otra needs. */
export type Queryable = Pick<pg.Pool, "query">;

export type PromiseKind =
  | "run"
  | "sleep"
  | "event"
  | "child"
  | "external"
  | "cancel";
export type PromiseStatus = "pending" | "resolved" | "rejected";

export interface PromiseRow {
  /** Row id (set on rows loaded from the database); doubles as the
   *  settlement token for kind 'external'. */
  id?: string;
  key: string;
  label: string;
  kind: PromiseKind;
  status: PromiseStatus;
  value: JsonValue;
  error: ErrorPayload | null;
  childExecutionId: string | null;
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** Wrap a promise row id as the opaque token handed to the outside world. */
export function promiseToken(execution: ExecutionRef, promiseId: string): string {
  const address = `${execution.queueId}:${execution.rootId}:${promiseId}`;
  return `otr1_${Buffer.from(address).toString("base64url")}`;
}

/** Parse a token back to its queue-local promise address. */
export function parsePromiseToken(
  token: string,
): { queueId: string; rootId: string; promiseId: string } {
  if (!token.startsWith("otr1_")) {
    throw new OtraError(`invalid promise token: ${JSON.stringify(token)}`);
  }
  let parts: string[];
  try {
    parts = Buffer.from(token.slice(5), "base64url").toString().split(":");
  } catch {
    throw new OtraError(`invalid promise token: ${JSON.stringify(token)}`);
  }
  if (parts.length !== 3 || parts.some((part) => !UUID_PATTERN.test(part))) {
    throw new OtraError(`invalid promise token: ${JSON.stringify(token)}`);
  }
  return { queueId: parts[0]!, rootId: parts[1]!, promiseId: parts[2]! };
}

export interface ClaimedExecution extends ExecutionRef {
  functionName: string;
  params: JsonValue;
  attempt: number;
  maxAttempts: number;
  cancelRequested: boolean;
}

export interface Queue {
  name: string;
  storageMode: QueueStorageMode;
}

export type QueueStorageMode = "unpartitioned" | "partitioned";
export type QueueDetachMode = "none" | "empty";

export interface QueuePolicyOptions {
  defaultPartition?: "enabled" | "disabled";
  partitionLookahead?: string;
  partitionLookback?: string;
  cleanupTtl?: string;
  cleanupLimit?: number;
  detachMode?: QueueDetachMode;
  detachMinAge?: string;
}

export interface QueuePolicy extends Required<QueuePolicyOptions> {
  name: string;
  storageMode: QueueStorageMode;
  defaultPartition: "enabled" | "disabled";
}

export interface DetachCandidate {
  queueName: string;
  parentTable: string;
  partitionTable: string;
}

function toJson(value: unknown): string {
  const encoded = JSON.stringify(value === undefined ? null : value);
  // JSON.stringify still returns undefined for bare functions/symbols.
  return encoded === undefined ? "null" : encoded;
}

function normalizeSpawnOptions(
  options: SpawnOptions | ChildSpawnOptions,
): Record<string, unknown> {
  const opts: Record<string, unknown> = {};
  if (options.maxAttempts !== undefined)
    opts.max_attempts = options.maxAttempts;
  if (options.retryStrategy !== undefined)
    opts.retry_strategy = options.retryStrategy;
  if (options.delaySeconds !== undefined) opts.delay_s = options.delaySeconds;
  if ("idempotencyKey" in options && options.idempotencyKey !== undefined)
    opts.idempotency_key = options.idempotencyKey;
  if (options.onParentCancel !== undefined)
    opts.on_parent_cancel = options.onParentCancel;
  return opts;
}

/**
 * True when an error is Postgres sqlstate OT001: the calling worker no
 * longer holds the claim on the execution (a zombie detecting its own
 * demise).  The driver maps this to a quiet "lost" outcome.
 */
export function isClaimLost(err: unknown): boolean {
  return (err as { code?: unknown })?.code === "OT001";
}

/**
 * True when an error is Postgres sqlstate OT002: the execution was killed
 * (an operator action, not a stolen claim). The driver reports "killed" and
 * runs no compensation -- no history write it made would be legal.
 */
export function isKilled(err: unknown): boolean {
  return (err as { code?: unknown })?.code === "OT002";
}

/** Thin, typed wrapper over the otra.* stored functions. */
export class Db {
  private readonly client: Queryable;

  constructor(client: Queryable) {
    this.client = client;
  }

  async createQueue(
    name: string,
    storageMode: QueueStorageMode = "unpartitioned",
  ): Promise<void> {
    await this.client.query(`select otra.create_queue($1, $2)`, [
      name,
      storageMode,
    ]);
  }

  async getQueue(name: string): Promise<Queue | null> {
    const { rows } = await this.client.query(
      `select name, storage_mode from otra.get_queue($1)`,
      [name],
    );
    if (rows.length === 0) return null;
    return { name: rows[0].name, storageMode: rows[0].storage_mode };
  }

  async listQueues(): Promise<Queue[]> {
    const { rows } = await this.client.query(
      `select name, storage_mode from otra.list_queues()`,
    );
    return rows.map((row: Record<string, unknown>) => ({
      name: row.name as string,
      storageMode: row.storage_mode as Queue["storageMode"],
    }));
  }

  async ensurePartitions(name?: string): Promise<void> {
    await this.client.query(`select otra.ensure_partitions($1)`, [name ?? null]);
  }

  async setQueuePolicy(name: string, options: QueuePolicyOptions): Promise<void> {
    const policy: Record<string, unknown> = {};
    if (options.defaultPartition !== undefined)
      policy.default_partition = options.defaultPartition;
    if (options.partitionLookahead !== undefined)
      policy.partition_lookahead = options.partitionLookahead;
    if (options.partitionLookback !== undefined)
      policy.partition_lookback = options.partitionLookback;
    if (options.cleanupTtl !== undefined) policy.cleanup_ttl = options.cleanupTtl;
    if (options.cleanupLimit !== undefined)
      policy.cleanup_limit = options.cleanupLimit;
    if (options.detachMode !== undefined) policy.detach_mode = options.detachMode;
    if (options.detachMinAge !== undefined)
      policy.detach_min_age = options.detachMinAge;
    await this.client.query(`select otra.set_queue_policy($1, $2::jsonb)`, [
      name,
      JSON.stringify(policy),
    ]);
  }

  async getQueuePolicy(name: string): Promise<QueuePolicy | null> {
    const { rows } = await this.client.query(
      `select name, storage_mode, default_partition,
              partition_lookahead::text, partition_lookback::text,
              cleanup_ttl::text, cleanup_limit, detach_mode,
              detach_min_age::text
         from otra.get_queue_policy($1)`,
      [name],
    );
    if (rows.length === 0) return null;
    const row = rows[0];
    return {
      name: row.name,
      storageMode: row.storage_mode,
      defaultPartition: row.default_partition,
      partitionLookahead: row.partition_lookahead,
      partitionLookback: row.partition_lookback,
      cleanupTtl: row.cleanup_ttl,
      cleanupLimit: row.cleanup_limit,
      detachMode: row.detach_mode,
      detachMinAge: row.detach_min_age,
    };
  }

  async listDetachCandidates(name?: string): Promise<DetachCandidate[]> {
    const { rows } = await this.client.query(
      `select queue_name, parent_table, partition_table
         from otra.list_detach_candidates($1)`,
      [name ?? null],
    );
    return rows.map((row: Record<string, unknown>) => ({
      queueName: row.queue_name as string,
      parentTable: row.parent_table as string,
      partitionTable: row.partition_table as string,
    }));
  }

  async spawn(
    functionName: string,
    params: unknown,
    queue: string,
    options: SpawnOptions | ChildSpawnOptions = {},
    parent?: {
      execution: ExecutionRef;
      key: string;
      label: string;
      worker: string;
    },
  ): Promise<ExecutionRef & { created: boolean }> {
    if (parent !== undefined) {
      const { rows } = await this.client.query(
        `select queue_id, root_id, execution_id, created
           from otra.spawn_child_local(
             $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb
           )`,
        [
          parent.execution.queueId,
          parent.execution.rootId,
          parent.execution.executionId,
          parent.worker,
          parent.key,
          parent.label,
          functionName,
          toJson(params),
          toJson(normalizeSpawnOptions(options)),
        ],
      );
      return {
        queueId: rows[0].queue_id,
        rootId: rows[0].root_id,
        executionId: rows[0].execution_id,
        created: rows[0].created,
      };
    }
    const { rows } = await this.client.query(
      `select queue_id, root_id, execution_id, created
         from otra.spawn_local($1, $2::jsonb, $3, $4::jsonb)`,
      [
        functionName,
        toJson(params),
        queue,
        toJson(normalizeSpawnOptions(options)),
      ],
    );
    return {
      queueId: rows[0].queue_id,
      rootId: rows[0].root_id,
      executionId: rows[0].execution_id,
      created: rows[0].created,
    };
  }

  async claim(
    queue: string,
    workerId: string,
    claimSeconds: number,
    batchSize: number,
  ): Promise<ClaimedExecution[]> {
    const { rows } = await this.client.query(
      `select queue_id, root_id, execution_id, function_name, params, attempt,
              max_attempts, cancel_requested
         from otra.claim_local($1, $2, $3, $4)`,
      [queue, workerId, claimSeconds, batchSize],
    );
    return rows.map((row: Record<string, unknown>) => ({
      queueId: row.queue_id as string,
      rootId: row.root_id as string,
      executionId: row.execution_id as string,
      functionName: row.function_name as string,
      params: row.params as JsonValue,
      attempt: row.attempt as number,
      maxAttempts: row.max_attempts as number,
      cancelRequested: row.cancel_requested === true,
    }));
  }

  private static rowToPromise(row: Record<string, unknown>): PromiseRow {
    return {
      id: row.id as string,
      key: row.key as string,
      label: row.label as string,
      kind: row.kind as PromiseKind,
      status: row.status as PromiseStatus,
      value: (row.value ?? null) as JsonValue,
      error: (row.error ?? null) as ErrorPayload | null,
      childExecutionId: (row.child_execution_id ?? null) as string | null,
    };
  }

  async loadHistory(execution: ExecutionRef): Promise<Map<string, PromiseRow>> {
    const { rows } = await this.client.query(
      `select id, key, label, kind, status, value, error, child_execution_id
         from otra.load_history_local($1, $2, $3)`,
      [execution.queueId, execution.rootId, execution.executionId],
    );
    const history = new Map<string, PromiseRow>();
    for (const row of rows) {
      const promise = Db.rowToPromise(row);
      history.set(promise.key, promise);
    }
    return history;
  }

  async getPromises(
    execution: ExecutionRef,
    keys: string[],
  ): Promise<Map<string, PromiseRow>> {
    const { rows } = await this.client.query(
      `select id, key, null as label, kind, status, value, error, child_execution_id
         from otra.get_promises_local($1::uuid, $2::uuid, $3::uuid, $4::text[])`,
      [execution.queueId, execution.rootId, execution.executionId, keys],
    );
    const result = new Map<string, PromiseRow>();
    for (const row of rows) {
      const promise = Db.rowToPromise(row);
      result.set(promise.key, promise);
    }
    return result;
  }

  async recordRun(
    execution: ExecutionRef,
    workerId: string,
    key: string,
    label: string,
    value: unknown,
    claimSeconds: number,
  ): Promise<JsonValue> {
    const { rows } = await this.client.query(
      `select otra.record_run_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::text, $6::text, $7::jsonb, $8::double precision) as value`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, key, label, toJson(value), claimSeconds],
    );
    return (rows[0].value ?? null) as JsonValue;
  }

  async createSleep(
    execution: ExecutionRef,
    workerId: string,
    key: string,
    label: string,
    seconds: number,
  ): Promise<{ status: PromiseStatus }> {
    const { rows } = await this.client.query(
      `select status from otra.create_sleep_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::text, $6::text, $7::double precision)`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, key, label, seconds],
    );
    return { status: rows[0].status };
  }

  async createEventWait(
    execution: ExecutionRef,
    workerId: string,
    key: string,
    label: string,
    eventName: string,
    timeoutSeconds: number | null,
  ): Promise<{
    status: PromiseStatus;
    value: JsonValue;
    error: ErrorPayload | null;
  }> {
    const { rows } = await this.client.query(
      `select status, value, error from otra.create_event_wait_local(
         $1::uuid, $2::uuid, $3::uuid, $4::text, $5::text, $6::text,
         $7::text, $8::double precision
       )`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, key, label, eventName, timeoutSeconds],
    );
    return {
      status: rows[0].status,
      value: (rows[0].value ?? null) as JsonValue,
      error: (rows[0].error ?? null) as ErrorPayload | null,
    };
  }

  async recordCancel(
    execution: ExecutionRef,
    workerId: string,
    position: unknown,
  ): Promise<JsonValue> {
    const { rows } = await this.client.query(
      `select otra.record_cancel_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::jsonb) as position`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, toJson(position)],
    );
    return (rows[0].position ?? null) as JsonValue;
  }

  async createExternal(
    execution: ExecutionRef,
    workerId: string,
    key: string,
    label: string,
    timeoutSeconds: number | null,
  ): Promise<{
    id: string;
    status: PromiseStatus;
    value: JsonValue;
    error: ErrorPayload | null;
  }> {
    const { rows } = await this.client.query(
      `select id, status, value, error from otra.create_external_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::text, $6::text, $7::double precision)`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, key, label, timeoutSeconds],
    );
    return {
      id: rows[0].id,
      status: rows[0].status,
      value: (rows[0].value ?? null) as JsonValue,
      error: (rows[0].error ?? null) as ErrorPayload | null,
    };
  }

  async resolvePromise(token: string, value: unknown): Promise<boolean> {
    const address = parsePromiseToken(token);
    const { rows } = await this.client.query(
      `select otra.resolve_promise_local($1::uuid, $2::uuid, $3::uuid, $4::jsonb) as settled`,
      [address.queueId, address.rootId, address.promiseId, toJson(value)],
    );
    return rows[0].settled === true;
  }

  async rejectPromise(token: string, error: ErrorPayload): Promise<boolean> {
    const address = parsePromiseToken(token);
    const { rows } = await this.client.query(
      `select otra.reject_promise_local($1::uuid, $2::uuid, $3::uuid, $4::jsonb) as settled`,
      [address.queueId, address.rootId, address.promiseId, toJson(error)],
    );
    return rows[0].settled === true;
  }

  async suspend(
    execution: ExecutionRef,
    workerId: string,
    blockerKeys: string[],
  ): Promise<{ suspended: boolean; cancelRequested: boolean }> {
    const { rows } = await this.client.query(
      `select suspended, cancel_requested from otra.suspend_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::text[])`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, blockerKeys],
    );
    return {
      suspended: rows[0].suspended === true,
      cancelRequested: rows[0].cancel_requested === true,
    };
  }

  async complete(
    execution: ExecutionRef,
    workerId: string,
    result: unknown,
  ): Promise<void> {
    await this.client.query(`select otra.complete_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::jsonb)`, [
      execution.queueId,
      execution.rootId,
      execution.executionId,
      workerId,
      toJson(result),
    ]);
  }

  async failAttempt(
    execution: ExecutionRef,
    workerId: string,
    error: ErrorPayload,
    retryable: boolean,
  ): Promise<{
    applied: boolean;
    failedPermanently: boolean;
    retryAt: Date | null;
  }> {
    const { rows } = await this.client.query(
      `select applied, failed_permanently, retry_at from otra.fail_attempt_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::jsonb, $6::boolean)`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, toJson(error), retryable],
    );
    return {
      applied: rows[0]?.applied ?? false,
      failedPermanently: rows[0]?.failed_permanently ?? false,
      retryAt: rows[0]?.retry_at ?? null,
    };
  }

  async defer(
    execution: ExecutionRef,
    workerId: string,
    delaySeconds: number,
  ): Promise<boolean> {
    const { rows } = await this.client.query(
      `select otra.defer_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::double precision) as deferred`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, delaySeconds],
    );
    return rows[0].deferred;
  }

  async extendClaim(
    execution: ExecutionRef,
    workerId: string,
    claimSeconds: number,
  ): Promise<{ held: boolean; cancelRequested: boolean }> {
    const { rows } = await this.client.query(
      `select held, cancel_requested from otra.extend_claim_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::double precision)`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, claimSeconds],
    );
    return {
      held: rows[0]?.held === true,
      cancelRequested: rows[0]?.cancel_requested === true,
    };
  }

  async requestCancel(
    execution: ExecutionRef,
    cascade: boolean,
    reason: string | null,
  ): Promise<Array<{ executionId: string; action: string }>> {
    const { rows } = await this.client.query(
      `select execution_id, action from otra.request_cancel_local($1::uuid, $2::uuid, $3::uuid, $4::boolean, $5::text)`,
      [execution.queueId, execution.rootId, execution.executionId, cascade, reason],
    );
    return rows
      .filter((row: { action: string }) => row.action !== "noop")
      .map((row: { execution_id: string; action: string }) => ({
        executionId: row.execution_id,
        action: row.action,
      }));
  }

  async kill(
    execution: ExecutionRef,
    cascade: boolean,
    reason: string | null,
  ): Promise<number> {
    const { rows } = await this.client.query(
      `select otra.kill_local($1::uuid, $2::uuid, $3::uuid, $4::boolean, $5::text) as killed`,
      [execution.queueId, execution.rootId, execution.executionId, cascade, reason],
    );
    return rows[0].killed;
  }

  async finalizeCancelled(
    execution: ExecutionRef,
    workerId: string,
    error: ErrorPayload | null,
  ): Promise<boolean> {
    const { rows } = await this.client.query(
      `select otra.finalize_cancelled_local($1::uuid, $2::uuid, $3::uuid, $4::text, $5::jsonb) as finalized`,
      [execution.queueId, execution.rootId, execution.executionId, workerId, error === null ? null : toJson(error)],
    );
    return rows[0].finalized === true;
  }

  async emitEvent(
    queue: string,
    name: string,
    payload: unknown,
  ): Promise<void> {
    await this.client.query(`select otra.emit_event_local($1, $2, $3::jsonb)`, [
      queue,
      name,
      toJson(payload),
    ]);
  }

  async getExecution(execution: ExecutionRef): Promise<ExecutionSnapshot | null> {
    const { rows } = await this.client.query(
      `select * from otra.get_execution_local($1, $2, $3)`,
      [execution.queueId, execution.rootId, execution.executionId],
    );
    if (rows.length === 0) return null;
    const row = rows[0];
    return {
      id: row.id,
      queue: row.queue,
      functionName: row.function_name,
      status: row.status,
      attempt: row.attempt,
      params: row.params ?? null,
      result: row.result ?? null,
      error: row.error ?? null,
      parentId: row.parent_id ?? null,
      rootId: row.root_id ?? null,
      cancelRequestedAt: row.cancel_requested_at ?? null,
      cancelReason: row.cancel_reason ?? null,
      createdAt: row.created_at,
      finishedAt: row.finished_at ?? null,
    };
  }
}
