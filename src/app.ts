import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import pg from "pg";

import { Db, type Queryable } from "./db.ts";
import { Worker, type WorkerOptions } from "./worker.ts";
import {
  ExecutionFailedError,
  type ExecutionSnapshot,
  type RegisteredTask,
  type SpawnOptions,
  type TaskHandle,
  type TaskHandler,
  type TaskOptions,
} from "./types.ts";

export interface OtraOptions {
  /** Connection string, pg.Pool options, or an existing pool. */
  db?: string | pg.PoolConfig | pg.Pool;
  /** Default queue for tasks, spawns and workers. */
  queue?: string;
}

export interface GetResultOptions {
  /** Give up after this many milliseconds (default 30s). */
  timeoutMs?: number;
  /** Poll interval in milliseconds (default 25ms). */
  pollMs?: number;
}

function isPool(db: unknown): db is pg.Pool {
  return typeof (db as { query?: unknown })?.query === "function";
}

export class Otra {
  readonly queue: string;
  readonly registry = new Map<string, RegisteredTask>();
  readonly db: Db;
  private readonly pool: Queryable;
  private readonly ownedPool: pg.Pool | null;

  constructor(options: OtraOptions = {}) {
    this.queue = options.queue ?? "default";
    const db =
      options.db ??
      process.env.OTRA_DATABASE_URL ??
      process.env.PGDATABASE ??
      "postgresql://localhost/otra";
    if (isPool(db)) {
      this.pool = db;
      this.ownedPool = null;
    } else if (typeof db === "string") {
      this.ownedPool = new pg.Pool({ connectionString: db });
      this.pool = this.ownedPool;
    } else {
      this.ownedPool = new pg.Pool(db);
      this.pool = this.ownedPool;
    }
    this.db = new Db(this.pool);
  }

  /**
   * Register a durable function.  The returned handle carries the parameter
   * and result types for `spawn` / `ctx.spawn` / `ctx.call`.
   */
  task<P, R>(
    nameOrOptions: string | TaskOptions,
    handler: TaskHandler<P, R>,
  ): TaskHandle<P, R> {
    const options: TaskOptions =
      typeof nameOrOptions === "string"
        ? { name: nameOrOptions }
        : nameOrOptions;
    if (this.registry.has(options.name)) {
      throw new Error(`task "${options.name}" is already registered`);
    }
    this.registry.set(options.name, {
      name: options.name,
      queue: options.queue,
      maxAttempts: options.maxAttempts,
      retryStrategy: options.retryStrategy,
      handler: handler as RegisteredTask["handler"],
    });
    return { name: options.name };
  }

  /** Spawn a top-level execution; returns its id immediately. */
  async spawn<P, R>(
    task: TaskHandle<P, R> | string,
    params: P,
    options: SpawnOptions = {},
  ): Promise<{ executionId: string }> {
    const name = typeof task === "string" ? task : task.name;
    const registered = this.registry.get(name);
    const spawned = await this.db.spawn(
      name,
      params,
      options.queue ?? registered?.queue ?? this.queue,
      {
        maxAttempts: options.maxAttempts ?? registered?.maxAttempts,
        retryStrategy: options.retryStrategy ?? registered?.retryStrategy,
        delaySeconds: options.delaySeconds,
        idempotencyKey: options.idempotencyKey,
      },
    );
    return { executionId: spawned.executionId };
  }

  /**
   * Resolve one external promise by its token (from `ctx.promise`), waking
   * the execution awaiting it. Write-once: returns false if it was already
   * settled (or the token names anything other than an external promise).
   */
  async resolvePromise(token: string, value: unknown): Promise<boolean> {
    return this.db.resolvePromise(token, value);
  }

  /** Reject one external promise by its token; the await throws, catchably. */
  async rejectPromise(
    token: string,
    error: string | { message: string; name?: string },
  ): Promise<boolean> {
    const payload = typeof error === "string" ? { message: error } : error;
    return this.db.rejectPromise(token, payload);
  }

  /** Emit an event, resolving every pending wait for it on the queue. */
  async emitEvent(
    name: string,
    payload?: unknown,
    queue?: string,
  ): Promise<void> {
    await this.db.emitEvent(queue ?? this.queue, name, payload ?? null);
  }

  async getExecution(executionId: string): Promise<ExecutionSnapshot | null> {
    return this.db.getExecution(executionId);
  }

  /**
   * Poll until an execution reaches a terminal state and return its result.
   * Throws `ExecutionFailedError` on failure or cancellation.
   */
  async getResult<R = unknown>(
    executionId: string,
    options: GetResultOptions = {},
  ): Promise<R> {
    const timeoutMs = options.timeoutMs ?? 30_000;
    const pollMs = options.pollMs ?? 25;
    const deadline = Date.now() + timeoutMs;
    while (true) {
      const snapshot = await this.db.getExecution(executionId);
      if (snapshot === null) {
        throw new ExecutionFailedError(executionId, "failed", {
          message: "execution not found",
        });
      }
      if (snapshot.status === "completed") return snapshot.result as R;
      if (snapshot.status === "failed" || snapshot.status === "cancelled") {
        throw new ExecutionFailedError(
          executionId,
          snapshot.status,
          snapshot.error,
        );
      }
      if (Date.now() >= deadline) {
        throw new Error(`timed out waiting for execution ${executionId}`);
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  }

  /**
   * Graceful cancellation: records a request against the execution (and, by
   * default, its non-detached descendants) and gets a worker to deliver a
   * CancelledError into each generator, where catch/finally compensation can
   * run. Returns what happened per execution: 'cancelled' (finalized in
   * place), 'woken' (suspended, now replaying to deliver), or 'requested'
   * (running or mid-retry; the worker delivers it).
   */
  async cancel(
    executionId: string,
    options: { cascade?: boolean; reason?: string } = {},
  ): Promise<Array<{ executionId: string; action: string }>> {
    return this.db.requestCancel(
      executionId,
      options.cascade ?? true,
      options.reason ?? null,
    );
  }

  /**
   * The escape hatch: immediately terminate the execution (and, by default,
   * its non-detached descendants) with NO compensation. A worker mid-drive
   * discovers this at its next history write and abandons. Returns how many
   * executions were terminated.
   */
  async kill(
    executionId: string,
    options: { cascade?: boolean; reason?: string } = {},
  ): Promise<number> {
    return this.db.kill(
      executionId,
      options.cascade ?? true,
      options.reason ?? null,
    );
  }

  /** Create a worker bound to this app's registry (not started). */
  createWorker(options: WorkerOptions = {}): Worker {
    return new Worker(this.db, this.registry, {
      queue: this.queue,
      ...options,
    });
  }

  /** Create and start a polling worker. */
  startWorker(options: WorkerOptions = {}): Worker {
    const worker = this.createWorker(options);
    worker.start();
    return worker;
  }

  /** Apply sql/schema.sql to the connected database (idempotent). */
  async applySchema(): Promise<void> {
    await this.pool.query(Otra.schemaSql());
  }

  static schemaPath(): string {
    return path.join(
      path.dirname(path.dirname(fileURLToPath(import.meta.url))),
      "sql",
      "schema.sql",
    );
  }

  static schemaSql(): string {
    return fs.readFileSync(Otra.schemaPath(), "utf8");
  }

  async close(): Promise<void> {
    await this.ownedPool?.end();
  }
}
