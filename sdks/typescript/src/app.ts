import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import pg from "pg";

import {
  Db,
  type Queryable,
  type CleanupOutcome,
  type DetachCandidate,
  type Queue,
  type QueuePolicy,
  type QueuePolicyOptions,
  type QueueStorageMode,
  type RetryOutcome,
} from "./db.ts";
import { WakeHub } from "./wake.ts";
import { Worker, type WorkerOptions } from "./worker.ts";
import {
  ExecutionFailedError,
  OtraError,
  TimeoutError,
  type ExecutionRef,
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

export interface CreateQueueOptions {
  storageMode?: QueueStorageMode;
}

export interface DropQueueOptions {
  /** Drop even while non-terminal executions remain. */
  force?: boolean;
}

export interface CleanupOptions {
  /** Postgres interval string, e.g. "30 days"; defaults to queue policy. */
  ttl?: string;
  /** Maximum rows swept per batch; defaults to queue policy. */
  limit?: number;
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
  private readonly workers = new Set<Worker>();
  /** LISTEN otra_wake hub; null when the db handle cannot LISTEN. */
  private readonly wakeHub: WakeHub | null;

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
    // LISTEN needs a dedicated connection; a bare Queryable can't provide
    // one, so listening quietly degrades to polling in that case.
    const listenPool =
      this.ownedPool ??
      (isPool(db) && typeof (db as pg.Pool).connect === "function"
        ? (db as pg.Pool)
        : null);
    this.wakeHub = listenPool === null ? null : new WakeHub(listenPool);
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
      maxAttempts: options.maxAttempts,
      retryStrategy: options.retryStrategy,
      handler: handler as RegisteredTask["handler"],
    });
    return { name: options.name };
  }

  /**
   * The schema version applied to the connected database: `"main"` for an
   * unreleased build, otherwise the release tag stamped into
   * `sql/schema.sql`. Until the first release there are no migrations --
   * upgrading is drop-and-reapply -- so this is purely a "which build is in
   * this database" handle.
   */
  async schemaVersion(): Promise<string> {
    return this.db.schemaVersion();
  }

  /** Provision a queue, defaulting to this app's queue. */
  async createQueue(
    name = this.queue,
    options: CreateQueueOptions = {},
  ): Promise<void> {
    await this.db.createQueue(name, options.storageMode);
  }

  /**
   * Drop a queue and all of its storage, defaulting to this app's queue.
   * Refuses while any execution is still non-terminal -- their workers would
   * hit a vanished table mid-replay -- unless `force` is set.
   */
  async dropQueue(
    name = this.queue,
    options: DropQueueOptions = {},
  ): Promise<void> {
    await this.db.dropQueue(name, options.force ?? false);
  }

  /** Return a provisioned queue, or null when it does not exist. */
  async getQueue(name = this.queue): Promise<Queue | null> {
    return this.db.getQueue(name);
  }

  /** List every provisioned queue in name order. */
  async listQueues(): Promise<Queue[]> {
    return this.db.listQueues();
  }

  /**
   * Extend partition windows for one queue, or every partitioned queue.
   * The multi-queue form runs one transaction per queue: a single
   * transaction would hold every queue's maintenance barrier at once,
   * blocking spawn/claim on queues whose DDL isn't even being touched.
   */
  async ensurePartitions(name?: string): Promise<void> {
    if (name !== undefined) {
      await this.db.ensurePartitions(name);
      return;
    }
    for (const queue of await this.listQueues()) {
      if (queue.storageMode === "partitioned") {
        await this.db.ensurePartitions(queue.name);
      }
    }
  }

  /** Update retention and partition lifecycle policy for a queue. */
  async setQueuePolicy(
    name: string,
    options: QueuePolicyOptions,
  ): Promise<void> {
    await this.db.setQueuePolicy(name, options);
  }

  /** Return the complete storage policy for a provisioned queue. */
  async getQueuePolicy(name = this.queue): Promise<QueuePolicy | null> {
    return this.db.getQueuePolicy(name);
  }

  /**
   * Retention sweep: deletes finished execution trees whose root settled
   * longer ago than `ttl`, and event facts older than it. Naming a queue
   * sweeps that one; omitting the name sweeps EVERY queue, each under its own
   * stored policy. `ttl` is a Postgres interval string ("30 days"), `limit`
   * bounds the batch; omitting either uses the queue's own policy. Run it on
   * a schedule -- nothing calls it for you.
   *
   * Returns one row per swept queue saying what it deleted. Read them: every
   * batch is bounded, so `rootsDeleted === limit` means the sweep saturated
   * its limit and there is more due work behind it -- call again until the
   * counts come back short, or retention quietly falls further behind on
   * every tick.
   */
  async cleanup(
    name?: string,
    options: CleanupOptions = {},
  ): Promise<CleanupOutcome[]> {
    return this.db.cleanup(
      name ?? null,
      options.ttl ?? null,
      options.limit ?? null,
    );
  }

  /** List old empty partitions eligible for an operator-managed detach. */
  async listDetachCandidates(name?: string): Promise<DetachCandidate[]> {
    return this.db.listDetachCandidates(name);
  }

  /**
   * Drop a week partition you have already detached, completing the detach
   * flow that {@link listDetachCandidates} starts. The DETACH itself stays
   * yours to run (PostgreSQL cannot do it concurrently from inside a
   * function); this only reclaims the storage afterwards.
   *
   * Every gate is an error, never a silent no-op: the relation must exist in
   * the otra schema, be named like a partition otra generates, and actually
   * be detached. Catch them with `isNotFound` / `isPreconditionFailed`.
   */
  async dropDetachedPartition(table: string): Promise<void> {
    await this.db.dropDetachedPartition(table);
  }

  /**
   * Spawn a top-level execution; returns its queue-local address immediately.
   *
   * `created` says whether this call is what put the execution there. It is
   * only ever false for an `idempotencyKey` spawn that lost -- the address
   * returned is then the winner's, unchanged, and none of this call's
   * arguments were used. Cron and webhook callers need the distinction to
   * tell "scheduled" from "already scheduled"; everything else can ignore it,
   * since the field is additive and existing destructuring is unaffected.
   */
  async spawn<P, R>(
    task: TaskHandle<P, R> | string,
    params: P,
    options: SpawnOptions = {},
  ): Promise<ExecutionRef & { created: boolean }> {
    const name = typeof task === "string" ? task : task.name;
    const registered = this.registry.get(name);
    if (registered === undefined && options.queue === undefined) {
      // A typo'd name would otherwise spawn with SQL defaults onto this
      // app's queue and cycle in the unknown-function defer loop with zero
      // diagnostics (absurd's a339dee lesson). An explicit queue marks a
      // deliberate cross-process spawn of a task registered elsewhere.
      throw new OtraError(
        `task "${name}" is not registered on this app; pass { queue } ` +
          `explicitly to spawn a task registered in another process`,
      );
    }
    const spawned = await this.db.spawn(
      name,
      params,
      options.queue ?? this.queue,
      {
        maxAttempts: options.maxAttempts ?? registered?.maxAttempts,
        retryStrategy: options.retryStrategy ?? registered?.retryStrategy,
        delaySeconds: options.delaySeconds,
        deadlines: options.deadlines,
        idempotencyKey: options.idempotencyKey,
        onParentCancel: options.onParentCancel,
      },
    );
    return {
      queueId: spawned.queueId,
      rootId: spawned.rootId,
      executionId: spawned.executionId,
      created: spawned.created,
    };
  }

  /**
   * Resolve one external promise by its token (from `ctx.promise`), waking
   * the execution awaiting it. Write-once: returns false if it was already
   * settled. Throws `OtraError` if the token names anything other than an
   * external promise -- an internal journal row, or nothing at all -- since
   * that is a programming error rather than a lost race.
   */
  async resolvePromise(token: string, value: unknown): Promise<boolean> {
    return this.db.resolvePromise(token, value);
  }

  /**
   * Reject one external promise by its token; the await throws, catchably.
   * Same outcomes as {@link resolvePromise}: false when already settled,
   * `OtraError` when the token names no external promise.
   */
  async rejectPromise(
    token: string,
    error: string | { message: string; name?: string },
  ): Promise<boolean> {
    const payload = typeof error === "string" ? { message: error } : error;
    return this.db.rejectPromise(token, payload);
  }

  /**
   * Emit an event, resolving every pending wait for it on the queue.  An
   * event name is an immutable one-shot fact: returns true when this call
   * created it, false when the name was already a fact -- in which case a
   * repeat emit with a DIFFERENT payload changed nothing, waiters keep seeing
   * the first payload.
   */
  async emitEvent(
    name: string,
    payload?: unknown,
    queue?: string,
  ): Promise<boolean> {
    return this.db.emitEvent(queue ?? this.queue, name, payload ?? null);
  }

  async getExecution(
    execution: ExecutionRef,
  ): Promise<ExecutionSnapshot | null> {
    return this.db.getExecution(execution);
  }

  /**
   * Poll until an execution reaches a terminal state and return its result.
   * Throws `ExecutionFailedError` on failure or cancellation.
   */
  async getResult<R = unknown>(
    execution: ExecutionRef,
    options: GetResultOptions = {},
  ): Promise<R> {
    const timeoutMs = options.timeoutMs ?? 30_000;
    const pollMs = options.pollMs ?? 25;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new TypeError(`invalid timeoutMs: ${timeoutMs}`);
    }
    if (!Number.isFinite(pollMs) || pollMs <= 0) {
      throw new TypeError(`invalid pollMs: ${pollMs}`);
    }
    const deadline = Date.now() + timeoutMs;
    // Exponential backoff from pollMs to a 1s ceiling, clamped to the
    // remaining budget: a flat 25ms interval against a 30s timeout was up
    // to 1200 round trips per wait (an absurd lesson, their 7def1b9).
    // Terminal transitions NOTIFY, so when the app can LISTEN the backoff
    // sleep is cut short the instant anything on the database moves --
    // `woken` closes the race between polling and parking.
    let interval = pollMs;
    let woken = false;
    let wakeNow: (() => void) | null = null;
    // Filter wakes by queue name once the first snapshot reveals it: a busy
    // installation notifies constantly, and R notifications/sec times G
    // waiters of unfiltered wakes is pure amplification.
    let queueName: string | null = null;
    const unsubscribe = this.wakeHub?.subscribe((queue) => {
      if (queue !== null && queueName !== null && queue !== queueName) return;
      woken = true;
      wakeNow?.();
    });
    try {
      while (true) {
        const wasWoken = woken;
        woken = false;
        if (wasWoken) {
          // Coalesce notification bursts: at most one wake-triggered poll
          // per ~20ms, whatever the queue's commit rate.
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
        const snapshot = await this.db.getExecution(execution);
        if (queueName === null && snapshot !== null) {
          queueName = snapshot.queue;
        }
        const executionId = execution.executionId;
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
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          throw new TimeoutError(
            `timed out waiting for execution ${executionId}`,
          );
        }
        if (!woken) {
          await new Promise<void>((resolve) => {
            wakeNow = resolve;
            if (woken) {
              resolve();
              return;
            }
            setTimeout(resolve, Math.min(interval, remaining));
          });
          wakeNow = null;
        }
        interval = Math.min(interval * 2, 1000);
      }
    } finally {
      unsubscribe?.();
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
    execution: ExecutionRef,
    options: { cascade?: boolean; reason?: string } = {},
  ): Promise<Array<{ executionId: string; action: string }>> {
    return this.db.requestCancel(
      execution,
      options.cascade ?? true,
      options.reason ?? null,
    );
  }

  /**
   * Operator retry of a permanently-failed execution: resume it in place,
   * from where it failed. The journal is kept, so replay fast-forwards
   * through every settled step -- only the work after the failure point runs
   * again. `maxAttempts` overrides the attempt budget; by default one more
   * attempt is granted.
   *
   * ROOT-ONLY: retrying a child is refused, because its parent has already
   * observed the write-once child promise reject -- retry the root instead.
   * Only 'failed' executions are retryable: 'cancelled' is terminal
   * (cancellation owns its outcome, and compensation already ran) and
   * 'completed' has nothing to redo.
   */
  async retry(
    execution: ExecutionRef,
    options: { maxAttempts?: number } = {},
  ): Promise<RetryOutcome> {
    return this.db.retry(execution, options.maxAttempts ?? null);
  }

  /**
   * The escape hatch: immediately terminate the execution (and, by default,
   * its non-detached descendants) with NO compensation. A worker mid-drive
   * discovers this at its next history write and abandons. Returns how many
   * executions were terminated.
   */
  async kill(
    execution: ExecutionRef,
    options: { cascade?: boolean; reason?: string } = {},
  ): Promise<number> {
    return this.db.kill(
      execution,
      options.cascade ?? true,
      options.reason ?? null,
    );
  }

  /** Create a worker bound to this app's registry (not started). */
  createWorker(options: WorkerOptions = {}): Worker {
    const worker = new Worker(this.db, this.registry, {
      queue: this.queue,
      wake: this.wakeHub ?? undefined,
      ...options,
    });
    this.workers.add(worker);
    return worker;
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

  /**
   * Stop every worker this app created (draining their in-flight drives)
   * BEFORE ending an owned pool -- ending the pool first would yank the
   * connection out from under running executions, stranding their claims
   * for a full lease timeout (an absurd lesson: their close() drains too).
   */
  async close(): Promise<void> {
    await Promise.all([...this.workers].map((worker) => worker.stop()));
    this.workers.clear();
    this.wakeHub?.close();
    await this.ownedPool?.end();
  }
}
