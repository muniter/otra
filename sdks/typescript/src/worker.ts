import * as os from "node:os";

import type { Db } from "./db.ts";
import { driveOnce, type DriveOutcome } from "./driver.ts";
import { serializeError, type RegisteredTask } from "./types.ts";

export interface WorkerOptions {
  queue?: string;
  workerId?: string;
  /** Claim lease in seconds; extended by checkpoints and heartbeats. */
  claimSeconds?: number;
  /** Maximum executions driven concurrently by start() (default 5). */
  concurrency?: number;
  /** How many executions to claim per poll (default: concurrency). */
  batchSize?: number;
  /** Poll interval in milliseconds when the queue is empty. */
  pollIntervalMs?: number;
  onError?: (err: unknown) => void;
}

/** Safety bound on immediate replays after a refused suspension. */
const MAX_REDRIVES = 100;

const UNKNOWN_TASK_DEFER_BASE_SECONDS = 15;
const UNKNOWN_TASK_DEFER_JITTER_SECONDS = 15;

/** FNV-1a of the seed, modulo the window: stable jitter, no randomness. */
function deterministicJitterSeconds(seed: string, window: number): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < seed.length; i++) {
    hash ^= seed.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash % window;
}

/**
 * A polling worker.  `tick()` claims and drives one batch (useful for
 * deterministic tests); `start()` polls forever until `stop()`.
 */
export class Worker {
  readonly queue: string;
  readonly workerId: string;
  private readonly claimSeconds: number;
  private readonly concurrency: number;
  private readonly batchSize: number;
  private readonly pollIntervalMs: number;
  private readonly onError: (err: unknown) => void;
  private running = false;
  private loop: Promise<void> | null = null;
  private wake: (() => void) | null = null;
  private readonly inflight = new Set<Promise<void>>();

  private readonly db: Db;
  private readonly registry: Map<string, RegisteredTask>;

  constructor(
    db: Db,
    registry: Map<string, RegisteredTask>,
    options: WorkerOptions = {},
  ) {
    this.db = db;
    this.registry = registry;
    this.queue = options.queue ?? "default";
    this.workerId = options.workerId ?? `${os.hostname()}:${process.pid}`;
    this.claimSeconds = options.claimSeconds ?? 30;
    this.concurrency = options.concurrency ?? 5;
    this.batchSize = options.batchSize ?? this.concurrency;
    this.pollIntervalMs = options.pollIntervalMs ?? 250;
    this.onError =
      options.onError ?? ((err) => console.error("otra worker error:", err));
  }

  /**
   * Claim one batch and drive every claimed execution to a stopping point
   * (completed, suspended, failed, or lost).  Returns the number of
   * executions processed.
   */
  async tick(): Promise<number> {
    const claimed = await this.db.claim(
      this.queue,
      this.workerId,
      this.claimSeconds,
      this.batchSize,
    );
    await Promise.all(claimed.map((execution) => this.drive(execution)));
    return claimed.length;
  }

  /** Repeatedly tick until the queue has nothing runnable right now. */
  async drain(): Promise<void> {
    while ((await this.tick()) > 0) {
      /* keep going */
    }
  }

  private async drive(
    execution: Awaited<ReturnType<Db["claim"]>>[number],
  ): Promise<DriveOutcome | undefined> {
    const registered = this.registry.get(execution.functionName);
    if (registered === undefined) {
      // Likely a rolling deploy: another worker knows this function.  The
      // delay is jittered DETERMINISTICALLY per execution (absurd's
      // deferClaimedRun) so a fleet deferring the same batch doesn't
      // re-collide on the same instant forever, while staying stable per
      // execution across workers.
      await this.db.defer(
        execution,
        this.workerId,
        UNKNOWN_TASK_DEFER_BASE_SECONDS +
          deterministicJitterSeconds(
            execution.executionId,
            UNKNOWN_TASK_DEFER_JITTER_SECONDS,
          ),
      );
      return undefined;
    }
    try {
      let outcome = await driveOnce(this.db, registered, execution, {
        queue: this.queue,
        workerId: this.workerId,
        claimSeconds: this.claimSeconds,
        registry: this.registry,
      });
      let redrives = 0;
      while (outcome.type === "redrive") {
        if (++redrives > MAX_REDRIVES) {
          throw new Error(
            `execution ${execution.executionId} exceeded ${MAX_REDRIVES} immediate replays`,
          );
        }
        outcome = await driveOnce(this.db, registered, execution, {
          queue: this.queue,
          workerId: this.workerId,
          claimSeconds: this.claimSeconds,
          registry: this.registry,
        });
      }
      return outcome;
    } catch (err) {
      // An error that escapes the driver (infrastructure failure, redrive
      // exhaustion) must not strand the execution as "running" with a live
      // claim -- that would cost a full lease timeout to recover.  Record a
      // failed attempt; if our claim is already gone this is a guarded no-op.
      this.onError(err);
      try {
        await this.db.failAttempt(
          execution,
          this.workerId,
          serializeError(err),
          true,
        );
      } catch {
        /* claim already lost; the sweep will recover it */
      }
      return undefined;
    }
  }

  /**
   * Slot-based polling loop: claimed executions run concurrently up to
   * `concurrency`, and a finishing execution frees its slot immediately --
   * one slow step never blocks the worker from claiming other work.
   */
  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = (async () => {
      while (this.running) {
        let claimed = 0;
        const free = this.concurrency - this.inflight.size;
        if (free > 0) {
          try {
            const batch = await this.db.claim(
              this.queue,
              this.workerId,
              this.claimSeconds,
              Math.min(free, this.batchSize),
            );
            claimed = batch.length;
            for (const execution of batch) {
              const task: Promise<void> = this.drive(execution)
                .then(
                  () => {},
                  (err) => this.onError(err),
                )
                .finally(() => {
                  this.inflight.delete(task);
                  this.wake?.();
                });
              this.inflight.add(task);
            }
          } catch (err) {
            this.onError(err);
          }
        }
        if (!this.running) break;
        if (claimed === 0) {
          await new Promise<void>((resolve) => {
            this.wake = resolve;
            const timer = setTimeout(resolve, this.pollIntervalMs);
            timer.unref?.();
          });
          this.wake = null;
        }
      }
      await Promise.allSettled([...this.inflight]);
    })();
  }

  /** Nudge a sleeping poll loop (e.g. after a local spawn). */
  notify(): void {
    this.wake?.();
  }

  /** Stop claiming and wait for in-flight executions to finish. */
  async stop(): Promise<void> {
    this.running = false;
    this.wake?.();
    await this.loop;
    this.loop = null;
  }
}
