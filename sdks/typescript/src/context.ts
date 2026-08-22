import {
  CancelledError,
  type DurableHandle,
  type Effect,
  type ExternalPromise,
  type JsonValue,
  type Op,
  type SpawnOptions,
  type TaskHandle,
} from "./types.ts";

/** Map a tuple of handles to the tuple of their result types. */
export type HandleResults<T extends readonly DurableHandle<unknown>[]> = {
  -readonly [K in keyof T]: T[K] extends DurableHandle<infer R> ? R : never;
};

/** Parse "500ms" | "10s" | "5m" | "2h" | "1d" | number-of-seconds. */
export function parseDuration(duration: number | string): number {
  if (typeof duration === "number") {
    if (!Number.isFinite(duration) || duration < 0) {
      throw new TypeError(`invalid duration: ${duration}`);
    }
    return duration;
  }
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/.exec(duration.trim());
  if (!match)
    throw new TypeError(`invalid duration: ${JSON.stringify(duration)}`);
  const value = Number(match[1]);
  switch (match[2]) {
    case "ms":
      return value / 1000;
    case "s":
      return value;
    case "m":
      return value * 60;
    case "h":
      return value * 3600;
    default:
      return value * 86400;
  }
}

function* op<T>(effect: Effect): Op<T> {
  return (yield effect) as T;
}

type RunResult<T> = [Awaited<T>] extends [void] ? null : Awaited<T>;
type RunReturnConstraint<T> = [Awaited<T>] extends [void]
  ? unknown
  : Awaited<T> extends JsonValue
    ? unknown
    : never;

/**
 * The durable context passed to every task handler.  All methods return
 * generators and must be consumed with `yield*`; the yielded effects are
 * interpreted by the replay driver against the execution's durable promise
 * history.
 */
export class Ctx {
  /** The current execution's id (stable across retries and resumes). */
  readonly executionId: string;
  /** How many attempts failed before this one (0 on the first attempt). */
  readonly attempt: number;
  /** The queue this execution runs on. */
  readonly queue: string;
  /**
   * True once a graceful cancel has been requested for this execution
   * (discovered via the claim or a heartbeat). Readable synchronously from
   * long loops; the driver delivers a CancelledError at the next yield.
   */
  cancelRequested = false;
  /**
   * Aborted when cancellation is requested -- pass it to fetch, child
   * processes, etc. inside ctx.run callbacks to stop in-flight I/O early.
   */
  readonly signal: AbortSignal;

  constructor(
    executionId: string,
    attempt: number,
    queue: string,
    signal?: AbortSignal,
  ) {
    this.executionId = executionId;
    this.attempt = attempt;
    this.queue = queue;
    this.signal = signal ?? new AbortController().signal;
  }

  /** Throw CancelledError now if a cancel is pending (for long loops). */
  throwIfCancelled(): void {
    if (this.cancelRequested) {
      throw new CancelledError("cancel requested");
    }
  }

  /**
   * Run a critical section that must not be interrupted by cancellation:
   * delivery of CancelledError is deferred until the body exits. This is the
   * forward-direction shield (Temporal's `nonCancellable`); note that plain
   * catch/finally compensation does NOT need it in otra.
   */
  *uninterruptible<T>(body: () => Generator<Effect, T, unknown>): Op<T> {
    yield { type: "shield", on: true };
    try {
      return yield* body();
    } finally {
      yield { type: "shield", on: false };
    }
  }

  /**
   * Checkpoint a local side effect.  `fn` runs at most once per recorded
   * result: on replay the memoized value is injected without re-executing.
   * Results must be JSON values because they are persisted in Postgres and
   * injected from that serialized history on replay. A callback with no
   * return value is persisted and typed as null. If `fn` throws, the current
   * attempt fails and the whole task retries (already-checkpointed steps are
   * skipped on the next attempt).
   */
  run<T>(
    label: string,
    fn: () => T & RunReturnConstraint<T>,
  ): Op<RunResult<T>> {
    return op({ type: "run", label, fn });
  }

  /** Durable timer.  The execution suspends; a worker resumes it when due. */
  sleep(duration: number | string, label = "$sleep"): Op<void> {
    return op({ type: "sleep", label, seconds: parseDuration(duration) });
  }

  /**
   * Suspend until an event with the given name is emitted on this queue.
   * Events are cached: emit-then-await and await-then-emit both resolve.
   * With a timeout, an `EventTimeoutError` is thrown into the task instead.
   */
  waitForEvent<T = JsonValue>(
    eventName: string,
    options: { timeout?: number | string; label?: string } = {},
  ): Op<T> {
    return op({
      type: "event",
      label: options.label ?? `$event:${eventName}`,
      eventName,
      timeoutSeconds:
        options.timeout === undefined ? null : parseDuration(options.timeout),
    });
  }

  /**
   * Spawn a child execution and get back a durable handle to its result
   * promise.  Replay-safe: re-executions reuse the recorded child instead of
   * spawning a duplicate.  The child is independent -- the parent may await
   * it (`ctx.await` / `ctx.all`), or never look at it again.
   */
  spawn<P, R>(
    task: TaskHandle<P, R> | string,
    params: P,
    options: SpawnOptions & { label?: string } = {},
  ): Op<DurableHandle<R>> {
    const taskName = typeof task === "string" ? task : task.name;
    const { label, ...spawnOptions } = options;
    return op({
      type: "spawn",
      label: label ?? `$spawn:${taskName}`,
      taskName,
      params,
      options: spawnOptions,
    });
  }

  /**
   * Create an externally-settleable durable promise.  Returns a normal
   * handle (redeem with `ctx.await` / `ctx.all`) plus an opaque token
   * (`otr_...`) to hand to the outside world; regular code settles exactly
   * this promise with `app.resolvePromise(token, value)` or
   * `app.rejectPromise(token, error)`.  With a timeout, the await throws a
   * catchable `TimeoutError` instead of waiting forever.
   */
  promise<T = JsonValue>(
    label = "$promise",
    options: { timeout?: number | string } = {},
  ): Op<ExternalPromise<T>> {
    return op({
      type: "external",
      label,
      timeoutSeconds:
        options.timeout === undefined ? null : parseDuration(options.timeout),
    });
  }

  /**
   * Await a durable promise.  If it is unresolved the execution suspends --
   * releasing its worker slot entirely -- and wakes when the child settles.
   * A failed child surfaces as a `ChildFailedError` thrown at this point.
   */
  await<R>(handle: DurableHandle<R>): Op<R> {
    return op({ type: "await", handles: [handle], single: true });
  }

  /** Await several durable promises; resolves to their results in order. */
  all<T extends readonly DurableHandle<unknown>[]>(
    handles: readonly [...T],
  ): Op<HandleResults<T>> {
    return op({ type: "await", handles: [...handles], single: false });
  }

  /** Spawn a child execution and await its result (spawn + await). */
  *call<P, R>(
    task: TaskHandle<P, R> | string,
    params: P,
    options: SpawnOptions & { label?: string } = {},
  ): Op<R> {
    const handle = yield* this.spawn(task, params, options);
    return yield* this.await(handle);
  }

  /** Deterministic current time (epoch ms), memoized on first execution. */
  now(label = "$now"): Op<number> {
    return this.run(label, () => Date.now());
  }

  /** Deterministic random number in [0, 1), memoized on first execution. */
  random(label = "$random"): Op<number> {
    return this.run(label, () => Math.random());
  }

  /** Deterministic UUID, memoized on first execution. */
  uuid(label = "$uuid"): Op<string> {
    return this.run(label, () => crypto.randomUUID());
  }
}
