import {
  CancelledError,
  DeterminismViolationError,
  type ChildSpawnOptions,
  type DurableHandle,
  type Effect,
  type ExternalPromise,
  type JsonConstraint,
  type JsonValue,
  type Op,
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
  let seconds: number;
  switch (match[2]) {
    case "ms":
      seconds = value / 1000;
      break;
    case "s":
      seconds = value;
      break;
    case "m":
      seconds = value * 60;
      break;
    case "h":
      seconds = value * 3600;
      break;
    default:
      seconds = value * 86400;
  }
  // Totality: a long enough digit run overflows Number() (309 nines, or
  // 305 with the day multiplier) and would sail through as Infinity --
  // the exact value the number branch above refuses. Found by fast-check.
  if (!Number.isFinite(seconds)) {
    throw new TypeError(`invalid duration: ${JSON.stringify(duration)}`);
  }
  return seconds;
}

function* op<T>(effect: Effect): Op<T> {
  return (yield effect) as T;
}

type RunResult<T> = [Awaited<T>] extends [void] ? null : Awaited<T>;
type RunReturnConstraint<T> = [Awaited<T>] extends [void]
  ? unknown
  : JsonConstraint<Awaited<T>>;

/**
 * The name/label string of a method whose *explicit* type argument decides
 * what gets read back out of the journal (`waitForEvent<T>`, `promise<T>`).
 * There is no value argument of type `T` to hang the JSON check on, and a
 * `T extends JsonCompatible<T>` type-parameter constraint is rejected by
 * TypeScript as circular -- so the check rides on the string argument, which
 * puts the error (and its reason) on the offending call itself.
 */
type JsonLabel<T> = string & JsonConstraint<T>;

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
   * Aborted when cancellation is requested, the execution is killed, or the
   * worker loses its claim -- pass it to fetch, child processes, etc. inside
   * ctx.run callbacks to stop in-flight I/O early.
   */
  readonly signal: AbortSignal;
  private readonly nowFn: () => number | Promise<number>;

  constructor(
    executionId: string,
    attempt: number,
    queue: string,
    signal?: AbortSignal,
    nowFn?: () => number | Promise<number>,
  ) {
    this.executionId = executionId;
    this.attempt = attempt;
    this.queue = queue;
    this.signal = signal ?? new AbortController().signal;
    this.nowFn = nowFn ?? (() => Date.now());
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
    return op({ type: "run", label: userLabel(label), fn });
  }

  /** Durable timer.  The execution suspends; a worker resumes it when due. */
  sleep(duration: number | string, label = "$sleep"): Op<void> {
    return op({
      type: "sleep",
      label: userLabel(label, "$sleep"),
      seconds: parseDuration(duration),
    });
  }

  /**
   * Suspend until an event with the given name is emitted on this queue.
   * Events are cached: emit-then-await and await-then-emit both resolve.
   * With a timeout, an `EventTimeoutError` is thrown into the task instead.
   */
  waitForEvent<T = JsonValue>(
    eventName: JsonLabel<T>,
    options: { timeout?: number | string; label?: string } = {},
  ): Op<T> {
    return op({
      type: "event",
      label:
        options.label === undefined
          ? `$event:${eventName}`
          : userLabel(options.label),
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
    options: ChildSpawnOptions & { label?: string } = {},
  ): Op<DurableHandle<R>> {
    const taskName = typeof task === "string" ? task : task.name;
    const { label, ...spawnOptions } = options;
    return op({
      type: "spawn",
      label: label === undefined ? `$spawn:${taskName}` : userLabel(label),
      taskName,
      params,
      options: spawnOptions,
    });
  }

  /**
   * Create an externally-settleable durable promise.  Returns a normal
   * handle (redeem with `ctx.await` / `ctx.all`) plus an opaque token
   * (`otr1_...`) to hand to the outside world; regular code settles exactly
   * this promise with `app.resolvePromise(token, value)` or
   * `app.rejectPromise(token, error)`.  With a timeout, the await throws a
   * catchable `TimeoutError` instead of waiting forever.
   */
  promise<T = JsonValue>(
    label: JsonLabel<T> = "$promise" as JsonLabel<T>,
    options: { timeout?: number | string } = {},
  ): Op<ExternalPromise<T>> {
    return op({
      type: "external",
      label: userLabel(label, "$promise"),
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
    options: ChildSpawnOptions & { label?: string } = {},
  ): Op<R> {
    const handle = yield* this.spawn(task, params, options);
    return yield* this.await(handle);
  }

  /**
   * Deterministic current time (epoch ms), memoized on first execution.
   * Reads the DATABASE clock (otra.now()), so it agrees with sleep timers,
   * timeouts, and the frozen test clock.
   */
  now(label = "$now"): Op<number> {
    return op({
      type: "run",
      label: userLabel(label, "$now"),
      fn: () => this.nowFn(),
    });
  }

  /** Deterministic random number in [0, 1), memoized on first execution. */
  random(label = "$random"): Op<number> {
    return op({
      type: "run",
      label: userLabel(label, "$random"),
      fn: () => Math.random(),
    });
  }

  /** Deterministic UUID, memoized on first execution. */
  uuid(label = "$uuid"): Op<string> {
    return op({
      type: "run",
      label: userLabel(label, "$uuid"),
      fn: () => crypto.randomUUID(),
    });
  }
}

/**
 * Labels starting with '$' are engine-reserved: they name the journal slots
 * the driver itself allocates ($sleep, $event:*, $spawn:*, $promise, $now,
 * $random, $uuid) and, critically, '$cancel' -- the cancellation-delivery
 * journal.  A user step squatting on '$cancel' would make the execution
 * permanently un-cancellable, so the whole namespace is rejected except for
 * the method's own engine default passed back verbatim.
 */
function userLabel(label: string, engineDefault?: string): string {
  if (label === engineDefault) return label;
  if (label.startsWith("$")) {
    throw new DeterminismViolationError(
      `label ${JSON.stringify(label)} uses the engine-reserved '$' prefix; pick another label`,
    );
  }
  return label;
}
