/**
 * Core types for otra, a generator-based durable execution engine on
 * Postgres.
 *
 * Durable functions are written as generator functions.  Every suspension
 * point is expressed with `yield*` on a context method, which yields a plain
 * data `Effect` to the replay driver.  The driver interprets the effect
 * against the execution's durable promise history: memoized results are
 * injected back into the generator, unresolved remote promises park the
 * execution.  Because the driver -- not user code -- owns every await,
 * suspension is a first-class transfer of control instead of a thrown
 * sentinel exception that user `catch` blocks can accidentally swallow.
 */

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/** Serialized error payload stored in Postgres. */
export interface ErrorPayload {
  name?: string;
  message: string;
  stack?: string;
  [key: string]: JsonValue | undefined;
}

/** Retry configuration, interpreted by the database (see otra._backoff). */
export interface RetryStrategy {
  kind?: "exponential" | "fixed";
  base_s?: number;
  factor?: number;
  max_s?: number;
}

interface CommonSpawnOptions {
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
  /** Delay initial execution by this many seconds. */
  delaySeconds?: number;
  /**
   * What a parent's graceful cancel does to this child: 'cascade' (default)
   * cancels it too; 'detach' lets it run to completion (fire-and-forget
   * audit trails, notifications).
   */
  onParentCancel?: "cascade" | "detach";
}

export interface SpawnOptions extends CommonSpawnOptions {
  queue?: string;
  /** At-most-one top-level execution per (queue, key). */
  idempotencyKey?: string;
}

export type ChildSpawnOptions = CommonSpawnOptions;

/** Complete physical address of an execution in queue-local storage. */
export interface ExecutionRef {
  readonly queueId: string;
  readonly rootId: string;
  readonly executionId: string;
}

/**
 * A registered durable function.  Purely a typed name; passing it to
 * `ctx.spawn` / `ctx.call` / `app.spawn` carries the parameter and result
 * types along.
 */
export interface TaskHandle<P = unknown, R = unknown> {
  readonly name: string;
  /** phantom */ readonly __params?: P;
  /** phantom */ readonly __result?: R;
}

/**
 * A reference to a durable promise (today: always a child execution's result
 * promise).  Obtained from `yield* ctx.spawn(...)`; redeemed with
 * `yield* ctx.await(...)` or `yield* ctx.all([...])`.  Handles are
 * replay-stable: on re-execution the same handle is reconstructed from the
 * memoized promise history.
 */
export interface DurableHandle<R = unknown> {
  readonly kind: "otra:handle";
  /** Promise key within the owning execution's history. */
  readonly key: string;
  /** The child execution this promise tracks. */
  readonly executionId: string;
  /** phantom */ readonly __result?: R;
}

export function isDurableHandle(value: unknown): value is DurableHandle {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { kind?: unknown }).kind === "otra:handle"
  );
}

/**
 * A handle to an externally-settleable promise (from `ctx.promise`), plus
 * the opaque token (`otr1_...`) the task hands to the outside world so that
 * `app.resolvePromise` / `app.rejectPromise` can settle exactly this one.
 * Redeemed like any other handle, with `ctx.await` / `ctx.all`.
 */
export interface ExternalPromise<R = unknown> extends DurableHandle<R> {
  readonly token: string;
}

// ---------------------------------------------------------------------------
// Effects: the values yielded by durable functions to the driver.
// ---------------------------------------------------------------------------

export interface RunEffect {
  type: "run";
  label: string;
  fn: () => unknown;
}

export interface SleepEffect {
  type: "sleep";
  label: string;
  seconds: number;
}

export interface EventEffect {
  type: "event";
  label: string;
  eventName: string;
  timeoutSeconds: number | null;
}

export interface SpawnEffect {
  type: "spawn";
  label: string;
  taskName: string;
  params: unknown;
  options: ChildSpawnOptions;
}

export interface AwaitEffect {
  type: "await";
  handles: DurableHandle[];
  /** true: inject the sole handle's value; false: inject an array. */
  single: boolean;
}

/** Creates an externally-settleable promise (ctx.promise). */
export interface ExternalEffect {
  type: "external";
  label: string;
  timeoutSeconds: number | null;
}

/** Enters/exits a ctx.uninterruptible section (defers cancel delivery). */
export interface ShieldEffect {
  type: "shield";
  on: boolean;
}

export type Effect =
  | RunEffect
  | SleepEffect
  | EventEffect
  | SpawnEffect
  | ExternalEffect
  | AwaitEffect
  | ShieldEffect;

/** The generator type produced by every `ctx.*` method. */
export type Op<T> = Generator<Effect, T, unknown>;

/** A durable function body. */
export type TaskHandler<P, R> = (
  params: P,
  ctx: import("./context.ts").Ctx,
) => Generator<Effect, R, unknown>;

export interface TaskOptions {
  name: string;
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
}

export interface RegisteredTask {
  name: string;
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
  handler: TaskHandler<never, unknown>;
}

// ---------------------------------------------------------------------------
// Execution snapshots
// ---------------------------------------------------------------------------

export type ExecutionStatus =
  | "pending"
  | "running"
  | "suspended"
  | "completed"
  | "failed"
  | "cancelled";

export interface ExecutionSnapshot {
  id: string;
  queue: string;
  functionName: string;
  status: ExecutionStatus;
  attempt: number;
  params: JsonValue;
  result: JsonValue;
  error: ErrorPayload | null;
  parentId: string | null;
  rootId: string | null;
  cancelRequestedAt: Date | null;
  cancelReason: string | null;
  createdAt: Date;
  finishedAt: Date | null;
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class OtraError extends Error {}

/**
 * Replay diverged from the recorded promise history (a promise key was
 * reused with a different kind or label).  Almost always caused by
 * non-deterministic control flow or a code change that reordered steps.
 * Non-retryable: replaying cannot help.
 */
export class DeterminismViolationError extends OtraError {
  constructor(message: string) {
    super(message);
    this.name = "DeterminismViolationError";
  }
}

/** An awaited child execution failed permanently (or was cancelled). */
export class ChildFailedError extends OtraError {
  readonly executionId: string;
  readonly errorPayload: ErrorPayload;

  constructor(executionId: string, errorPayload: ErrorPayload) {
    super(
      `child execution ${executionId} failed: ${errorPayload?.message ?? "unknown error"}`,
    );
    this.name = "ChildFailedError";
    this.executionId = executionId;
    this.errorPayload = errorPayload;
  }
}

/** A durable wait timed out. Catchable inside the durable function. */
export class TimeoutError extends OtraError {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

/** An event wait timed out. Catchable inside the durable function. */
export class EventTimeoutError extends TimeoutError {
  constructor(message: string) {
    super(message);
    this.name = "EventTimeoutError";
  }
}

/**
 * Thrown into a durable function when a graceful cancel is delivered.
 * Catchable: `catch`/`finally` may run local `ctx.run` compensation steps,
 * which checkpoint normally. Whatever the generator does next -- return,
 * rethrow, or throw something else -- the engine finalizes the execution to
 * 'cancelled'.
 */
export class CancelledError extends OtraError {
  readonly retryable = false;

  constructor(message = "execution was cancelled") {
    super(message);
    this.name = "CancelledError";
  }
}

/**
 * True for a delivered `CancelledError` and for a `ChildFailedError` whose
 * cause was the child's cancellation (name mirrors Temporal's helper).
 */
export function isCancellation(err: unknown): boolean {
  if (err instanceof CancelledError) return true;
  if (err instanceof ChildFailedError) {
    return err.errorPayload?.name === "CancelledError";
  }
  return false;
}

/** Thrown by client-side result helpers when an execution ends badly. */
export class ExecutionFailedError extends OtraError {
  readonly executionId: string;
  readonly status: ExecutionStatus;
  readonly errorPayload: ErrorPayload | null;

  constructor(
    executionId: string,
    status: ExecutionStatus,
    errorPayload: ErrorPayload | null,
  ) {
    super(
      `execution ${executionId} ${status}: ${errorPayload?.message ?? "no error recorded"}`,
    );
    this.name = "ExecutionFailedError";
    this.executionId = executionId;
    this.status = status;
    this.errorPayload = errorPayload;
  }
}

/** Wrap an error to force a specific retryable classification. */
export class TaskError extends OtraError {
  readonly retryable: boolean;

  constructor(message: string, retryable: boolean) {
    super(message);
    this.name = "TaskError";
    this.retryable = retryable;
  }
}

export function serializeError(err: unknown): ErrorPayload {
  if (err instanceof Error) {
    const payload: ErrorPayload = { name: err.name, message: err.message };
    if (err.stack) payload.stack = err.stack;
    return payload;
  }
  return { message: String(err) };
}
