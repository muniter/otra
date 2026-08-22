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

// Type-only, so it is erased at runtime and creates no import cycle with
// context.ts (which imports this module for real).
import type { Ctx } from "./context.ts";

export type JsonValue =
  string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

/**
 * The shape a value degrades to when it cannot survive a JSON round trip.
 * It exists only to put a readable reason into the type error: an offending
 * value is reported as not assignable to
 * `NotJsonSerializable<"a Date deserializes as a string; ...">` instead of
 * the opaque `not assignable to type 'never'`.
 */
export interface NotJsonSerializable<Reason extends string = string> {
  readonly __otraNotJsonSerializable: Reason;
}

/**
 * Structural test that `T` survives the JSON round trip Postgres imposes on
 * every persisted value (task params and results, `ctx.run` checkpoints,
 * event payloads, externally-settled promises).
 *
 * `T extends JsonValue` alone is not usable as that test: TypeScript grants
 * an implicit index signature to *type aliases* only, so every `interface`
 * fails it -- the very shape most codebases are written in.  Recursing
 * structurally accepts interfaces, optional properties and arrays of them,
 * while still rejecting values whose shape changes on the way back
 * (`Date` -> string, `Map`/`Set` -> `{}`, functions and symbols -> gone,
 * `bigint` -> a thrown TypeError).
 *
 * `undefined` and `void` are allowed on purpose: they persist as null, which
 * is the established runtime behavior.
 */
export type JsonCompatible<T> = unknown extends T
  ? T // `any` / `unknown`: nothing to check against
  : T extends JsonValue
    ? T
    : T extends undefined | void
      ? T
      : T extends readonly (infer Element)[]
        ? readonly JsonCompatible<Element>[]
        : T extends Date
          ? NotJsonSerializable<"a Date deserializes as a string; persist an ISO string or epoch millis">
          : T extends Map<unknown, unknown> | Set<unknown>
            ? NotJsonSerializable<"a Map/Set serializes to {}; persist an array or a plain object">
            : T extends (...args: never[]) => unknown
              ? NotJsonSerializable<"a function cannot be persisted">
              : T extends symbol
                ? NotJsonSerializable<"a symbol cannot be persisted">
                : T extends bigint
                  ? NotJsonSerializable<"a bigint throws in JSON.stringify; persist a string or a number">
                  : T extends object
                    ? { [K in keyof T]: JsonCompatible<T[K]> }
                    : NotJsonSerializable<"this value cannot be persisted as JSON">;

/**
 * `unknown` -- the identity for intersection -- when `T` round-trips through
 * JSON, otherwise the degraded shape.  Intersecting it with `T` in a
 * signature therefore costs nothing for good values and turns a bad one into
 * an error naming the offending property and the reason it is rejected.
 */
export type JsonConstraint<T> = [T] extends [JsonCompatible<T>]
  ? unknown
  : JsonCompatible<T>;

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

/**
 * A durable function body.
 *
 * Params are written to Postgres as JSON at spawn time and results are read
 * back from it by whoever awaits them, so both must survive that round trip.
 * The result is checked in its natural (covariant) position.  The params
 * check deliberately rides on the return type instead: a parameter position
 * is contravariant, so intersecting the constraint there would accept every
 * bad type (`{ when: Date & NotJsonSerializable }` is still assignable to
 * `{ when: Date }`).  `NoInfer` keeps that second occurrence of `P` out of
 * inference, so `P` is still read off the params argument alone.
 */
export type TaskHandler<P, R> = (
  params: P,
  ctx: Ctx,
) => Generator<Effect, R & JsonConstraint<R>, unknown> &
  JsonConstraint<NoInfer<P>>;

export interface TaskOptions {
  name: string;
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
}

export interface RegisteredTask {
  name: string;
  maxAttempts?: number;
  retryStrategy?: RetryStrategy;
  /**
   * The erased handler the driver calls.  Params and results were checked
   * against `JsonCompatible` at registration; the registry itself is
   * untyped, so it must not re-apply a constraint the driver cannot satisfy
   * (it feeds in params decoded from the journal).
   */
  handler: (params: never, ctx: Ctx) => Generator<Effect, unknown, unknown>;
}

// ---------------------------------------------------------------------------
// Execution snapshots
// ---------------------------------------------------------------------------

export type ExecutionStatus =
  "pending" | "running" | "suspended" | "completed" | "failed" | "cancelled";

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
