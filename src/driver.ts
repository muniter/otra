import { Ctx } from "./context.ts";
import {
  isClaimLost,
  isKilled,
  promiseToken,
  type ClaimedExecution,
  type Db,
  type PromiseRow,
} from "./db.ts";
import {
  CancelledError,
  ChildFailedError,
  DeterminismViolationError,
  EventTimeoutError,
  TimeoutError,
  serializeError,
  type AwaitEffect,
  type Effect,
  type ErrorPayload,
  type RegisteredTask,
} from "./types.ts";

export interface DriveOptions {
  queue: string;
  workerId: string;
  claimSeconds: number;
}

export type DriveOutcome =
  | { type: "completed"; result: unknown }
  | { type: "suspended" }
  /** A blocker settled while we were deciding to park: replay immediately. */
  | { type: "redrive" }
  | {
      type: "failed";
      error: ErrorPayload;
      retryable: boolean;
      permanent: boolean;
    }
  /** A graceful cancel was delivered and the execution finalized. */
  | { type: "cancelled" }
  /** The execution was killed out from under us (OT002); no compensation. */
  | { type: "killed" }
  /** Our claim was stolen (lease expired mid-flight); abandon quietly. */
  | { type: "lost" };

const EFFECT_KINDS = {
  run: "run",
  sleep: "sleep",
  event: "event",
  spawn: "child",
  external: "external",
} as const;

/** Where a graceful cancel was delivered, journaled in the $cancel row. */
type CancelPosition = { key: string } | { await: string[] };

type Resume =
  | { type: "next"; value: unknown }
  | { type: "throw"; error: unknown };

function classifyRetryable(err: unknown, injected: unknown): boolean {
  // An error we injected from a memoized rejection escaped uncaught: the
  // rejection is durable, so every retry would deterministically rethrow it.
  if (injected !== null && err === injected) return false;
  if (err instanceof DeterminismViolationError) return false;
  const retryable = (err as { retryable?: unknown })?.retryable;
  if (typeof retryable === "boolean") return retryable;
  return true;
}

function rejectionToError(row: PromiseRow): Error {
  const payload = row.error ?? { message: "promise rejected" };
  if (row.kind === "child") {
    return new ChildFailedError(row.childExecutionId ?? "unknown", payload);
  }
  if (payload.name === "EventTimeoutError") {
    return new EventTimeoutError(payload.message);
  }
  if (payload.name === "TimeoutError") {
    return new TimeoutError(payload.message);
  }
  const err = new Error(payload.message);
  err.name = payload.name ?? "Error";
  return err;
}

/**
 * Replay one claimed execution: run its generator from the top, fast-forward
 * through the memoized promise history, execute the first unrecorded local
 * side effect(s), and either complete, fail, cancel, or suspend on
 * unresolved remote promises.
 *
 * Graceful cancellation (see docs/cancellation-design.md): a pending cancel
 * is discovered via the claim row or a heartbeat response and delivered as a
 * CancelledError thrown into the generator at the first effect that needs
 * NEW work -- never before the generator starts, never at a memoized step,
 * which keeps the delivery point deterministic across replays.  After
 * delivery, local `run` steps still execute and checkpoint (that IS the
 * compensation); remote effects rethrow.  However the generator ends, the
 * engine finalizes to 'cancelled'.
 */
export async function driveOnce(
  db: Db,
  registered: RegisteredTask,
  claimed: ClaimedExecution,
  options: DriveOptions,
): Promise<DriveOutcome> {
  const { executionId } = claimed;
  const { workerId, claimSeconds, queue } = options;

  const history = await db.loadHistory(executionId);
  const labelCounts = new Map<string, number>();
  const keyFor = (label: string): string => {
    const n = (labelCounts.get(label) ?? 0) + 1;
    labelCounts.set(label, n);
    return n === 1 ? label : `${label}#${n}`;
  };

  const checkRecorded = (
    key: string,
    effect: Exclude<Effect, AwaitEffect | { type: "shield" }>,
  ): PromiseRow | undefined => {
    const row = history.get(key);
    if (row === undefined) return undefined;
    const expectedKind = EFFECT_KINDS[effect.type];
    if (row.kind !== expectedKind || row.label !== effect.label) {
      throw new DeterminismViolationError(
        `replay diverged at promise "${key}": history has ${row.kind} "${row.label}", ` +
          `code produced ${expectedKind} "${effect.label}". ` +
          `Durable functions must be deterministic; if the code changed, ` +
          `in-flight executions may need renamed steps.`,
      );
    }
    return row;
  };

  // --- cancellation state ---------------------------------------------------
  const abort = new AbortController();
  const ctx = new Ctx(executionId, claimed.attempt, queue, abort.signal);
  let cancelPending = false;
  let cancelDelivered = false;
  let shielded = 0;

  const markCancelPending = (): void => {
    if (cancelPending) return;
    cancelPending = true;
    ctx.cancelRequested = true;
    abort.abort(new CancelledError());
  };
  if (claimed.cancelRequested) markCancelPending();

  // A journaled $cancel row means delivery already happened on an earlier
  // attempt: this drive is a compensation replay, and CancelledError must be
  // re-thrown at the recorded yield -- never re-derived, so a forward
  // promise that settles after cancellation can't divert the replay back
  // onto the forward path.
  const cancelRow = history.get("$cancel");
  const journaledDelivery = cancelRow !== undefined;
  let pendingReplayPosition: CancelPosition | null = null;
  if (cancelRow !== undefined) {
    markCancelPending();
    pendingReplayPosition = cancelRow.value as unknown as CancelPosition;
  }

  const shouldDeliver = (): boolean =>
    cancelPending &&
    !cancelDelivered &&
    shielded === 0 &&
    pendingReplayPosition === null;

  const matchesKeyed = (key: string): boolean =>
    !cancelDelivered &&
    pendingReplayPosition !== null &&
    (pendingReplayPosition as { key?: unknown }).key === key;

  const matchesAwait = (handleKeys: string[]): boolean => {
    if (cancelDelivered || pendingReplayPosition === null) return false;
    const at = (pendingReplayPosition as { await?: unknown }).await;
    if (!Array.isArray(at)) return false;
    const sorted = [...handleKeys].sort();
    return at.length === sorted.length && at.every((k, i) => k === sorted[i]);
  };

  /** Re-deliver at the journaled position during a compensation replay. */
  const deliverReplay = (): Resume => {
    cancelDelivered = true;
    pendingReplayPosition = null;
    return { type: "throw", error: new CancelledError() };
  };

  /** First delivery: journal the position BEFORE throwing, so a crash after
   *  this write re-delivers at the same yield on replay. */
  const deliverFresh = async (position: CancelPosition): Promise<Resume> => {
    await db.recordCancel(executionId, workerId, position);
    cancelDelivered = true;
    return { type: "throw", error: new CancelledError() };
  };

  const gen = registered.handler(claimed.params as never, ctx);

  // Heartbeat for the entire drive; its response doubles as the cancellation
  // discovery channel (Temporal delivers activity cancellation the same way).
  const heartbeatMs = Math.max((claimSeconds * 1000) / 2, 500);
  const heartbeat = setInterval(() => {
    void db
      .extendClaim(executionId, workerId, claimSeconds)
      .then(({ cancelRequested }) => {
        if (cancelRequested) markCancelPending();
      })
      .catch(() => {});
  }, heartbeatMs);
  heartbeat.unref?.();

  const finalizeCancelled = async (
    error: ErrorPayload | null,
  ): Promise<DriveOutcome> => {
    try {
      const finalized = await db.finalizeCancelled(
        executionId,
        workerId,
        error,
      );
      return finalized ? { type: "cancelled" } : { type: "lost" };
    } catch {
      return { type: "lost" };
    }
  };

  const reportFailedAttempt = async (
    payload: ErrorPayload,
    retryable: boolean,
  ): Promise<DriveOutcome> => {
    try {
      const { applied, failedPermanently } = await db.failAttempt(
        executionId,
        workerId,
        payload,
        retryable,
      );
      if (!applied) return { type: "lost" };
      return {
        type: "failed",
        error: payload,
        retryable,
        permanent: failedPermanently,
      };
    } catch {
      return { type: "lost" };
    }
  };

  let resume: Resume = { type: "next", value: undefined };

  try {
    return await driveLoop();
  } catch (err) {
    // The worker lost its claim mid-drive: OT001 = stolen lease, OT002 =
    // killed by an operator.  Either way, abandon without further writes.
    if (isClaimLost(err)) return { type: "lost" };
    if (isKilled(err)) return { type: "killed" };
    // Driver-detected divergence (as opposed to one thrown into the
    // generator): fail the attempt permanently rather than crash the worker.
    if (err instanceof DeterminismViolationError) {
      return reportFailedAttempt(serializeError(err), false);
    }
    throw err; // infrastructure errors (e.g. lost DB) bubble to the worker
  } finally {
    clearInterval(heartbeat);
  }

  /** Park on the blockers, or deliver a just-discovered cancel instead. */
  async function suspendOrDeliver(
    blockerKeys: string[],
    position: CancelPosition,
  ): Promise<DriveOutcome | null> {
    const { suspended, cancelRequested } = await db.suspend(
      executionId,
      workerId,
      blockerKeys,
    );
    if (suspended) return { type: "suspended" };
    if (cancelRequested && !cancelDelivered) {
      // Cancel landed between our last check and the park: deliver in place.
      markCancelPending();
      resume = await deliverFresh(position);
      return null;
    }
    return { type: "redrive" };
  }

  async function driveLoop(): Promise<DriveOutcome> {
    while (true) {
      const injected = resume.type === "throw" ? resume.error : null;
      let step: IteratorResult<Effect, unknown>;
      try {
        step =
          resume.type === "next"
            ? gen.next(resume.value)
            : gen.throw(resume.error);
      } catch (err) {
        if (cancelDelivered || journaledDelivery) {
          // The engine owns the outcome after delivery: rethrown Cancelled
          // finalizes clean; any other error is recorded but never retried.
          return finalizeCancelled(
            err instanceof CancelledError ? null : serializeError(err),
          );
        }
        if (err instanceof CancelledError && cancelPending) {
          // ctx.throwIfCancelled() escaped uncaught: same as a delivery.
          cancelDelivered = true;
          return finalizeCancelled(null);
        }
        const retryable = classifyRetryable(err, injected);
        return reportFailedAttempt(serializeError(err), retryable);
      }

      if (step.done) {
        if (cancelDelivered || journaledDelivery)
          return finalizeCancelled(null);
        try {
          await db.complete(executionId, workerId, step.value);
        } catch (err) {
          return isKilled(err) ? { type: "killed" } : { type: "lost" };
        }
        return { type: "completed", result: step.value };
      }

      const effect = step.value;

      if (effect.type === "shield") {
        shielded = Math.max(0, shielded + (effect.on ? 1 : -1));
        resume = { type: "next", value: undefined };
        continue;
      }

      switch (effect.type) {
        case "run": {
          const key = keyFor(effect.label);
          if (matchesKeyed(key)) {
            resume = deliverReplay();
            break;
          }
          const recorded = checkRecorded(key, effect);
          if (recorded !== undefined) {
            // Runs are only ever recorded resolved.
            resume = { type: "next", value: recorded.value };
            break;
          }
          if (shouldDeliver()) {
            resume = await deliverFresh({ key });
            break;
          }
          // Executes both before delivery and after it: post-delivery local
          // runs ARE the compensation, and they checkpoint normally because
          // the execution is still 'running' and the claim is still ours.
          // A failing compensation step goes through the normal retry path;
          // the SQL keeps the cancel flag, and the retry replays back into
          // the compensation via the journaled delivery point.
          let value: unknown;
          try {
            value = await effect.fn();
          } catch (err) {
            const retryable = classifyRetryable(err, null);
            return reportFailedAttempt(serializeError(err), retryable);
          }
          const stored = await db.recordRun(
            executionId,
            workerId,
            key,
            effect.label,
            value,
            claimSeconds,
          );
          history.set(key, {
            key,
            label: effect.label,
            kind: "run",
            status: "resolved",
            value: stored,
            error: null,
            childExecutionId: null,
          });
          resume = { type: "next", value: stored };
          break;
        }

        case "sleep": {
          const key = keyFor(effect.label);
          if (matchesKeyed(key)) {
            resume = deliverReplay();
            break;
          }
          let recorded = checkRecorded(key, effect);
          if (recorded !== undefined && recorded.status === "resolved") {
            resume = { type: "next", value: undefined };
            break;
          }
          if (shouldDeliver()) {
            resume = await deliverFresh({ key });
            break;
          }
          if (recorded === undefined) {
            const created = await db.createSleep(
              executionId,
              workerId,
              key,
              effect.label,
              effect.seconds,
            );
            recorded = {
              key,
              label: effect.label,
              kind: "sleep",
              status: created.status,
              value: null,
              error: null,
              childExecutionId: null,
            };
            history.set(key, recorded);
            if (recorded.status === "resolved") {
              resume = { type: "next", value: undefined };
              break;
            }
          }
          const outcome = await suspendOrDeliver([key], { key });
          if (outcome !== null) return outcome;
          break;
        }

        case "event": {
          const key = keyFor(effect.label);
          if (matchesKeyed(key)) {
            resume = deliverReplay();
            break;
          }
          let recorded = checkRecorded(key, effect);
          if (recorded !== undefined && recorded.status === "resolved") {
            resume = { type: "next", value: recorded.value };
            break;
          }
          if (recorded !== undefined && recorded.status === "rejected") {
            // A settled rejection is memoized history: inject it, cancel or
            // not, so replays stay deterministic.
            resume = { type: "throw", error: rejectionToError(recorded) };
            break;
          }
          if (shouldDeliver()) {
            resume = await deliverFresh({ key });
            break;
          }
          if (recorded === undefined) {
            const created = await db.createEventWait(
              executionId,
              workerId,
              key,
              effect.label,
              effect.eventName,
              effect.timeoutSeconds,
            );
            recorded = {
              key,
              label: effect.label,
              kind: "event",
              status: created.status,
              value: created.value,
              error: created.error,
              childExecutionId: null,
            };
            history.set(key, recorded);
            if (recorded.status === "resolved") {
              resume = { type: "next", value: recorded.value };
              break;
            }
            if (recorded.status === "rejected") {
              resume = { type: "throw", error: rejectionToError(recorded) };
              break;
            }
          }
          const outcome = await suspendOrDeliver([key], { key });
          if (outcome !== null) return outcome;
          break;
        }

        case "spawn": {
          const key = keyFor(effect.label);
          if (matchesKeyed(key)) {
            resume = deliverReplay();
            break;
          }
          const recorded = checkRecorded(key, effect);
          if (recorded !== undefined) {
            resume = {
              type: "next",
              value: {
                kind: "otra:handle",
                key,
                executionId: recorded.childExecutionId!,
              },
            };
            break;
          }
          if (shouldDeliver()) {
            resume = await deliverFresh({ key });
            break;
          }
          const spawned = await db.spawn(
            effect.taskName,
            effect.params,
            effect.options.queue ?? queue,
            effect.options,
            { executionId, key, label: effect.label, worker: workerId },
          );
          history.set(key, {
            key,
            label: effect.label,
            kind: "child",
            status: "pending",
            value: null,
            error: null,
            childExecutionId: spawned.executionId,
          });
          resume = {
            type: "next",
            value: {
              kind: "otra:handle",
              key,
              executionId: spawned.executionId,
            },
          };
          break;
        }

        case "external": {
          const key = keyFor(effect.label);
          if (matchesKeyed(key)) {
            resume = deliverReplay();
            break;
          }
          const recorded = checkRecorded(key, effect);
          if (recorded !== undefined) {
            resume = {
              type: "next",
              value: {
                kind: "otra:handle",
                key,
                executionId,
                token: promiseToken(recorded.id!),
              },
            };
            break;
          }
          if (shouldDeliver()) {
            resume = await deliverFresh({ key });
            break;
          }
          const created = await db.createExternal(
            executionId,
            workerId,
            key,
            effect.label,
            effect.timeoutSeconds,
          );
          history.set(key, {
            id: created.id,
            key,
            label: effect.label,
            kind: "external",
            status: created.status,
            value: created.value,
            error: created.error,
            childExecutionId: null,
          });
          resume = {
            type: "next",
            value: {
              kind: "otra:handle",
              key,
              executionId,
              token: promiseToken(created.id),
            },
          };
          break;
        }

        case "await": {
          // Awaits reference promises created by earlier spawns; they do not
          // allocate history entries of their own.
          const rows: PromiseRow[] = [];
          for (const handle of effect.handles) {
            const row = history.get(handle.key);
            if (row === undefined) {
              throw new DeterminismViolationError(
                `awaited unknown promise "${handle.key}"; handles must come from ` +
                  `ctx.spawn within the same execution`,
              );
            }
            rows.push(row);
          }

          // Journaled delivery point takes precedence over everything --
          // even a forward promise that settled after cancellation.
          const awaitKeys = effect.handles.map((h) => h.key);
          if (matchesAwait(awaitKeys)) {
            resume = deliverReplay();
            break;
          }

          // Child promises settle out-of-band, so our snapshot may be stale;
          // refresh the still-pending ones before deciding to park.
          const staleKeys = rows
            .filter((r) => r.status === "pending")
            .map((r) => r.key);
          if (staleKeys.length > 0) {
            const fresh = await db.getPromises(executionId, staleKeys);
            for (const row of rows) {
              const update = fresh.get(row.key);
              if (update !== undefined && update.status !== "pending") {
                row.status = update.status;
                row.value = update.value;
                row.error = update.error;
              }
            }
          }

          const rejected = rows.find((r) => r.status === "rejected");
          if (rejected !== undefined) {
            resume = { type: "throw", error: rejectionToError(rejected) };
            break;
          }
          const pending = rows.filter((r) => r.status === "pending");
          if (pending.length > 0) {
            const position: CancelPosition = { await: [...awaitKeys].sort() };
            if (shouldDeliver()) {
              resume = await deliverFresh(position);
              break;
            }
            const outcome = await suspendOrDeliver(
              pending.map((r) => r.key),
              position,
            );
            if (outcome !== null) return outcome;
            break;
          }
          resume = {
            type: "next",
            value: effect.single ? rows[0]!.value : rows.map((r) => r.value),
          };
          break;
        }
      }
    }
  }
}
