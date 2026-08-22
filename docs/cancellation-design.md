# Cancelling running executions: survey and design

Status: **v1 and v2 core implemented** — suspending compensation via the
journaled `$cancel` delivery-point row landed without needing a `cancelling`
status (the flag column + journal row carry the state; see
tests/compensation.test.ts). Remaining tiers: leaf-first finalization,
preemptible runs, pause/resume. This documents how production
durable-execution engines handle cancellation of running executions, and the
design otra should adopt. Compiled from primary sources (official docs,
SDK source, design blogs) in Aug 2026.

## Survey

### Temporal

Draws the canonical line every other engine has since copied:

- **Cancel** records a `WorkflowExecutionCancelRequested` event and schedules
  a workflow task; the workflow **stays Running** and its code reacts
  ("resembles sending SIGTERM"). Cleanup logic is expected.
- **Terminate** stops immediately; "the Workflow code gets no chance to
  handle termination" ("resembles killing a process"). Guidance: terminate
  only when a workflow is stuck and cannot be cancelled normally.

TypeScript workflows are "a tree of cancellation scopes":
`CancellationScope.cancellable / nonCancellable / withTimeout`. On
cancellation, timers/triggers throw `CancelledFailure`; activities and
children throw failures with `cause: CancelledFailure`; test with
`isCancellation(err)`. Two documented footguns:

1. **Cleanup must be wrapped in `nonCancellable`** — inside a cancelled scope,
   any new cancellable operation "will immediately throw a CancelledFailure",
   so unwrapped cleanup fails before it starts.
2. **The user decides the terminal state** — swallow `CancelledFailure` and
   return normally, and the workflow reports *Completed*, not *Cancelled*.

Activities receive cancellation **on the heartbeat response channel** ("if
the Heartbeat is not invoked, the Activity cannot receive a cancellation
request"); the activity-side API is `Context.current().cancellationSignal`,
an `AbortSignal` for `fetch`/`child_process`. Callers pick strictness per
activity: `TRY_CANCEL` / `WAIT_CANCELLATION_COMPLETED` / `ABANDON`.

Cascade to children is per-child `ParentClosePolicy`
(`ABANDON` / `REQUEST_CANCEL` / `TERMINATE`, default TERMINATE), triggered by
the parent reaching a closed state.

### Restate

Promise-tree shaped, TS-first — the closest relative to otra.

- Cancellation surfaces as a `TerminalError` "thrown at the next await
  point". An in-flight `ctx.run` is **not aborted**: "if cancelled during run
  block execution, then a terminal error gets thrown here once execution
  finishes."
- Propagation is **recursive and leaf-first**: "cancellation first reaches
  the leaves of the call graph", each handler compensates, then it moves up.
- **Compensation is first-class and durable**: "these compensations are
  executed with the same guarantees as the service code." The sagas guide
  (push compensations onto a list, unwind in reverse in the catch, rethrow)
  is the canonical pattern; compensations must be idempotent.
- Detached work (`ctx.send`, `ctx.sendDelayed`) is exempt from cancellation.
- `cancel --kill` skips compensation entirely — the escape hatch.

### DBOS

The deliberate negative case: cancellation with **no compensation hook**.

- Cancel flips status to CANCELLED immediately; running workflows are
  "interrupted at the beginning of the next step" via a status read at step
  boundaries — placed *outside* the retry loop "so workflow cancellation
  propagates immediately instead of consuming the remaining attempts".
- `DBOSWorkflowCancelledError` extends `BaseException` explicitly so user
  code *cannot* catch it. The run just parks.
- In-flight steps run to completion unless marked `preemptible` (a 1s poller
  races the step; on preemption **no outcome is recorded** so the step
  re-runs on resume).
- Cascade is `cancel_children=False` by default; timeouts always cascade.
- Because no compensation runs, cancel is **reversible**: `resume` (from last
  completed step) and `fork(startStep)` exist. Only sound *because* nothing
  was compensated.

### Resonate

Cancellation is a promise-state transition, not an interrupt. The durable
promise spec makes `Cancel` a *downstream* operation (the awaiter cancels;
the doer resolves/rejects): `Pending + Cancel → REJECTED_CANCELED`, after
which `Resolve`/`Reject` are refused ("Already Canceled"). A running
execution is never interrupted; **it discovers cancellation when its settle
is refused**. No scopes, no compensation hook. Detachment (`ctx.detached()`)
is the documented opt-out of structured concurrency.

### Jack Vanlightly (Theory of Durable Execution)

No dedicated cancellation post, but the governing principle is stated in
The Durable Function Tree part 2:

> "errors propagate up, cancellations propagate down the tree"

Failure propagation is *data* travelling up through promise settlement
(otra has this: `_settle_child_promises`); cancellation is *authority*
travelling down through the ownership tree — a different write path.

### Azure Durable Functions / Inngest (brief)

Azure has terminate only — not graceful, and "termination doesn't currently
propagate: activity functions and sub-orchestrations run to completion".
Suspend/resume is a separate primitive. Inngest cancels between steps
("actively executing steps will run to completion") and points users at an
out-of-band pattern: subscribe a separate function to the
`inngest/function.cancelled` system event for cleanup.

## Comparison

|                          | Temporal | Restate | DBOS | Resonate |
| ------------------------ | -------- | ------- | ---- | -------- |
| Request mechanism        | event + new workflow task; stays Running | recursive cancel down tree | status flip, immediate | settle promise REJECTED_CANCELED |
| Surfaces as              | `CancelledFailure` at cancellable ops | `TerminalError` at next await | uncatchable `BaseException` at step boundary | refused settle |
| Discovery channel        | activity **heartbeat response** | engine, at await points | status read at step boundary (+1s poller for `preemptible`) | refused settle |
| In-flight local step     | runs on unless heartbeating; `AbortSignal` opt-in | runs to completion, then throw | runs to completion; opt-in preempt discards outcome | runs to completion |
| Durable compensation?    | yes, **only inside `nonCancellable`** | yes, unconditionally first-class | **no, by construction** | no hook |
| Cascade default          | per-child policy, TERMINATE on close | always, leaf-first | off by default | not modelled |
| Detached opt-out         | ABANDON policy | `ctx.send` | — | `ctx.detached()` |
| Hard-kill variant        | terminate | `cancel --kill` | (cancel is one) | — |
| Terminal state chosen by | **user code** (footgun) | engine | engine | engine |

Three convergent facts:

1. **Nobody interrupts a running local step by default.** Interruption is
   opt-in (heartbeat+AbortSignal, `preemptible`) and always pairs with
   "the result is discarded".
2. **Whether compensation can do durable work is the deciding axis**, and it
   is determined by one choice: cancel-as-request against a live execution
   (Temporal, Restate) vs cancel-as-status-flip (DBOS — which then *must*
   make the error uncatchable, because nothing written after the flip could
   be checkpointed).
3. Cascade defaults are contested, but everyone who cascades ships a
   per-child detach.

## Recommended design for otra

### The constraint that decides everything

otra's `_assert_owner` guards every history write with
`status = 'running' and claimed_by = worker`. Therefore flipping a running
execution to `'cancelled'` makes all subsequent writes illegal — compensation
could execute but never checkpoint, i.e. the DBOS dead end. So:

**`cancel_requested_at` is a column, not a status.** The execution stays
`'running'`, the claim stays held, and every existing guard keeps working —
which means `catch`/`finally` blocks can run `ctx.run` compensation steps
that checkpoint normally, with **no `nonCancellable` wrapper needed**. That
is otra's structural advantage over Temporal TS here, and it falls
directly out of the request-flag choice.

### v1 semantics

- **Discovery**: `extend_claim` (already called every `claimSeconds/2` for
  the whole drive, result currently discarded) returns
  `(held, cancel_requested)`; `claim` also returns the flag. Backstops:
  `suspend` refuses when a cancel is pending (driver's existing redrive path
  then delivers), and `_fail_attempt` finalizes to `'cancelled'` instead of
  retrying — which also makes the claim sweep finalize a worker that died
  mid-compensation.
- **Delivery**: `gen.throw(new CancelledError())` (catch-shaped compensation,
  like Temporal TS/Restate — not `gen.return()`, which only reaches
  `finally`), at the first effect requiring *new* work after the memoized
  fast-forward. Never before the generator starts.
- **Engine-owned outcome** (closes Temporal's footgun): once delivered, the
  execution finalizes to `'cancelled'` regardless of how the generator ends —
  catch-and-return still yields `'cancelled'`, never `'completed'`, and never
  a retry.
- **Effect gating after delivery**: `run` allowed (that *is* compensation);
  `sleep`/`event`/`spawn`/pending-`await` throw `CancelledError` again (a
  cancelled execution that suspends has nothing left to wake it). Cap
  re-deliveries (~10), then `gen.return()`. Grace window
  `cancelGraceSeconds` (default = claimSeconds), heartbeating throughout.
- **Cancel vs kill — ship both**: `app.cancel(id, {cascade, reason})`
  (graceful: flag + wake suspended targets so they replay and unwind;
  pending-with-no-history finalizes immediately) and
  `app.kill(id, {cascade, reason})` (today's `cancel` extended to running
  rows; discovered as a new sqlstate **OT002** ≠ OT001, so a kill is
  reported as `killed`, not confused with a stolen claim, and runs no
  compensation).
- **Cascade on by default, top-down, one transaction** (locks in id order),
  with per-child `ctx.spawn(..., { onParentCancel: 'detach' })` opt-out.
  Marking the whole subtree atomically guarantees no descendant starts new
  forward work — most of what Restate's leaf-first ordering buys.
- **API**: `ctx.cancelRequested`, `ctx.signal` (AbortSignal),
  `ctx.throwIfCancelled()`, `ctx.uninterruptible(fn)` (defers delivery —
  needed for *forward*-direction critical sections, not for cleanup),
  `CancelledError`, `isCancellation(err)`.

### The v1/v2 line (principled, not arbitrary)

**Compensation that never suspends never replays, so it never needs a
durable record of where cancellation was delivered.** That is why v1
restricts compensation to local `run` steps. v2, in order:

1. **Suspending compensation**: a real `'cancelling'` status accepted by
   `_assert_owner`, plus a durable `$cancel` promise row recording the
   delivery point so replay re-throws at the same yield (Temporal gets this
   free from its event history position), plus a budget.
2. **Leaf-first finalization** (parent waits for descendants before
   compensating) — requires (1).
3. **`preemptible` runs**: AbortSignal into the step fn; on abort, don't
   record the run (DBOS).
4. **LISTEN/NOTIFY on cancel** for millisecond discovery.
5. **Out-of-band hook**: emit `otra.cancelled` event on finalize
   (Inngest's pattern) so cleanup can be a separate durable task with its
   own fault domain.
6. **Operator pause/resume as a distinct `'paused'` status** — never bolted
   onto cancel: resume is only sound if compensation did not run.
7. `awaitCancellation` option (Temporal's `WAIT_CANCELLATION_COMPLETED`).

### Test list (abridged)

Delivery: mid-run cancel records the in-flight run then delivers at the next
yield; suspended target wakes, replays, delivers at the blocked await;
empty-history pending finalizes without a worker; catch-and-return still
ends `'cancelled'`; catch-and-throw-other ends `'cancelled'` without retry.
Compensation: `finally` runs are checkpointed; reverse saga unwind; remote
effects in compensation are refused; grace cutoff; worker crash
mid-compensation finalizes via the sweep. Kill: OT002 vs OT001
distinguishable; no compensation runs; kill-during-cancel converges.
Cascade: tree of four cancels with per-node compensation; detached child
survives and settles against a terminal parent; concurrent overlapping
cascades don't deadlock (id-order locks; use the pg_stat_activity
forced-contention technique). Races: cancel between flag-check and suspend
(refused suspend → redrive → delivery — the no-lost-wakeup analogue);
concurrent cancel/complete settle child promises exactly once.
