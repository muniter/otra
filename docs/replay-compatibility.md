# Replay compatibility: changing task code under live executions

Assumes otra at **pre-release `main`** — the API in
`sdks/typescript/src/context.ts` and the driver in
`sdks/typescript/src/driver.ts`. Nothing here is a stability promise.

A task that sleeps for a week, waits 30 days for an event, or simply retries
tomorrow will resume against code you deployed after it started. The journal
in Postgres is the past; your source tree is the present. This document is
about keeping the two from contradicting each other.

## What replay actually checks

Every effect gets a **key**: its label, plus a `#N` counter for repeats of
that same label within the execution (`charge`, `charge#2`, `charge#3`).
Effects without a user label get the engine's default: `$sleep`, `$now`,
`$random`, `$uuid`, `$event:<name>`, `$spawn:<task>`, `$promise`. Labels
starting with `$` are rejected for user steps.

On replay the driver looks up each key in the loaded history and compares the
recorded **kind** (`run`, `sleep`, `event`, `child`, `external`) with the kind
the code just produced. A mismatch raises `DeterminismViolationError`:

```text
replay diverged at promise "point": history has run "point", code produced
event "point". Durable functions must be deterministic; if the code changed,
in-flight executions may need renamed steps.
```

That is the whole guard. It is worth being precise about what it does *not*
catch, because those are the changes that hurt quietly:

- **A renamed label is not detected.** The new key is absent from history, so
  the step runs again — a second charge, a second email. The old row is
  orphaned.
- **Reordering two differently-labeled steps is not detected.** Each key still
  finds its own memoized value; only the order of *new* work changes.
- **Deleting a step is not detected.** Its row sits in the journal unread.

So the guard is a backstop against shape changes, not a version check. The
discipline is still yours.

## The frontier

Think of an in-flight execution as having a **frontier**: the set of keys
already in its journal. Everything behind the frontier is fixed — it will be
injected verbatim on every future replay, forever. Everything ahead of it is
still just code, and you may change it freely.

Two consequences worth internalizing:

- Editing the body of an already-recorded `ctx.run` has **no effect** on
  in-flight executions. The memoized value replays; your fixed function never
  runs. A bug fix inside a step is a fix for future executions only.
- Retry policy is captured at spawn time (`max_attempts` and `retry_strategy`
  are columns on the execution row), so changing `maxAttempts` in code does
  not re-tune executions already spawned.

## Safe changes

**Append after the frontier.** Adding steps at the end of a task — or into a
branch no in-flight execution has reached — is always safe.

```typescript
const settle = app.task("settle-invoice", function* (params: { id: string }, ctx) {
  const charge = yield* ctx.run("charge", async () => {
    const result = await psp.charge(params.id);
    return { chargeId: result.id };
  });

  // New: everything above still replays from the journal.
  yield* ctx.run("ledger-entry", async () => {
    await ledger.append({ invoice: params.id, charge: charge.chargeId });
  });

  return charge;
});
```

**Change a step's implementation, keep its label and kind.** Switching HTTP
clients, adding logging, tightening a timeout inside the callback: in-flight
executions replay the recorded value and never see the new code; fresh
executions get it.

**Add fields to a step's result.** Old executions still hold the old shape —
that is the case Strategy 2 below exists for — but nothing breaks structurally.

**Register new tasks.** New task names have no history to contradict.

## Changes that break in-flight executions

**Changing an effect's kind at the same label.** This is the one the guard
catches, and it fails the execution permanently:

```typescript
// before
yield* ctx.run("approval", async () => askOverSlack(params.id));

// after — same label, different kind: DeterminismViolationError
yield* ctx.waitForEvent<{ ok: boolean }>("approval-decision", { label: "approval" });
```

**Inserting an unlabeled effect ahead of another with the same default
label.** This one is silent and nasty. Two `ctx.sleep()` calls both use the
label `$sleep`, so they are keyed `$sleep` and `$sleep#2` — positionally, in
effect. Insert a new sleep in front of an old one and the old sleep's memo is
handed to the new call:

```typescript
// before:  $sleep (24h)
yield* ctx.sleep("24h");

// after:   $sleep (5m) reuses the recorded 24h sleep, and the 24h call
//          becomes a brand new $sleep#2
yield* ctx.sleep("5m");
yield* ctx.sleep("24h");
```

The same shift applies to repeated `ctx.now()`, `ctx.random()`, `ctx.uuid()`,
to `ctx.waitForEvent` on the same event name, and to `ctx.spawn` of the same
task. **Label anything you might later insert in front of** — every one of
those methods takes an explicit label:

```typescript
yield* ctx.sleep("24h", "cooloff");
yield* ctx.spawn(reserveInventory, params.items, { label: "reserve" });
yield* ctx.waitForEvent("shipment.packed", { label: "packed" });
```

**Renaming a step.** No error, but the step re-executes and the old result is
abandoned. For a `ctx.run` with an external side effect that is a duplicate
charge. Rename deliberately (see Strategy 1) and only with downstream
idempotency in place.

**Renaming a task that a parent spawns.** `ctx.spawn(child, …)` without a
label keys on `$spawn:<task name>`, so renaming the *child task* changes the
*parent's* key: the parent spawns a second child on its next replay while the
first one keeps running. Pass an explicit `label` to `ctx.spawn` and this
class of change stops mattering.

**Unregistering a task that still has executions.** A worker that claims an
execution whose `functionName` is not in its registry does not fail it — it
defers the claim by 15–30 seconds and tries again, forever. Old executions do
not die; they idle until some worker knows the name again.

**Changing the awaited handle set under an in-flight cancellation.** When a
graceful cancel is delivered, the driver journals *where* it landed (the
`$cancel` row: either `{ key }` or `{ await: [...sorted keys] }`) so every
replay re-delivers at exactly that yield. Change the `ctx.all([...])` set at
that position and the journaled point is never reached: the generator runs its
**forward** path to completion, compensation does not run, and the engine
still finalizes the execution as `cancelled` with an error recorded:

```text
journaled cancellation position {"await":["a","b"]} was never reached during
replay; compensation did not run (did the task code change?)
```

**Changing the params shape.** Params are frozen in the execution row at spawn
time; a replay feeds the old JSON to the new handler. Keep new fields
optional, or read them defensively, until the backlog drains.

## Strategy 1: rename the step

When the meaning or the shape of a result changed enough that reinterpreting
the old value would be wrong, version the label and let old executions keep
their old row:

```typescript
const charge = yield* ctx.run("charge:v2", async () => {
  const result = await psp.charge(params.id, { idempotencyKey: params.id });
  return { chargeId: result.id, provider: "adyen" as const };
});
```

In-flight executions that already recorded `charge` will **not** use it — they
run `charge:v2` from scratch. That is the trade-off: renaming buys you a clean
type at the cost of re-executing the side effect. Only do it where the
downstream call is idempotent (a stable idempotency key on the payment
provider, an upsert, a no-op on repeat).

## Strategy 2: normalize old results

When the change is compatible enough to reinterpret, keep the label and fix up
the value on the way out of the journal:

```typescript
type ChargeV1 = { chargeId: string };
type ChargeV2 = { chargeId: string; provider: "stripe" | "adyen" };

function normalizeCharge(value: ChargeV1 | ChargeV2): ChargeV2 {
  return { chargeId: value.chargeId, provider: "provider" in value ? value.provider : "stripe" };
}

const settle = app.task("settle-invoice", function* (params: { id: string }, ctx) {
  const recorded = yield* ctx.run("charge", async () => {
    const result = await psp.charge(params.id);
    return { chargeId: result.id, provider: "adyen" as const };
  });

  const charge = normalizeCharge(recorded);   // the only place that knows about v1
  yield* ctx.run("receipt", async () => sendReceipt(charge));
  return charge;
});
```

This is usually the better option during a rollout, because nothing
re-executes. Keep the compatibility mess **at the boundary** — one normalizer
immediately after the step — and let the rest of the task speak only the new
shape. Delete the shim once every execution older than the change has
finished — which for a task with a 30-day event wait means 30 days, so date
the shim in a comment rather than trusting memory.

## Major rewrites become new task names

If the control flow itself changed — steps removed, order swapped, a fan-out
where there was a sequence — do not try to teach one generator to be both.
Register `order-fulfillment-v2` alongside `order-fulfillment`, point new
spawns at the new name, and **keep the old handler registered** until the old
executions drain (otherwise workers defer them forever). Deleting the old task
is a separate, later commit.

Two habits make this cheaper: split side effects into small, individually
named steps, so a change to email rendering does not force you to re-version
payment; and prefer short tasks spawned repeatedly over one long-lived task
that accumulates months of journal and months of compatibility risk (see
[cron.md](cron.md)).

## When a replay does break

The execution ends as `failed`, immediately — `DeterminismViolationError` is
non-retryable, so it burns exactly one attempt no matter what `maxAttempts`
says:

```typescript
const snapshot = await app.getExecution(execution);
// status: "failed", attempt: 1
// error: { name: "DeterminismViolationError", message: "replay diverged at promise …" }
```

What you can do about it, in order of preference:

1. **Roll the code back or fix it forward, fast.** Executions that have not
   replayed yet are still fine; each one only diverges when a worker next
   drives it. Getting the labels to line up again saves everything that has
   not yet failed.
2. **Spawn a fresh execution.** `failed` is terminal, and there is currently
   no `app.retry` / resume verb (`app.cancel` and `app.kill` are the only
   lifecycle verbs today). Before re-spawning, read the old journal to see
   which side effects already happened — it is a plain table:
   `select key, label, kind, status, value from otra.p_<queue> where
   execution_id = '…' order by created_at`.
3. **Kill the tree** (`app.kill`) if a half-finished parent is holding
   children you do not want to complete.

None of this is automatic recovery, which is the real argument for the
conservative default: **if you are unsure whether a change is compatible,
assume it is not.** Carrying one dead checkpoint forever is cheaper than
explaining, six months from now, why a task resumed into a shape nobody
recognizes.
