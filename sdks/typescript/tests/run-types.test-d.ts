import type { Ctx, DurableHandle, ExternalPromise, Op } from "../src/index.ts";
import type { Otra } from "../src/index.ts";

declare const ctx: Ctx;
declare const app: Otra;

// ---------------------------------------------------------------------------
// ctx.run: results are persisted as JSON, so the callback's return type must
// survive a JSON round trip.
// ---------------------------------------------------------------------------

ctx.run("string", () => "value");
ctx.run("object", () => ({ id: "item-1", count: 2, active: true }));
ctx.run("async", async () => ({ values: [1, null, "three"] }));
const voidResult: Op<null> = ctx.run("void", () => {});
const asyncVoidResult: Op<null> = ctx.run("async-void", async () => {});
void voidResult;
void asyncVoidResult;

// @ts-expect-error Dates change shape when persisted as JSON.
ctx.run("date", () => new Date());

// @ts-expect-error Nested values must also be JSON-compatible.
ctx.run("nested-date", () => ({ createdAt: new Date() }));

const undefinedResult: Op<null> = ctx.run("undefined", () => undefined);
void undefinedResult;

// Interfaces are JSON-compatible.  TypeScript refuses to give them the
// implicit index signature `JsonValue` demands, so a naive
// `extends JsonValue` constraint rejects every interface in a codebase.
interface OrderRecord {
  id: string;
  total: number;
  /** optional properties are fine: `undefined` persists as null */
  note?: string;
  tags: string[];
  customer: { email: string };
}

declare const order: OrderRecord;
ctx.run("interface", () => order);
ctx.run("interface-array", () => [order]);
ctx.run("interface-nested", () => ({ order, orders: [order] }));

// Type aliases (which do get the implicit index signature) keep working.
type OrderAlias = { id: string; total: number };
declare const aliased: OrderAlias;
ctx.run("alias", () => aliased);

// Optional and undefined-bearing values survive (persisted as null).
ctx.run("optional-prop", () => ({ note: undefined as string | undefined }));
const maybeString: Op<string | undefined> = ctx.run(
  "maybe-string",
  () => "x" as string | undefined,
);
void maybeString;

// Inference still yields the precise result type, not `unknown`.
const inferredRun: Op<{ id: string; total: number }> = ctx.run(
  "inference",
  () => aliased,
);
void inferredRun;

// Non-JSON values are rejected, one kind per case.

// @ts-expect-error Maps serialize to `{}`.
ctx.run("map", () => new Map<string, string>());

// @ts-expect-error Sets serialize to `{}`.
ctx.run("set", () => new Set<string>());

// @ts-expect-error Functions do not serialize at all.
ctx.run("function", () => () => "nope");

// @ts-expect-error BigInt throws in JSON.stringify.
ctx.run("bigint", () => 1n);

// @ts-expect-error Symbols do not serialize.
ctx.run("symbol", () => Symbol("nope"));

class Money {
  cents = 0;
  format(): string {
    return `${this.cents}`;
  }
}
// @ts-expect-error A class instance loses its methods on the way through JSON.
ctx.run("class-instance", () => new Money());

// @ts-expect-error A Date nested in an array is still a Date.
ctx.run("array-of-dates", () => [new Date()]);

// ---------------------------------------------------------------------------
// Task params and results cross the same boundary: params are written as JSON
// at spawn time, results are read back as JSON by whoever awaits them.
// ---------------------------------------------------------------------------

const typedTask = app.task("typed", function* (params: OrderRecord, taskCtx) {
  void params;
  void taskCtx;
  return { count: 1 };
});

// Both parameter and result types survive inference through the constraint.
const spawned: Op<DurableHandle<{ count: number }>> = ctx.spawn(
  typedTask,
  order,
);
void spawned;

const called: Op<{ count: number }> = ctx.call(typedTask, order);
void called;

app.task("void-result", function* (_params: null, _ctx) {
  // Tasks that return nothing stay legal.
});

// @ts-expect-error Task params must be JSON-compatible.
app.task("date-params", function* (_params: { when: Date }, _ctx) {});

app.task(
  "nested-date-params",
  // @ts-expect-error Nested non-JSON params are caught too.
  function* (_params: { at: { on: Date } }, _ctx) {},
);

app.task(
  "date-result",
  // @ts-expect-error Task results must be JSON-compatible.
  function* (_params: null, _ctx) {
    return new Date();
  },
);

app.task(
  "map-result",
  // @ts-expect-error Task results must be JSON-compatible.
  function* (_params: null, _ctx) {
    return new Map<string, string>();
  },
);

// ---------------------------------------------------------------------------
// waitForEvent and ctx.promise both inject values decoded from JSON.
// ---------------------------------------------------------------------------

const event: Op<{ trackingNumber: string }> = ctx.waitForEvent<{
  trackingNumber: string;
}>("shipment.packed");
void event;

const interfaceEvent: Op<OrderRecord> = ctx.waitForEvent<OrderRecord>("order");
void interfaceEvent;

const untypedEvent = ctx.waitForEvent("anything");
void untypedEvent;

// @ts-expect-error An event payload is JSON, never a Date.
ctx.waitForEvent<Date>("bad-event");

// @ts-expect-error Nested Dates in an event payload are equally wrong.
ctx.waitForEvent<{ at: Date }>("bad-nested-event");

const external: Op<ExternalPromise<{ approvedBy: string }>> = ctx.promise<{
  approvedBy: string;
}>("approval");
void external;

const interfacePromise: Op<ExternalPromise<OrderRecord>> =
  ctx.promise<OrderRecord>("order-promise");
void interfacePromise;

const untypedPromise = ctx.promise("plain");
void untypedPromise;

// @ts-expect-error An externally-settled promise carries JSON, never a Map.
ctx.promise<Map<string, string>>("bad-promise");

// @ts-expect-error Nested non-JSON values are caught in promises too.
ctx.promise<{ approvedAt: Date }>("bad-nested-promise");
