import type { Ctx, Op } from "../src/index.ts";

declare const ctx: Ctx;

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
