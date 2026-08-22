/**
 * The order-fulfillment example from absurd's README, rewritten for otra's
 * generator API.  Note what became possible: charging and reserving run as
 * real parallel child executions on the same queue, and the parent genuinely
 * suspends (zero worker footprint) while it waits for them and for the
 * warehouse event.
 *
 * Run against a Postgres with the schema applied:
 *   OTRA_DATABASE_URL=postgres://... node --experimental-strip-types examples/order.ts
 */
import { Otra } from "../src/index.ts";

const app = new Otra({ queue: "orders" });

interface OrderParams {
  orderId: string;
  amount: number;
  items: string[];
  email: string;
}

const processPayment = app.task(
  "process-payment",
  function* (params: { orderId: string; amount: number }, ctx) {
    return yield* ctx.run("charge", async () => {
      // Derive an idempotency key from the execution id if the downstream
      // system supports one: `${ctx.executionId}:charge`.
      console.log(`charging ${params.amount} for order ${params.orderId}`);
      return { chargeId: `ch_${params.orderId}` };
    });
  },
);

const reserveInventory = app.task(
  "reserve-inventory",
  function* (params: { items: string[] }, ctx) {
    return yield* ctx.run("reserve", async () => {
      console.log(`reserving ${params.items.join(", ")}`);
      return { reserved: params.items.length };
    });
  },
);

const orderFulfillment = app.task(
  "order-fulfillment",
  function* (params: OrderParams, ctx) {
    // Fan out: both children run concurrently, possibly on other workers.
    const payment = yield* ctx.spawn(processPayment, {
      orderId: params.orderId,
      amount: params.amount,
    });
    const inventory = yield* ctx.spawn(reserveInventory, {
      items: params.items,
    });

    // The parent suspends here until both children settle.
    const [charge, stock] = yield* ctx.all([payment, inventory]);

    // Suspend (for days, if need be) until the warehouse emits the event.
    const shipment = yield* ctx.waitForEvent<{ trackingNumber: string }>(
      `shipment.packed:${params.orderId}`,
      { timeout: "30d" },
    );

    yield* ctx.run("send-notification", async () => {
      console.log(`emailing ${params.email}: ${shipment.trackingNumber}`);
    });

    return {
      chargeId: charge.chargeId,
      reserved: stock.reserved,
      trackingNumber: shipment.trackingNumber,
    };
  },
);

const { executionId } = await app.spawn(orderFulfillment, {
  orderId: "42",
  amount: 9_999,
  items: ["widget-1", "gadget-2"],
  email: "customer@example.com",
});

app.startWorker({ workerId: "example-worker" });

// Somewhere else -- a webhook handler, say -- the warehouse reports:
setTimeout(() => {
  void app.emitEvent(`shipment.packed:42`, { trackingNumber: "TRACK-1234" });
}, 500);

console.log("order result:", await app.getResult(executionId));
process.exit(0);
