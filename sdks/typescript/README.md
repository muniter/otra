# otra TypeScript SDK

The TypeScript SDK for [otra](https://github.com/muniter/otra), a
generator-based durable execution engine on Postgres.

```bash
npm install otra
```

```typescript
import { Otra } from "otra";

const app = new Otra({ db: process.env.DATABASE_URL, queue: "default" });

const greeting = app.task("greeting", function* (name: string, ctx) {
  return yield* ctx.run("format", () => `Hello, ${name}!`);
});

await app.applySchema(); // install the engine's schema (once per database)
await app.createQueue(); // provision the "default" queue (once per queue)

await app.spawn(greeting, "world");
app.startWorker();
```

Requires Node 24 or newer. A queue must exist before anything is spawned onto
it -- `app.spawn` on a missing queue fails with `Queue "default" does not
exist`.

See the [project README](https://github.com/muniter/otra#readme) for the full
execution model and interface documentation.
