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

await app.applySchema();
await app.spawn(greeting, "world");
app.startWorker();
```

See the [project README](https://github.com/muniter/otra#readme) for the full
execution model and interface documentation.
