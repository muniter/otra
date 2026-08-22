import { copyFile, mkdir } from "node:fs/promises";

const packageRoot = new URL("../", import.meta.url);

await mkdir(new URL("sql/", packageRoot), { recursive: true });
await copyFile(
  new URL("../../../sql/schema.sql", import.meta.url),
  new URL("sql/schema.sql", packageRoot),
);
await copyFile(
  new URL("../../../LICENSE", import.meta.url),
  new URL("LICENSE", packageRoot),
);
