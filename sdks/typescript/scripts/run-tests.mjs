import { spawn } from "node:child_process";
import { readdir } from "node:fs/promises";

// --test-global-setup (which builds the shared template database every test
// clones) landed in Node 24; older runtimes reject it with an opaque
// "bad option" line, so fail with something actionable instead.
const REQUIRED_NODE_MAJOR = 24;
const nodeMajor = Number.parseInt(process.versions.node, 10);
if (!(nodeMajor >= REQUIRED_NODE_MAJOR)) {
  console.error(
    `otra tests need Node >= ${REQUIRED_NODE_MAJOR} (--test-global-setup); you are on v${process.versions.node}`,
  );
  process.exit(1);
}

const concurrency = process.env.OTRA_TEST_DB
  ? 1
  : Number(process.env.OTRA_TEST_CONCURRENCY ?? 8);

if (!Number.isInteger(concurrency) || concurrency < 1) {
  throw new Error("OTRA_TEST_CONCURRENCY must be a positive integer");
}

const testFiles = (await readdir("tests"))
  .filter((name) => name.endsWith(".test.ts"))
  .sort()
  .map((name) => `tests/${name}`);

const child = spawn(
  process.execPath,
  [
    "--experimental-strip-types",
    "--test",
    `--test-concurrency=${concurrency}`,
    "--test-global-setup=tests/global-setup.ts",
    ...testFiles,
  ],
  { stdio: "inherit" },
);

child.once("error", (error) => {
  throw error;
});
child.once("exit", (code, signal) => {
  if (signal) process.kill(process.pid, signal);
  process.exitCode = code ?? 1;
});
