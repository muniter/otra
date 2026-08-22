import { spawn } from "node:child_process";
import { readdir } from "node:fs/promises";

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
