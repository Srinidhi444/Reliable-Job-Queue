import crypto from "node:crypto";

import {
  JobRepository,
  WorkerRepository,
} from "../../packages/database/src";

import { HandlerRegistry } from "../../packages/core/src/worker/HandleRegistry";
import { JobExecutor } from "../../packages/core/src/worker/JobExecutor";
import { WorkerRuntime } from "../../packages/core/src/worker/WorkerRuntime";
import { LeaseManager } from "../../packages/core/src/lease/LeaseManager";
import { ExponentialBackoffStrategy } from "../../packages/core/src/retry/ExponentialBackoffStrategy";

import type { WorkerContext } from "../../packages/core/src/worker/WorkerContext";
import type { WorkerOptions } from "../../packages/core/src/types/WorkerOptions";

const TEST_MODE =
  "FAIL" as
    | "NORMAL"
    | "LONG_RUNNING"
    | "LEASE_EXPIRE"
    | "CRASH"
    | "FAIL";

// -----------------------------
// NORMAL
// Immediate success
//
// LONG_RUNNING
// Long-running successful job
//
// LEASE_EXPIRE
// Used before heartbeat manager
//
// CRASH
// Worker crashes
//
// FAIL
// Handler throws to test retries
// -----------------------------

async function main() {
  const workerId = crypto.randomUUID();

  const context: WorkerContext = {
    workerId,
  };

  const options: WorkerOptions = {
    pollingInterval: 1000,
    heartbeatInterval: 3000,
    leaseDuration: 10000,
  };

  const jobRepository = new JobRepository();
  const workerRepository = new WorkerRepository();

  const leaseManager = new LeaseManager(
    jobRepository,
    options.leaseDuration!
  );

  const retryStrategy =
    new ExponentialBackoffStrategy(5000);

  const registry = new HandlerRegistry();

  registry.register("send-email", async (job) => {
    console.log("==================================");
    console.log(`Worker: ${workerId}`);
    console.log(`Mode  : ${TEST_MODE}`);
    console.log(`Job Started: ${job.id}`);
    console.log(job.payload);
    console.log("==================================");

    switch (TEST_MODE) {
      case "NORMAL":
        break;

      case "LONG_RUNNING":
        console.log(
          "Sleeping 60 seconds..."
        );
        await sleep(60_000);
        break;

      case "LEASE_EXPIRE":
        console.log(
          "Sleeping 120 seconds..."
        );
        await sleep(120_000);
        break;

      case "CRASH":
        console.log(
          "Simulating worker crash..."
        );

        await sleep(3000);

        process.exit(1);

      case "FAIL":
        console.log(
          "Throwing intentional error..."
        );

        throw new Error(
          "Intentional failure for retry testing."
        );
    }

    console.log("Job Finished");
  });

  const executor = new JobExecutor(
    registry,
    context
  );

  const worker = new WorkerRuntime(
    jobRepository,
    workerRepository,
    leaseManager,
    executor,
    workerId,
    options,
    retryStrategy
  );

  process.on("SIGINT", async () => {
    console.log("\nStopping worker...");

    await worker.stop();

    process.exit(0);
  });

  process.on("SIGTERM", async () => {
    console.log("\nStopping worker...");

    await worker.stop();

    process.exit(0);
  });

  await worker.start();
}

function sleep(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

main().catch(console.error);