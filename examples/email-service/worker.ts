import crypto from "node:crypto";

import {
  JobRepository,
  WorkerRepository,
} from "../../packages/database/src";

import { HandlerRegistry } from "../../packages/core/src/worker/HandleRegistry";
import { JobExecutor } from "../../packages/core/src/worker/JobExecutor";
import { WorkerRuntime } from "../../packages/core/src/worker/WorkerRuntime";
import { LeaseManager } from "../../packages/core/src/lease/LeaseManager";

import type { WorkerContext } from "../../packages/core/src/worker/WorkerContext";
import type { WorkerOptions } from "../../packages/core/src/types/WorkerOptions";

const TEST_MODE =
  "NORMAL" as
    | "NORMAL"
    | "LONG_RUNNING"
    | "LEASE_EXPIRE"
    | "CRASH";

// -----------------------------
// NORMAL
// lease = 30 sec
// job = 0 sec
//
// LONG_RUNNING
// lease = 30 sec
// job = 15 sec
//
// LEASE_EXPIRE
// lease = 5 sec
// job = 10 sec
//
// CRASH
// worker exits while processing
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
        console.log("Lease should expire around:", new Date(Date.now() + options.leaseDuration!).toISOString());
console.log("Current time:", new Date().toISOString());
        await sleep(60_000);
        break;

      case "LEASE_EXPIRE":
        console.log(
          "Sleeping 10 seconds (lease is only 5 seconds)..."
        );

        await sleep(120_000);

        break;

      case "CRASH":
        console.log(
          "Simulating worker crash in 3 seconds..."
        );

        await sleep(3000);

        process.exit(1);
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
    options
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