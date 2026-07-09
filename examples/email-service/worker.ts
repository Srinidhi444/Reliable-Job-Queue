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

async function main() {
  const workerId = crypto.randomUUID();

  const context: WorkerContext = {
    workerId,
  };

  const options: WorkerOptions = {
    pollingInterval: 1000,
    heartbeatInterval: 10000,
    leaseDuration: 30000,
  };

  const jobRepository = new JobRepository();

  const workerRepository = new WorkerRepository();

  const leaseManager = new LeaseManager(
    jobRepository,
    options.leaseDuration!
  );

  const registry = new HandlerRegistry();

  registry.register("send-email", async (job) => {
    console.log("================================");
    console.log("📧 Sending Email");
    console.log(job.payload);
    console.log("================================");
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

main().catch(console.error);