import crypto from "node:crypto";

import { JobRepository } from "../../packages/database/src";
import {
  HandlerRegistry,
} from "../../packages/core/src/worker/HandleRegistry";
import {
  JobExecutor,
} from "../../packages/core/src/worker/JobExecutor";
import {
  WorkerRuntime,
} from "../../packages/core/src/worker/WorkerRuntime";
import type {
  WorkerContext,
} from "../../packages/core/src/worker/WorkerContext";

async function main() {
  const repository = new JobRepository();

  const registry = new HandlerRegistry();

  registry.register("send-email", async (job) => {
    console.log("================================");
    console.log("📧 Sending Email");
    console.log(job.payload);
    console.log("================================");
  });

  const context: WorkerContext = {
    workerId: crypto.randomUUID(),
  };

  const executor = new JobExecutor(
    registry,
    context
  );

  const worker = new WorkerRuntime(
    repository,
    executor,
    context.workerId
  );

  await worker.start();
}

main().catch(console.error);