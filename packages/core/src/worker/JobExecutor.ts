import type { Job } from "@reliable-job-queue/shared";

import { HandlerRegistry } from "./HandlerRegistry";
import type { WorkerContext } from "./WorkerContext";

export class JobExecutor {
  constructor(
    private readonly registry: HandlerRegistry,
    private readonly context: WorkerContext
  ) {}

  async execute(job: Job): Promise<void> {
    const handler = this.registry.get(job.type);

    await handler(job, this.context);
  }
}