import type { Job } from "@prisma/client";

import { HandlerRegistry } from "./HandleRegistry";
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