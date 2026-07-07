import type { Job } from "@prisma/client";

import { JobRepository } from "@reliable-job-queue/database";
import { JobExecutor } from "./JobExecutor";

export class WorkerRuntime {
  private running = false;

  constructor(
    private readonly repository: JobRepository,
    private readonly executor: JobExecutor,
    private readonly workerId: string,
    private readonly pollingInterval = 1000
  ) {}

  public async start(): Promise<void> {
    this.running = true;

    console.log(`Worker ${this.workerId} started.`);

    while (this.running) {
      await this.poll();

      await this.sleep(this.pollingInterval);
    }
  }

  public stop(): void {
    this.running = false;

    console.log(`Worker ${this.workerId} stopped.`);
  }

  private async poll(): Promise<void> {
    const jobs = await this.repository.findAvailableJobs();

    for (const job of jobs) {
      await this.processJob(job);
    }
  }

  private async processJob(job: Job): Promise<void> {
    const claimed = await this.repository.claimJob(
      job.id,
      this.workerId
    );

    if (!claimed) {
      return;
    }

    try {
      await this.executor.execute(job);

      await this.repository.completeJob(job.id);
    } catch (error) {
      console.error(
        `Job ${job.id} failed:`,
        error
      );

      await this.repository.failJob(job.id);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}