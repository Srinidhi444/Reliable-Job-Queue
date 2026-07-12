import { JobRepository } from "@reliable-job-queue/database";
import { LeaseManager } from "../lease/LeaseManager";

import { EventBus } from "../events/EventBus";
import { QueueEvent } from "../events/QueueEvents";

export class RecoveryManager {
  private timer?: NodeJS.Timeout;

  constructor(
    private readonly leaseManager: LeaseManager,
    private readonly jobRepository: JobRepository,
    private readonly eventBus: EventBus,
    private readonly intervalMs = 5000
  ) {}

  public start(): void {
    if (this.timer) {
      return;
    }

    this.timer = setInterval(async () => {
      try {
        await this.recoverExpiredJobs();
      } catch (error) {
        console.error("Recovery manager failed:", error);
      }
    }, this.intervalMs);

    // this.timer.unref();
  }

  public stop(): void {
    if (!this.timer) {
      return;
    }

    clearInterval(this.timer);
    this.timer = undefined;
  }

  private async recoverExpiredJobs(): Promise<void> {
    console.log("Checking for expired jobs...");

    const jobs = await this.jobRepository.findExpiredLeases();

    console.log("Expired jobs found:", jobs.length);

    for (const job of jobs) {
      console.log("Recovering:", job.id);

      await this.leaseManager.releaseLease(job.id);

      console.log("Recovered:", job.id);

      this.eventBus.emit(QueueEvent.JOB_RECOVERED, { job });
    }
  }
}