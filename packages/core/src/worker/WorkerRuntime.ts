import os from "node:os";

import type { Job } from "@reliable-job-queue/shared";
import {
  JobRepository,
  WorkerRepository,
} from "@reliable-job-queue/database";

import { JobExecutor } from "./JobExecutor";
import { LeaseManager } from "../lease/LeaseManager";
import { WorkerOptions } from "../types/WorkerOptions";
export class WorkerRuntime {
  private running = false;

  private heartbeatTimer?: NodeJS.Timeout;

  constructor(
    private readonly repository: JobRepository,
    private readonly workerRepository: WorkerRepository,
    private readonly leaseManager: LeaseManager,
    private readonly executor: JobExecutor,
    private readonly workerId: string,
    private readonly options: WorkerOptions
  ) {}

  public async start(): Promise<void> {
    if (this.running) {
      throw new Error("Worker is already running.");
    }

    this.running = true;

    await this.workerRepository.register({
      id: this.workerId,
      hostname: os.hostname(),
    });

    this.startHeartbeat();

    console.log(`Worker ${this.workerId} started.`);

    while (this.running) {
      try {
        await this.poll();
      } catch (error) {
        console.error(
          `Worker ${this.workerId} polling failed:`,
          error
        );
      }

      await this.sleep(this.options.pollingInterval ?? 1000);
    }
  }

  public async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    this.stopHeartbeat();

    await this.workerRepository.markOffline(this.workerId);

    console.log(`Worker ${this.workerId} stopped.`);
  }

  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(async () => {
      try {
        await this.workerRepository.heartbeat(this.workerId);
      } catch (error) {
        console.error(
          `Heartbeat failed for worker ${this.workerId}:`,
          error
        );
      }
    }, this.options.heartbeatInterval ?? 10000);

    this.heartbeatTimer.unref();
  }

  private stopHeartbeat(): void {
    if (!this.heartbeatTimer) {
      return;
    }

    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = undefined;
  }

  private async poll(): Promise<void> {
    const jobs = await this.repository.findAvailableJobs();

    for (const job of jobs) {
      await this.processJob(job);
    }
  }

  private async processJob(job: Job): Promise<void> {
   const claimed = await this.leaseManager.claimLease(
      job.id,
      this.workerId
    );

    if (!claimed) {
      return;
    }

    try {
      await this.executor.execute(job);

      await this.repository.completeJob(job.id);

      console.log(`Completed job ${job.id}`);
    } catch (error) {
      console.error(`Job ${job.id} failed:`, error);

      await this.repository.failJob(job.id);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}