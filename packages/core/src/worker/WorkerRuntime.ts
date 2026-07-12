import os from "node:os";

import type { Job } from "@reliable-job-queue/shared";
import {
  JobRepository,
  WorkerRepository,
} from "@reliable-job-queue/database";

import { JobExecutor } from "./JobExecutor";
import { LeaseManager } from "../lease/LeaseManager";
import { HeartbeatManager } from "../lease/HeartbeatManager";
import { WorkerOptions } from "../types/WorkerOptions";
import { RetryStrategy } from "@reliable-job-queue/shared";

export class WorkerRuntime {
  private running = false;

  // Worker heartbeat (updates Worker table)
  private workerHeartbeatTimer?: NodeJS.Timeout;

  // Job heartbeat (renews job leases)
  private readonly heartbeatManager: HeartbeatManager;

  constructor(
    private readonly repository: JobRepository,
    private readonly workerRepository: WorkerRepository,
    private readonly leaseManager: LeaseManager,
    private readonly executor: JobExecutor,
    private readonly workerId: string,
    private readonly options: WorkerOptions,
    private readonly retryStrategy: RetryStrategy
  ) {
    this.heartbeatManager = new HeartbeatManager(
      this.leaseManager,
      this.workerId,
      this.options.heartbeatInterval ?? 10_000
    );
  }

  public async start(): Promise<void> {
    if (this.running) {
      throw new Error("Worker is already running.");
    }

    this.running = true;

    await this.workerRepository.register({
      id: this.workerId,
      hostname: os.hostname(),
    });

    this.startWorkerHeartbeat();

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

    this.stopWorkerHeartbeat();

    this.heartbeatManager.stopAll();

    await this.workerRepository.markOffline(this.workerId);

    console.log(`Worker ${this.workerId} stopped.`);
  }

  // ------------------------
  // Worker heartbeat
  // ------------------------

  private startWorkerHeartbeat(): void {
    this.workerHeartbeatTimer = setInterval(async () => {
      try {
        await this.workerRepository.heartbeat(this.workerId);
      } catch (error) {
        console.error(
          `Worker heartbeat failed for ${this.workerId}:`,
          error
        );
      }
    }, this.options.heartbeatInterval ?? 10_000);

    this.workerHeartbeatTimer.unref();
  }

  private stopWorkerHeartbeat(): void {
    if (!this.workerHeartbeatTimer) {
      return;
    }

    clearInterval(this.workerHeartbeatTimer);
    this.workerHeartbeatTimer = undefined;
  }

  // ------------------------
  // Polling
  // ------------------------

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

    this.heartbeatManager.start(job.id);

    try {
      await this.executor.execute(job);

      await this.repository.completeJob(job.id);

      console.log(`Completed job ${job.id}`);
    }catch (error) {
  console.error(`Job ${job.id} failed:`, error);

  const errorMessage =
    error instanceof Error
      ? error.message
      : String(error);

  const updatedJob = await this.repository.retryJob(
    job.id,
    this.retryStrategy,
    errorMessage
  );

  if (updatedJob.status === "DLQ") {
    console.log(
      `Job ${job.id} moved to the Dead Letter Queue.`
    );
  } else {
    console.log(
      `Retry #${updatedJob.attempts} scheduled at ${updatedJob.availableAt.toISOString()}`
    );
  }
}finally {
  this.heartbeatManager.stop(job.id);
}
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}