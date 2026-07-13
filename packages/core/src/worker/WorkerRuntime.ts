import os from "node:os";

import type { Job, RetryStrategy } from "@reliable-job-queue/shared";
import {
  JobRepository,
  WorkerRepository,
  JobAttemptRepository,
} from "@reliable-job-queue/database";

import { JobExecutor } from "./JobExecutor";
import { LeaseManager } from "../lease/LeaseManager";
import { HeartbeatManager } from "../lease/HeartbeatManager";
import { WorkerOptions } from "../types/WorkerOptions";

import { EventBus } from "../events/EventBus";
import { QueueEvent } from "../events/QueueEvents";

export interface StopOptions {
  /**
   * Max time (ms) to wait for active jobs to finish before forcing shutdown.
   */
  timeout?: number;
}

export class WorkerRuntime {
  private running = false;

  /**
   * Number of jobs currently executing.
   */
  private activeJobs = 0;

  /**
   * Worker heartbeat timer.
   */
  private workerHeartbeatTimer?: NodeJS.Timeout;

  /**
   * Renews leases for running jobs.
   */
  private readonly heartbeatManager: HeartbeatManager;

  /**
   * Resolves once all active jobs have finished during shutdown.
   */
  private shutdownResolver?: () => void;

  private shutdownPromise?: Promise<void>;

  constructor(
    private readonly repository: JobRepository,
    private readonly workerRepository: WorkerRepository,
    private readonly jobAttemptRepository: JobAttemptRepository,
    private readonly leaseManager: LeaseManager,
    private readonly executor: JobExecutor,
    private readonly workerId: string,
    private readonly options: WorkerOptions,
    private readonly retryStrategy: RetryStrategy,
    private readonly eventBus: EventBus
  ) {
    this.heartbeatManager = new HeartbeatManager(
      this.leaseManager,
      this.workerId,
      this.options.heartbeatInterval ?? 10_000,
      this.eventBus
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

    this.eventBus.emit(QueueEvent.WORKER_STARTED, {
      workerId: this.workerId,
    });

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

  public async stop(options: StopOptions = {}): Promise<void> {
    if (!this.running) {
      return;
    }

    console.log(
      `Worker ${this.workerId} is shutting down gracefully...`
    );

    // Stop polling for new jobs
    this.running = false;

    // Stop worker heartbeat
    this.stopWorkerHeartbeat();

    // If jobs are still running, wait for them (with optional timeout)
    if (this.activeJobs > 0) {
      console.log(
        `Waiting for ${this.activeJobs} running job(s) to finish...`
      );

      this.shutdownPromise = new Promise<void>((resolve) => {
        this.shutdownResolver = resolve;
      });

      const timeout = options.timeout;

      if (timeout && timeout > 0) {
        await this.waitWithTimeout(this.shutdownPromise, timeout);
      } else {
        await this.shutdownPromise;
      }
    }

    // Stop all job heartbeats
    this.heartbeatManager.stopAll();

    await this.workerRepository.markOffline(this.workerId);

    console.log(`Worker ${this.workerId} stopped.`);

    this.eventBus.emit(QueueEvent.WORKER_STOPPED, {
      workerId: this.workerId,
    });
  }

  private async waitWithTimeout(
    promise: Promise<void>,
    timeoutMs: number
  ): Promise<void> {
    let timer: NodeJS.Timeout | undefined;

    const timeoutPromise = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        console.warn(
          `Worker ${this.workerId} shutdown timed out after ${timeoutMs}ms. ` +
            `${this.activeJobs} job(s) still running — forcing shutdown.`
        );
        resolve();
      }, timeoutMs);
    });

    try {
      await Promise.race([promise, timeoutPromise]);
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
    }
  }

  // ----------------------------------------------------
  // Worker heartbeat
  // ----------------------------------------------------

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

  // ----------------------------------------------------
  // Polling
  // ----------------------------------------------------

  private async poll(): Promise<void> {
    const concurrency = this.options.concurrency ?? 1;

    const availableSlots = concurrency - this.activeJobs;

    if (availableSlots <= 0) {
      return;
    }

    const jobs = await this.repository.findAvailableJobs(
      availableSlots
    );

    for (const job of jobs) {
      this.activeJobs++;

      void this.processJob(job);
    }
  }

  private async processJob(job: Job): Promise<void> {
    const claimed = await this.leaseManager.claimLease(
      job.id,
      this.workerId
    );

    if (!claimed) {
      this.activeJobs--;
      this.checkShutdownComplete();
      return;
    }

    this.eventBus.emit(QueueEvent.JOB_CLAIMED, { job });

    this.heartbeatManager.start(job.id);

    // Attempt number is the job's next attempt (existing attempts + 1)
    const attemptNumber = job.attempts + 1;

    const attempt = await this.jobAttemptRepository.createAttempt(
      job.id,
      this.workerId,
      attemptNumber
    );

    try {
      this.eventBus.emit(QueueEvent.JOB_STARTED, { job });

      await this.executor.execute(job);

      await this.repository.completeJob(job.id);

      await this.jobAttemptRepository.completeAttempt(attempt.id);

      console.log(`Completed job ${job.id}`);

      this.eventBus.emit(QueueEvent.JOB_COMPLETED, { job });
    } catch (error) {
      console.error(`Job ${job.id} failed:`, error);

      const errorMessage =
        error instanceof Error ? error.message : String(error);

      await this.jobAttemptRepository.failAttempt(
        attempt.id,
        errorMessage
      );

      this.eventBus.emit(QueueEvent.JOB_FAILED, {
        job,
        error: errorMessage,
      });

      const updatedJob = await this.repository.retryJob(
        job.id,
        this.retryStrategy,
        errorMessage
      );

      if (updatedJob.status === "DLQ") {
        console.log(
          `Job ${job.id} moved to the Dead Letter Queue.`
        );

        this.eventBus.emit(QueueEvent.JOB_DLQ, {
          job: updatedJob,
        });
      } else {
        console.log(
          `Retry #${updatedJob.attempts} scheduled at ${updatedJob.availableAt.toISOString()}`
        );

        this.eventBus.emit(QueueEvent.JOB_RETRY, {
          job: updatedJob,
        });
      }
    } finally {
      this.heartbeatManager.stop(job.id);

      this.activeJobs--;

      this.checkShutdownComplete();
    }
  }

  private checkShutdownComplete(): void {
    if (!this.running && this.activeJobs === 0) {
      this.shutdownResolver?.();
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}