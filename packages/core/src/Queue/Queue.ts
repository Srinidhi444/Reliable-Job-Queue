import crypto from "node:crypto";

import {
    JobAttemptRepository,
  JobRepository,
  WorkerRepository,
} from "@reliable-job-queue/database";

import { HandlerRegistry } from "../worker/HandlerRegistry";
import { JobExecutor } from "../worker/JobExecutor";
import { WorkerRuntime } from "../worker/WorkerRuntime";

import { LeaseManager } from "../lease/LeaseManager";
import { ExponentialBackoffStrategy } from "../retry/ExponentialBackoffStrategy";

import { QueueOptions } from "../types/QueueOptions";
import { WorkerOptions } from "../types/WorkerOptions";
import { EnqueueJobInput } from "../types/EnqueueJobInput";

import { EventBus } from "../events/EventBus";
import { QueueEvent } from "../events/QueueEvents";

import { MetricsCollector } from "../metrics/MetricsCollector";
import { MetricsSnapshot } from "../metrics/MetricSnapshot";

import type { Job, RetryStrategy } from "@reliable-job-queue/shared";

export class Queue {
  private readonly jobRepository: JobRepository;
  private readonly jobAttemptRepository: JobAttemptRepository;
  private readonly workerRepository: WorkerRepository;
  private readonly handlerRegistry: HandlerRegistry;

  /**
   * Shared event bus for this queue instance.
   */
  private readonly eventBus: EventBus;

  /**
   * Runtime metrics collector.
   */
  private readonly metricsCollector: MetricsCollector;

  private readonly workerId: string;
  private readonly workerOptions: WorkerOptions;
  private readonly retryStrategy: RetryStrategy;

  private workerRuntime?: WorkerRuntime;

  constructor(options: QueueOptions = {}) {
    this.jobRepository = new JobRepository();
    this.workerRepository = new WorkerRepository();
    this.handlerRegistry = new HandlerRegistry();
    this.jobAttemptRepository = new JobAttemptRepository();

    this.eventBus = new EventBus();

    this.metricsCollector = new MetricsCollector(
      this.eventBus
    );

    this.metricsCollector.start();

    this.workerId =
      options.workerId ?? crypto.randomUUID();

    this.workerOptions =
      options.worker ?? {};

    this.retryStrategy =
      options.retryStrategy ??
      new ExponentialBackoffStrategy();
  }

  /**
   * Register a handler.
   */
  register(
    type: string,
    handler: (job: Job) => Promise<void>
  ): void {
    this.handlerRegistry.register(type, handler);
  }

  /**
   * Subscribe to queue events.
   */
  public on<T>(
    event: QueueEvent,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.eventBus.on(event, listener);
  }

  /**
   * Remove an event listener.
   */
  public off<T>(
    event: QueueEvent,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.eventBus.off(event, listener);
  }

  /**
   * Enqueue a new job.
   */
  async enqueue(input: EnqueueJobInput) {
    const availableAt =
      input.options?.runAt ??
      (input.options?.delay
        ? new Date(
            Date.now() + input.options.delay
          )
        : new Date());

    return this.jobRepository.createJob({
      queue: input.queue,
      type: input.type,
      payload: input.payload,
      priority: input.options?.priority,
      maxAttempts:
        input.options?.maxAttempts,
      availableAt,
    });
  }

  /**
   * Starts a worker.
   */
  async startWorker(): Promise<void> {
    if (this.workerRuntime) {
      throw new Error(
        "Worker is already running."
      );
    }

    const leaseManager =
      new LeaseManager(
        this.jobRepository,
        this.workerOptions
          .leaseDuration ?? 30_000
      );

    const executor =
      new JobExecutor(
        this.handlerRegistry,
        {
          workerId: this.workerId,
        }
      );

    this.workerRuntime =
      new WorkerRuntime(
        this.jobRepository,
        this.workerRepository,
        this.jobAttemptRepository,
        leaseManager,
        executor,
        this.workerId,
        this.workerOptions,
        this.retryStrategy,
        this.eventBus
      );

    await this.workerRuntime.start();
  }

  /**
   * Stops the worker.
   */
  async stopWorker(): Promise<void> {
    if (!this.workerRuntime) {
      return;
    }

    await this.workerRuntime.stop();

    this.workerRuntime = undefined;
  }

  /**
   * Runtime metrics.
   */
  public metrics(): MetricsSnapshot {
    return this.metricsCollector.getSnapshot();
  }

  /**
   * Queue statistics.
   *
   * Pending
   * Processing
   * Completed
   * Failed
   * DLQ
   */
  public async stats() {
    return this.jobRepository.getQueueStats();
  }

  /**
   * Returns all jobs currently
   * in the Dead Letter Queue.
   */
  public async getDLQ() {
    return this.jobRepository.getDLQJobs();
  }

  /**
   * Replay a DLQ job.
   *
   * The job is reset back to
   * PENDING with attempts = 0.
   */
  public async replay(jobId: string) {
    return this.jobRepository.replayDLQJob(jobId);
  }

  // -------------------------------------------------
  // Internal getters
  // -------------------------------------------------

  private getJobRepository(): JobRepository {
    return this.jobRepository;
  }

  private getWorkerRepository(): WorkerRepository {
    return this.workerRepository;
  }

  private getHandlerRegistry(): HandlerRegistry {
    return this.handlerRegistry;
  }

  private getWorkerId(): string {
    return this.workerId;
  }

  private getWorkerOptions(): WorkerOptions {
    return this.workerOptions;
  }

  private getRetryStrategy(): RetryStrategy {
    return this.retryStrategy;
  }

  private getEventBus(): EventBus {
    return this.eventBus;
  }

  private getMetricsCollector(): MetricsCollector {
    return this.metricsCollector;
  }
}