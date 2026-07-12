import crypto from "node:crypto";

import {
  JobRepository,
  WorkerRepository,
} from "@reliable-job-queue/database";

import { HandlerRegistry } from "../worker/HandleRegistry";
import { JobExecutor } from "../worker/JobExecutor";
import { WorkerRuntime } from "../worker/WorkerRuntime";

import { LeaseManager } from "../lease/LeaseManager";
import { ExponentialBackoffStrategy } from "../retry/ExponentialBackoffStrategy";

import { QueueOptions } from "../types/QueueOptions";
import { WorkerOptions } from "../types/WorkerOptions";

import type { RetryStrategy } from "@reliable-job-queue/shared";
import type { Prisma } from "@prisma/client";
import { EnqueueJobInput } from "../types/EnqueueJobInput";

import { EventBus } from "../events/EventBus";
import { QueueEvent } from "../events/QueueEvents";

export class Queue {
  private readonly jobRepository: JobRepository;
  private readonly workerRepository: WorkerRepository;
  private readonly handlerRegistry: HandlerRegistry;

  /**
   * Single shared event bus for the entire queue instance.
   * Injected into WorkerRuntime, RecoveryManager, SchedulerRuntime, etc.
   */
  private readonly eventBus: EventBus;

  private readonly workerId: string;
  private readonly workerOptions: WorkerOptions;
  private readonly retryStrategy: RetryStrategy;

  private workerRuntime?: WorkerRuntime;

  constructor(options: QueueOptions = {}) {
    this.jobRepository = new JobRepository();
    this.workerRepository = new WorkerRepository();
    this.handlerRegistry = new HandlerRegistry();
    this.eventBus = new EventBus();

    this.workerId = options.workerId ?? crypto.randomUUID();

    this.workerOptions = options.worker ?? {};

    this.retryStrategy =
      options.retryStrategy ?? new ExponentialBackoffStrategy();
  }

  /**
   * Register a job handler.
   */
  register(
    type: string,
    handler: (job: any) => Promise<void>
  ): void {
    this.handlerRegistry.register(type, handler);
  }

  /**
   * Subscribe to a queue event.
   */
  public on<T>(
    event: QueueEvent,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.eventBus.on(event, listener);
  }

  /**
   * Unsubscribe from a queue event.
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
        ? new Date(Date.now() + input.options.delay)
        : new Date());

    return this.jobRepository.createJob({
      queue: input.queue,
      type: input.type,
      payload: input.payload,
      priority: input.options?.priority,
      maxAttempts: input.options?.maxAttempts,
      availableAt,
    });
  }

  /**
   * Starts a worker.
   */
  async startWorker(): Promise<void> {
    if (this.workerRuntime) {
      throw new Error("Worker is already running.");
    }

    const leaseManager = new LeaseManager(
      this.jobRepository,
      this.workerOptions.leaseDuration ?? 30_000
    );

    const executor = new JobExecutor(this.handlerRegistry, {
      workerId: this.workerId,
    });

    this.workerRuntime = new WorkerRuntime(
      this.jobRepository,
      this.workerRepository,
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
   * Stops the running worker.
   */
  async stopWorker(): Promise<void> {
    if (!this.workerRuntime) {
      return;
    }

    await this.workerRuntime.stop();
    this.workerRuntime = undefined;
  }

  // -----------------------------
  // Internal getters
  // -----------------------------

  getJobRepository(): JobRepository {
    return this.jobRepository;
  }

  getWorkerRepository(): WorkerRepository {
    return this.workerRepository;
  }

  getHandlerRegistry(): HandlerRegistry {
    return this.handlerRegistry;
  }

  getWorkerId(): string {
    return this.workerId;
  }

  getWorkerOptions(): WorkerOptions {
    return this.workerOptions;
  }

  getRetryStrategy(): RetryStrategy {
    return this.retryStrategy;
  }

  getEventBus(): EventBus {
    return this.eventBus;
  }
}