import { JobRepository } from "@reliable-job-queue/database";

import { LeaseManager } from "../lease/LeaseManager";
import { RecoveryManager } from "./RecoveryManager";
import { SchedulerRuntime } from "./SchedulerRuntime";

import { EventBus } from "../events/EventBus";

export interface SchedulerOptions {
  /**
   * How often to check for expired leases.
   *
   * Default: 5000ms
   */
  recoveryInterval?: number;

  /**
   * Lease duration used when releasing
   * recovered jobs.
   *
   * Default: 30000ms
   */
  leaseDuration?: number;
}

export class Scheduler {
  private readonly jobRepository: JobRepository;
  private readonly leaseManager: LeaseManager;
  private readonly eventBus: EventBus;

  private readonly recoveryManager: RecoveryManager;
  private readonly runtime: SchedulerRuntime;

  constructor(
    private readonly options: SchedulerOptions = {}
  ) {
    this.jobRepository = new JobRepository();

    this.eventBus = new EventBus();

    this.leaseManager = new LeaseManager(
      this.jobRepository,
      options.leaseDuration ?? 30_000
    );

    this.recoveryManager = new RecoveryManager(
      this.leaseManager,
      this.jobRepository,
      this.eventBus,
      options.recoveryInterval ?? 5_000
    );

    this.runtime = new SchedulerRuntime(
      this.recoveryManager
    );
  }

  /**
   * Starts the scheduler.
   */
  async start(): Promise<void> {
    await this.runtime.start();
  }

  /**
   * Stops the scheduler.
   */
  async stop(): Promise<void> {
    await this.runtime.stop();
  }

  /**
   * Subscribe to scheduler events.
   */
  on<T>(
    event: string,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.eventBus.on(event as any, listener);
  }

  /**
   * Remove a listener.
   */
  off<T>(
    event: string,
    listener: (payload: T) => void | Promise<void>
  ): void {
    this.eventBus.off(event as any, listener);
  }
}