import { JobRepository } from "@reliable-job-queue/database";

import { LeaseManager } from "../lease/LeaseManager";
import { RecoveryManager } from "./RecoveryManager";
import { SchedulerRuntime } from "./SchedulerRuntime";


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


  private readonly recoveryManager: RecoveryManager;
  private readonly runtime: SchedulerRuntime;

  constructor(
    private readonly options: SchedulerOptions = {}
  ) {
    this.jobRepository = new JobRepository();

    this.leaseManager = new LeaseManager(
      this.jobRepository,
      options.leaseDuration ?? 30_000
    );

    this.recoveryManager = new RecoveryManager(
      this.leaseManager,
      this.jobRepository,
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

 
}