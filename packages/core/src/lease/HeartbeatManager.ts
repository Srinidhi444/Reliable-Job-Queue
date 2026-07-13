import { LeaseManager } from "./LeaseManager";

export class HeartbeatManager {
  private readonly timers = new Map<
    string,
    NodeJS.Timeout
  >();

  /**
   * Consecutive heartbeat failures
   * for each running job.
   */
  private readonly failures = new Map<
    string,
    number
  >();

  private static readonly MAX_FAILURES = 3;

  constructor(
    private readonly leaseManager: LeaseManager,
    private readonly workerId: string,
    private readonly heartbeatIntervalMs: number,
  
  ) {}

  start(jobId: string): void {
    if (this.timers.has(jobId)) {
      return;
    }

    this.failures.set(jobId, 0);

    const timer = setInterval(async () => {
      try {
        const renewed =
          await this.leaseManager.extendLease(
            jobId,
            this.workerId
          );

        if (!renewed) {
          console.warn(
            `Lease renewal rejected for job ${jobId}. Stopping heartbeat.`
          );

          this.stop(jobId);
          return;
        }

        // Successful renewal resets failure count.
        this.failures.set(jobId, 0);

        console.log(
          `[Heartbeat] Job ${jobId} renewed at ${new Date().toISOString()}`
        );

        
      } catch (error) {
        const failures =
          (this.failures.get(jobId) ?? 0) + 1;

        this.failures.set(jobId, failures);

        console.error(
          `Heartbeat failed for job ${jobId} (${failures}/${HeartbeatManager.MAX_FAILURES}):`,
          error
        );

        if (
          failures >=
          HeartbeatManager.MAX_FAILURES
        ) {
          console.error(
            `Heartbeat permanently stopped for job ${jobId} after ${failures} consecutive failures.`
          );

          this.stop(jobId);
        }
      }
    }, this.heartbeatIntervalMs);

    timer.unref();

    this.timers.set(jobId, timer);
  }

  stop(jobId: string): void {
    const timer = this.timers.get(jobId);

    if (!timer) {
      return;
    }

    clearInterval(timer);

    this.timers.delete(jobId);
    this.failures.delete(jobId);
  }

  stopAll(): void {
    for (const timer of this.timers.values()) {
      clearInterval(timer);
    }

    this.timers.clear();
    this.failures.clear();
  }
}