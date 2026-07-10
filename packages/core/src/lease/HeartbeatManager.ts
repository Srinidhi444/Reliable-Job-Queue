import { LeaseManager } from "./LeaseManager";

export class HeartbeatManager {
  private readonly timers = new Map<string, NodeJS.Timeout>();

  constructor(
    private readonly leaseManager: LeaseManager,
    private readonly workerId: string,
    private readonly heartbeatIntervalMs: number
  ) {}

  start(jobId: string): void {
    // Prevent duplicate heartbeats for the same job
    if (this.timers.has(jobId)) {
      return;
    }

    const timer = setInterval(async () => {
      try {
        const renewed = await this.leaseManager.extendLease(
          jobId,
          this.workerId
        );
        console.log(
        `[Heartbeat] Job ${jobId} renewed: ${renewed} at ${new Date().toISOString()}`
        );

        if (!renewed) {
          console.warn(
            `Failed to renew lease for job ${jobId}. Stopping heartbeat.`
          );

          this.stop(jobId);
        }
      } catch (error) {
        console.error(
          `Heartbeat failed for job ${jobId}:`,
          error
        );
      }
    }, this.heartbeatIntervalMs);

    // Don't keep the Node process alive just because of this timer
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
  }

  stopAll(): void {
    for (const [jobId, timer] of this.timers) {
      clearInterval(timer);
    }

    this.timers.clear();
  }
}