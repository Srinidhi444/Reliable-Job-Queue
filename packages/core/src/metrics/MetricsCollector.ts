import { EventBus } from "../events/EventBus";
import { QueueEvent } from "../events/QueueEvents";
import { MetricsSnapshot } from "./MetricSnapshot";

export class MetricsCollector {
  private started = false;

  private readonly metrics: MetricsSnapshot = {
    jobsCompleted: 0,
    jobsFailed: 0,
    jobsRetried: 0,
    jobsInDLQ: 0,

    jobsStarted: 0,
    jobsClaimed: 0,
    jobsRecovered: 0,

    workersStarted: 0,
    workersStopped: 0,

    leaseRenewals: 0,
  };

  constructor(
    private readonly eventBus: EventBus
  ) {}

  // -------------------------
  // Event listeners
  // -------------------------

  private readonly onWorkerStarted = () => {
    this.metrics.workersStarted++;
  };

  private readonly onWorkerStopped = () => {
    this.metrics.workersStopped++;
  };

  private readonly onJobClaimed = () => {
    this.metrics.jobsClaimed++;
  };

  private readonly onJobStarted = () => {
    this.metrics.jobsStarted++;
  };

  private readonly onJobCompleted = () => {
    this.metrics.jobsCompleted++;
  };

  private readonly onJobFailed = () => {
    this.metrics.jobsFailed++;
  };

  private readonly onJobRetry = () => {
    this.metrics.jobsRetried++;
  };

  private readonly onJobDLQ = () => {
    this.metrics.jobsInDLQ++;
  };

  private readonly onJobRecovered = () => {
    this.metrics.jobsRecovered++;
  };

  private readonly onLeaseRenewed = () => {
    this.metrics.leaseRenewals++;
  };

  /**
   * Starts collecting metrics.
   * Safe to call multiple times.
   */
  start(): void {
    if (this.started) {
      return;
    }

    this.started = true;

    this.eventBus.on(
      QueueEvent.WORKER_STARTED,
      this.onWorkerStarted
    );

    this.eventBus.on(
      QueueEvent.WORKER_STOPPED,
      this.onWorkerStopped
    );

    this.eventBus.on(
      QueueEvent.JOB_CLAIMED,
      this.onJobClaimed
    );

    this.eventBus.on(
      QueueEvent.JOB_STARTED,
      this.onJobStarted
    );

    this.eventBus.on(
      QueueEvent.JOB_COMPLETED,
      this.onJobCompleted
    );

    this.eventBus.on(
      QueueEvent.JOB_FAILED,
      this.onJobFailed
    );

    this.eventBus.on(
      QueueEvent.JOB_RETRY,
      this.onJobRetry
    );

    this.eventBus.on(
      QueueEvent.JOB_DLQ,
      this.onJobDLQ
    );

    this.eventBus.on(
      QueueEvent.JOB_RECOVERED,
      this.onJobRecovered
    );

    this.eventBus.on(
      QueueEvent.LEASE_RENEWED,
      this.onLeaseRenewed
    );
  }

  /**
   * Stops collecting metrics.
   */
  stop(): void {
    if (!this.started) {
      return;
    }

    this.started = false;

    this.eventBus.off(
      QueueEvent.WORKER_STARTED,
      this.onWorkerStarted
    );

    this.eventBus.off(
      QueueEvent.WORKER_STOPPED,
      this.onWorkerStopped
    );

    this.eventBus.off(
      QueueEvent.JOB_CLAIMED,
      this.onJobClaimed
    );

    this.eventBus.off(
      QueueEvent.JOB_STARTED,
      this.onJobStarted
    );

    this.eventBus.off(
      QueueEvent.JOB_COMPLETED,
      this.onJobCompleted
    );

    this.eventBus.off(
      QueueEvent.JOB_FAILED,
      this.onJobFailed
    );

    this.eventBus.off(
      QueueEvent.JOB_RETRY,
      this.onJobRetry
    );

    this.eventBus.off(
      QueueEvent.JOB_DLQ,
      this.onJobDLQ
    );

    this.eventBus.off(
      QueueEvent.JOB_RECOVERED,
      this.onJobRecovered
    );

    this.eventBus.off(
      QueueEvent.LEASE_RENEWED,
      this.onLeaseRenewed
    );
  }

  getSnapshot(): MetricsSnapshot {
    return { ...this.metrics };
  }

  reset(): void {
    Object.keys(this.metrics).forEach((key) => {
      this.metrics[key as keyof MetricsSnapshot] = 0;
    });
  }
}