import { EventBus } from "../events/EventBus";
import { QueueEvent } from "../events/QueueEvents";
import { MetricsSnapshot } from "./MetricSnapshot";

export class MetricsCollector {
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

  constructor(private readonly eventBus: EventBus) {}

  start(): void {
    this.eventBus.on(QueueEvent.WORKER_STARTED, () => {
      this.metrics.workersStarted++;
    });

    this.eventBus.on(QueueEvent.WORKER_STOPPED, () => {
      this.metrics.workersStopped++;
    });

    this.eventBus.on(QueueEvent.JOB_CLAIMED, () => {
      this.metrics.jobsClaimed++;
    });

    this.eventBus.on(QueueEvent.JOB_STARTED, () => {
      this.metrics.jobsStarted++;
    });

    this.eventBus.on(QueueEvent.JOB_COMPLETED, () => {
      this.metrics.jobsCompleted++;
    });

    this.eventBus.on(QueueEvent.JOB_FAILED, () => {
      this.metrics.jobsFailed++;
    });

    this.eventBus.on(QueueEvent.JOB_RETRY, () => {
      this.metrics.jobsRetried++;
    });

    this.eventBus.on(QueueEvent.JOB_DLQ, () => {
      this.metrics.jobsInDLQ++;
    });

    this.eventBus.on(QueueEvent.JOB_RECOVERED, () => {
      this.metrics.jobsRecovered++;
    });

    this.eventBus.on(QueueEvent.LEASE_RENEWED, () => {
      this.metrics.leaseRenewals++;
    });
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