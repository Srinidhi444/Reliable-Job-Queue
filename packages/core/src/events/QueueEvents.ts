export enum QueueEvent {
  // Worker lifecycle
  WORKER_STARTED = "worker.started",
  WORKER_STOPPED = "worker.stopped",

  // Job lifecycle
  JOB_CLAIMED = "job.claimed",
  JOB_STARTED = "job.started",
  JOB_COMPLETED = "job.completed",
  JOB_FAILED = "job.failed",
  JOB_RETRY = "job.retry",
  JOB_DLQ = "job.dlq",

  // Lease lifecycle
  LEASE_RENEWED = "lease.renewed",
  LEASE_EXPIRED = "lease.expired",

  // Recovery
  JOB_RECOVERED = "job.recovered",
}