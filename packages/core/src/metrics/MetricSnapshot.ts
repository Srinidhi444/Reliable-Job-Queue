export interface MetricsSnapshot {
  jobsCompleted: number;
  jobsFailed: number;
  jobsRetried: number;
  jobsInDLQ: number;

  jobsStarted: number;
  jobsClaimed: number;
  jobsRecovered: number;

  workersStarted: number;
  workersStopped: number;

  leaseRenewals: number;
}