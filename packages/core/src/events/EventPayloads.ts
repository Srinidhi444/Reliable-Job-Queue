import type { Job } from "@reliable-job-queue/shared";

export interface JobEventPayload {
  job: Job;
}

export interface JobFailedEventPayload extends JobEventPayload {
  error: string;
}

export interface WorkerEventPayload {
  workerId: string;
}

export interface LeaseEventPayload {
  jobId: string;
  workerId: string;
}