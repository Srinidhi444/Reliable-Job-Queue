import { RetryStrategy } from "@reliable-job-queue/shared";
import { WorkerOptions } from "./WorkerOptions";

export interface QueueOptions {
  workerId?: string;
  worker?: WorkerOptions;
  retryStrategy?: RetryStrategy;
}