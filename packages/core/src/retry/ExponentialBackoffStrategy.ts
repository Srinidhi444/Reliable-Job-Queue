import { RetryStrategy } from "@reliable-job-queue/shared";

export class ExponentialBackoffStrategy
  implements RetryStrategy
{
  constructor(
    private readonly baseDelayMs = 5000
  ) {}

  getDelay(attempt: number): number {
    return this.baseDelayMs * Math.pow(2, attempt);
  }
}