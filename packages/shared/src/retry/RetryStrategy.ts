export interface RetryStrategy {
  getDelay(attempt: number): number;
}