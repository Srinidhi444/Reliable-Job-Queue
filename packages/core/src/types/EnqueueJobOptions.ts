export interface EnqueueJobOptions {
  priority?: number;
  delay?: number;
  runAt?: Date;
  maxAttempts?: number;
}