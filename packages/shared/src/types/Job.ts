export interface Job {
  id: string;
  queue: string;
  type: string;
  payload: unknown;
  status: string;
  priority: number;
  availableAt: Date;
  leaseUntil: Date | null;
  workerId: string | null;
  attempts: number;
  maxAttempts: number;
  createdAt: Date;
  updatedAt: Date;
}