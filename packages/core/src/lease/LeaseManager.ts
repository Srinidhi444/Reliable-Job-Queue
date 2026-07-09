import { JobRepository } from "@reliable-job-queue/database";

export class LeaseManager {
  constructor(
    private readonly repository: JobRepository,
    private readonly leaseDurationMs:number
  ) {}

  async claimLease(
    jobId: string,
    workerId: string
  ): Promise<boolean> {
    return this.repository.claimJob(
      jobId,
      workerId,
      this.leaseDurationMs
    );
  }

  async extendLease(
    jobId: string,
    workerId: string
  ): Promise<boolean> {
    return this.repository.extendLease(
      jobId,
      workerId,
      this.leaseDurationMs
    );
  }

  async releaseLease(jobId: string): Promise<void> {
    await this.repository.releaseLease(jobId);
  }

  getLeaseDuration(): number {
    return this.leaseDurationMs;
  }
}