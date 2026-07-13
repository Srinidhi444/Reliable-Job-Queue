import { JobAttempt, JobStatus } from "@prisma/client";
import { prisma } from "../client";

export class JobAttemptRepository {
  /**
   * Creates a new execution attempt.
   */
  async createAttempt(
    jobId: string,
    workerId: string,
    attemptNumber: number
  ): Promise<JobAttempt> {
    return prisma.jobAttempt.create({
      data: {
        jobId,
        workerId,
        attemptNumber,
        status: JobStatus.PROCESSING,
      },
    });
  }

  /**
   * Marks an attempt as completed.
   */
  async completeAttempt(
    attemptId: string
  ): Promise<JobAttempt> {
    return prisma.jobAttempt.update({
      where: {
        id: attemptId,
      },
      data: {
        status: JobStatus.COMPLETED,
        finishedAt: new Date(),
      },
    });
  }

  /**
   * Marks an attempt as failed.
   */
  async failAttempt(
    attemptId: string,
    error: string
  ): Promise<JobAttempt> {
    return prisma.jobAttempt.update({
      where: {
        id: attemptId,
      },
      data: {
        status: JobStatus.FAILED,
        error,
        finishedAt: new Date(),
      },
    });
  }

  /**
   * Returns the execution history
   * of a job.
   */
  async getAttempts(
    jobId: string
  ): Promise<JobAttempt[]> {
    return prisma.jobAttempt.findMany({
      where: {
        jobId,
      },
      orderBy: {
        attemptNumber: "asc",
      },
    });
  }
}