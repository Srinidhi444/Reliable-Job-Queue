import { prisma } from "../client";
import { Job, JobStatus, Prisma } from "@prisma/client";
import { RetryStrategy } from "@reliable-job-queue/shared/";
export interface CreateJobInput {
  queue: string;
  type: string;
  payload: Prisma.InputJsonValue;
  priority?: number;
  availableAt?: Date;
  maxAttempts?: number;
}

export class JobRepository {
  async createJob(data: CreateJobInput): Promise<Job> {
    return prisma.job.create({
      data: {
        queue: data.queue,
        type: data.type,
        payload: data.payload,
        status: JobStatus.PENDING,
        priority: data.priority ?? 0,
        availableAt: data.availableAt ?? new Date(),
        attempts: 0,
        maxAttempts: data.maxAttempts ?? 3,
      },
    });
  }

  async getJobById(id: string): Promise<Job | null> {
    return prisma.job.findUnique({
      where: { id },
    });
  }

  async getJobs(): Promise<Job[]> {
    return prisma.job.findMany({
      orderBy: {
        createdAt: "desc",
      },
    });
  }

  /**
   * Returns jobs that are ready to execute.
   */
  async findAvailableJobs(limit = 10): Promise<Job[]> {
    return prisma.job.findMany({
      where: {
        status: JobStatus.PENDING,
        availableAt: {
          lte: new Date(),
        },
        leaseUntil: null,
      },
      orderBy: [
        {
          priority: "desc",
        },
        {
          createdAt: "asc",
        },
      ],
      take: limit,
    });
  }

  /**
   * Atomically claims a job.
   * Returns true only if this worker successfully acquired the job.
   */
  async claimJob(
    jobId: string,
    workerId: string,
    leaseDurationMs = 30000
  ): Promise<boolean> {
    const leaseUntil = new Date(Date.now() + leaseDurationMs);

    const result = await prisma.job.updateMany({
      where: {
        id: jobId,
        status: JobStatus.PENDING,
        leaseUntil: null,
        availableAt: {
          lte: new Date(),
        },
      },
      data: {
        status: JobStatus.PROCESSING,
        workerId,
        leaseUntil,
      },
    });

    return result.count === 1;
  }

  /**
   * Marks a job as completed.
   */
  async completeJob(jobId: string): Promise<Job> {
    return prisma.job.update({
      where: {
        id: jobId,
      },
      data: {
        status: JobStatus.COMPLETED,
        workerId: null,
        leaseUntil: null,
      },
    });
  }

  /**
   * Marks a job as failed.
   * Retry logic will be added later.
   */
  async failJob(jobId: string): Promise<Job> {
    return prisma.job.update({
      where: {
        id: jobId,
      },
      data: {
        status: JobStatus.FAILED,
        workerId: null,
        leaseUntil: null,
      },
    });
  }

  async extendLease(
  jobId: string,
  workerId: string,
  leaseDurationMs: number
): Promise<boolean> {
  const leaseUntil = new Date(Date.now() + leaseDurationMs);

  const result = await prisma.job.updateMany({
    where: {
      id: jobId,
      workerId,
      status: JobStatus.PROCESSING,
    },
    data: {
      leaseUntil,
    },
  });

  return result.count === 1;
}

async releaseLease(jobId: string): Promise<Job> {
  return prisma.job.update({
    where: {
      id: jobId,
    },
    data: {
      status: JobStatus.PENDING,
      workerId: null,
      leaseUntil: null,
    },
  });
}

async findExpiredLeases(): Promise<Job[]> {
  const now = new Date();

  return prisma.job.findMany({
    where: {
      status: JobStatus.PROCESSING,
      leaseUntil: {
        lt: now,
      },
    },
    orderBy: {
      leaseUntil: "asc",
    },
  });
}


  async retryJob(
  jobId: string,
  retryStrategy: RetryStrategy
): Promise<Job> {
  const job = await prisma.job.findUnique({
    where: {
      id: jobId,
    },
  });

  if (!job) {
    throw new Error(`Job ${jobId} not found.`);
  }

  const attempts = job.attempts + 1;

  if (attempts >= job.maxAttempts) {
    return prisma.job.update({
      where: {
        id: jobId,
      },
      data: {
        attempts,
        status: JobStatus.FAILED,
        workerId: null,
        leaseUntil: null,
      },
    });
  }

  const delay = retryStrategy.getDelay(attempts);

  return prisma.job.update({
    where: {
      id: jobId,
    },
    data: {
      attempts,
      status: JobStatus.PENDING,
      workerId: null,
      leaseUntil: null,
      availableAt: new Date(Date.now() + delay),
    },
  });
}
}