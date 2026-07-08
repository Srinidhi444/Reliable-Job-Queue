import { WorkerStatus, type Worker } from "@prisma/client";

import { prisma } from "../client";

export interface RegisterWorkerInput {
  id: string;
  hostname: string;
}

export class WorkerRepository {
  async register(input: RegisterWorkerInput): Promise<Worker> {
    return prisma.worker.upsert({
      where: {
        id: input.id,
      },
      create: {
        id: input.id,
        hostname: input.hostname,
        status: WorkerStatus.ONLINE,
        lastHeartbeat: new Date(),
        startedAt: new Date(),
      },
      update: {
        hostname: input.hostname,
        status: WorkerStatus.ONLINE,
        lastHeartbeat: new Date(),
      },
    });
  }

  async heartbeat(workerId: string): Promise<Worker> {
    return prisma.worker.update({
      where: {
        id: workerId,
      },
      data: {
        status: WorkerStatus.ONLINE,
        lastHeartbeat: new Date(),
      },
    });
  }

  async markOffline(workerId: string): Promise<Worker> {
    return prisma.worker.update({
      where: {
        id: workerId,
      },
      data: {
        status: WorkerStatus.OFFLINE,
      },
    });
  }

  async getById(workerId: string): Promise<Worker | null> {
    return prisma.worker.findUnique({
      where: {
        id: workerId,
      },
    });
  }

  async getOnline(): Promise<Worker[]> {
    return prisma.worker.findMany({
      where: {
        status: WorkerStatus.ONLINE,
      },
      orderBy: {
        lastHeartbeat: "desc",
      },
    });
  }

  async findStaleWorkers(olderThan: Date): Promise<Worker[]> {
    return prisma.worker.findMany({
      where: {
        status: WorkerStatus.ONLINE,
        lastHeartbeat: {
          lt: olderThan,
        },
      },
      orderBy: {
        lastHeartbeat: "asc",
      },
    });
  }

  async delete(workerId: string): Promise<Worker> {
    return prisma.worker.delete({
      where: {
        id: workerId,
      },
    });
  }
}