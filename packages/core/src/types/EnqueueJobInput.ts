import type { Prisma } from "@prisma/client";
import { EnqueueJobOptions } from "./EnqueueJobOptions";

export interface EnqueueJobInput {
  queue: string;
  type: string;
  payload: Prisma.InputJsonValue;
  options?: EnqueueJobOptions;
}