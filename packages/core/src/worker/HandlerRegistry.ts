import type { Job } from "@reliable-job-queue/shared";
import type { WorkerContext } from "./WorkerContext";

export type JobHandler = (
  job: Job,
  context: WorkerContext
) => Promise<void>;

export class HandlerRegistry {
  private readonly handlers = new Map<string, JobHandler>();

  register(type: string, handler: JobHandler): void {
    if (this.handlers.has(type)) {
      throw new Error(
        `Handler already registered for job type "${type}".`
      );
    }

    this.handlers.set(type, handler);
  }

  get(type: string): JobHandler {
    const handler = this.handlers.get(type);

    if (!handler) {
      throw new Error(
        `No handler registered for job type "${type}".`
      );
    }

    return handler;
  }

  has(type: string): boolean {
    return this.handlers.has(type);
  }

  clear(): void {
    this.handlers.clear();
  }

  size(): number {
    return this.handlers.size;
  }
}