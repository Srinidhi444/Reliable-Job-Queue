import { RecoveryManager } from "./RecoveryManager";

export class SchedulerRuntime {
  constructor(
    private readonly recoveryManager: RecoveryManager
  ) {}

  async start(): Promise<void> {
    console.log("Scheduler started.");

    this.recoveryManager.start();
  }

  async stop(): Promise<void> {
    this.recoveryManager.stop();

    console.log("Scheduler stopped.");
  }
}