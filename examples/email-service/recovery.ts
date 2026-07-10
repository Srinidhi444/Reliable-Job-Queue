import { JobRepository } from "../../packages/database/src";
import { LeaseManager } from "../../packages/core/src/lease/LeaseManager";
import { RecoveryManager } from "../../packages/core/src/scheduler/RecoveryManager";

async function main() {
  const jobRepository = new JobRepository();

  // Lease duration is irrelevant here since RecoveryManager
  // only calls releaseLease().
  const leaseManager = new LeaseManager(
    jobRepository,
    10_000
  );

  const recoveryManager = new RecoveryManager(
    leaseManager,
    jobRepository,
    5000 // Scan every 5 seconds
  );

  console.log("==================================");
  console.log("Recovery Manager Started");
  console.log("Checking every 5 seconds...");
  console.log("==================================");

  recoveryManager.start();

  process.on("SIGINT", () => {
    console.log("\nStopping Recovery Manager...");
    recoveryManager.stop();
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    console.log("\nStopping Recovery Manager...");
    recoveryManager.stop();
    process.exit(0);
  });
}

main().catch(console.error);