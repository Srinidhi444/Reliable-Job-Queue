
import { Queue } from "../../packages/core/src/queue/Queue";
import type { WorkerOptions } from "../../packages/core/src/types/WorkerOptions";

const TEST_MODE =
  "NORMAL" as
    | "NORMAL"
    | "LONG_RUNNING"
    | "LEASE_EXPIRE"
    | "CRASH"
    | "FAIL";

async function main() {
  const options: WorkerOptions = {
    pollingInterval: 1000,
    heartbeatInterval: 3000,
    leaseDuration: 10000,
    
    // Test concurrency
    concurrency: 3,
  };

  const queue = new Queue({
    worker: options,
  });
 


  // await queue.replay("f69f707f-4c6c-404f-9d16-da158418ffbe");
//   await queue.enqueue({
//   queue: "emails",
//   type: "send-email",
//   payload: {
//     to: "john@example.com",
//     subject: "Welcome",
//     body: "Hello",
//   },
//   options: {
//     delay: 30_000, // 30 seconds
//   },
// });

  queue.register("send-email", async (job) => {
    console.log("==================================");
    console.log(`Worker : ${job.workerId ?? "N/A"}`);
    console.log(`Mode   : ${TEST_MODE}`);
    console.log(`Job    : ${job.id}`);
    console.log(job.payload);
    console.log("==================================");

    switch (TEST_MODE) {
      case "NORMAL":
        break;

      case "LONG_RUNNING":
        console.log("Sleeping 20 seconds...");
        await sleep(20_000);
        break;

      case "LEASE_EXPIRE":
        console.log("Sleeping 120 seconds...");
        await sleep(120_000);
        break;

      case "CRASH":
        console.log("Simulating worker crash...");
        await sleep(3000);
        process.exit(1);

      case "FAIL":
        console.log("Throwing intentional error...");
        throw new Error("Intentional failure.");
    }

    console.log(`Finished Job ${job.id}`);
  });

  process.on("SIGINT", async () => {
    await queue.stopWorker();
    process.exit(0);
  });

  process.on("SIGTERM", async () => {
    await queue.stopWorker();
    process.exit(0);
  });

  await queue.startWorker();
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch(console.error);