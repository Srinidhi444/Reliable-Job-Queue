import { JobRepository } from "../../packages/database/src";

async function main() {
  const repository = new JobRepository();

  const job = await repository.createJob({
    queue: "emails",
    type: "send-email",
    payload: {
      to: "john@example.com",
      subject: "Welcome to Reliable Job Queue",
      body: "Hello from our queue!"
    }
  });

  console.log("Job created:");
  console.log(job);
}

main().catch(console.error);