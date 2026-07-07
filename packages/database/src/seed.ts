import { JobRepository } from "./repositories/JobRepository";

async function main() {
  const repository = new JobRepository();

  const job = await repository.createJob({
    queue: "emails",
    type: "send-email",
    payload: {
      to: "john@example.com",
      subject: "Welcome",
    },
  });

  console.log(job);
}

main().catch(console.error);