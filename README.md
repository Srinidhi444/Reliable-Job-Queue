# Reliable Job Queue

A lightweight, PostgreSQL-backed background job queue for Node.js with support for durable jobs, retries, delayed execution, priorities, worker leasing, crash recovery, and dead-letter queues.

Unlike in-memory queues, jobs are persisted in PostgreSQL, allowing them to survive process crashes and restarts.

## Features

- Durable PostgreSQL-backed job storage
- Background workers
- Configurable worker concurrency
- Delayed and scheduled jobs
- Priority-based scheduling
- Automatic retries with exponential backoff
- Dead Letter Queue (DLQ)
- Replay failed jobs
- Visibility timeout (lease-based processing)
- Automatic lease renewal (heartbeats)
- Recovery of orphaned jobs after worker crashes
- Graceful worker shutdown
- Queue statistics
- Job execution history

## Installation

```bash
npm install @reliable-job-queue/core
```

You'll also need PostgreSQL.

## Quick Start

### 1. Create a Queue

```ts
import { Queue } from "@reliable-job-queue/core";

const queue = new Queue();
```

### 2. Register a Worker

```ts
queue.register("send-email", async (job) => {
  console.log(job.payload);

  // Send email...
});
```

### 3. Start the Worker

```ts
await queue.startWorker();
```

### 4. Enqueue a Job

```ts
await queue.enqueue({
  queue: "emails",
  type: "send-email",
  payload: {
    to: "john@example.com",
    subject: "Welcome",
    body: "Hello!"
  }
});
```

## Delayed Jobs

Run a job after a delay.

```ts
await queue.enqueue({
  queue: "emails",
  type: "send-email",
  payload: {...},
  options: {
    delay: 30_000
  }
});
```

## Scheduled Jobs

Execute at a specific time.

```ts
await queue.enqueue({
  queue: "emails",
  type: "send-email",
  payload: {...},
  options: {
    runAt: new Date(Date.now() + 60_000)
  }
});
```

## Job Priority

Higher priority jobs execute first.

```ts
await queue.enqueue({
  queue: "emails",
  type: "send-email",
  payload: {...},
  options: {
    priority: 100
  }
});
```

## Retry Configuration

```ts
await queue.enqueue({
  queue: "emails",
  type: "send-email",
  payload: {...},
  options: {
    maxAttempts: 5
  }
});
```

Retries use exponential backoff by default.

## Worker Configuration

```ts
const queue = new Queue({
  worker: {
    concurrency: 5,
    pollingInterval: 1000,
    heartbeatInterval: 3000,
    leaseDuration: 10000
  }
});
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `concurrency` | `1` | Number of jobs processed simultaneously |
| `pollingInterval` | `1000ms` | Database polling interval |
| `heartbeatInterval` | `10000ms` | Lease renewal interval |
| `leaseDuration` | `30000ms` | Visibility timeout |

## Dead Letter Queue

Jobs exceeding the retry limit are automatically moved to the Dead Letter Queue.

Retrieve them:

```ts
const jobs = await queue.getDLQ();
```

Replay a failed job:

```ts
await queue.replay(jobId);
```

## Queue Statistics

```ts
const stats = await queue.stats();
```

Example:

```json
{
  "pending": 10,
  "processing": 3,
  "completed": 250,
  "failed": 0,
  "dlq": 2
}
```

## Scheduler

Recovery is handled by a dedicated scheduler process.

```ts
import { Scheduler } from "@reliable-job-queue/core";

const scheduler = new Scheduler();

await scheduler.start();
```

The scheduler periodically:

- Detects expired leases
- Releases abandoned jobs
- Makes them available for processing again

## Graceful Shutdown

```ts
process.on("SIGINT", async () => {
  await queue.stopWorker();
});
```

The worker:

- Stops polling
- Waits for running jobs
- Stops heartbeats
- Marks itself offline

## Reliability

### Durable Storage

Jobs are stored in PostgreSQL and survive application restarts.

### Atomic Job Claiming

Workers claim jobs using an atomic database update.

Even if multiple workers attempt to claim the same job simultaneously, only one succeeds.

### Visibility Timeout

When a worker claims a job:

- Status becomes `PROCESSING`
- A lease is assigned
- Other workers cannot execute it

### Heartbeats

Long-running jobs periodically renew their lease.

### Crash Recovery

If a worker crashes:

- Lease expires
- Scheduler detects expired lease
- Job becomes `PENDING`
- Another worker processes it

### Dead Letter Queue

Jobs that exceed the configured retry count are moved to the DLQ for inspection and replay.

## Architecture

```text
Producer
     │
     ▼
 PostgreSQL
     │
     ▼
 Worker
     │
     ├── Lease Manager
     ├── Retry Strategy
     ├── Heartbeats
     └── Job Executor

 Scheduler
     │
     └── Recovery Manager
```

## Current Limitations

- PostgreSQL polling (no Redis notifications)
- At-least-once delivery semantics
- Job handlers should be idempotent
- No cron scheduling
- No web dashboard

## API

### Queue

```ts
new Queue(options)
```

#### Methods

- `register(type, handler)`
- `enqueue(job)`
- `startWorker()`
- `stopWorker()`
- `stats()`
- `getDLQ()`
- `replay(jobId)`

### Scheduler

```ts
new Scheduler()
```

#### Methods

- `start()`
- `stop()`

## Example Project

```text
producer.ts
    │
    ▼
PostgreSQL
    │
    ▼
worker.ts
    │
    ▼
Job Handler
```

## License

MIT