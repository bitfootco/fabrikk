# fabrikk

> A Postgres-native job queue for TypeScript. Bare by default. Powerful by choice.

Fabrikk is a zero-dependency job queue built on Postgres and TypeScript. The base install fits in your head. When you need more — retries, cron, rate limiting, a dashboard API — you opt in, one battery at a time.

---

## Why Fabrikk?

Postgres-backed queues are a well-trodden pattern, and there are good libraries out there. Fabrikk has a specific opinion: **you shouldn't have to learn a framework to use a queue.**

- The bare API is three concepts: define a job, enqueue it, work it
- TypeScript types flow from definition through to your worker handler — no casting
- Every advanced feature is opt-in via config — unused features have zero footprint
- The library owns its schema — no migrations, no setup scripts, it self-heals on startup
- Graceful shutdown is built in — every worker receives an `AbortSignal`

---

## Install

```bash
npm install @bitfootco/fabrikk
```

Requires Node.js 18+ and Postgres 14+.

---

## Quickstart

This is the entire bare API.

```ts
import { Queue } from '@bitfootco/fabrikk'

// 1. Define your job types
type Jobs = {
  send-email: { to: string; subject: string; body: string }
  resize-image: { imageId: string; width: number }
}

// 2. Create a queue — it manages its own schema
const queue = new Queue<Jobs>({
  connectionString: process.env.DATABASE_URL,
})

// 3. Enqueue a job
await queue.enqueue('send-email', {
  to: 'hi@example.com',
  subject: 'Hello',
  body: 'World',
})

// 4. Work jobs — payload is fully typed
queue.work('send-email', async (job, signal) => {
  await sendEmail(job.payload) // payload: { to, subject, body } — no casting needed
})

// 5. Graceful shutdown — the AbortSignal is wired up automatically
process.on('SIGTERM', () => queue.stop())
```

That's it. No config files. No migrations to run. No registration ceremony.

---

## Async Iterator Interface

Prefer pull-based consumption? Every queue exposes an async iterator alongside `.work()`.

```ts
for await (const job of queue.jobs('send-email')) {
  await sendEmail(job.payload)
  await job.done()
}
```

The iterator respects backpressure and the same `AbortSignal`-based shutdown as `.work()`.

---

## Batteries

Advanced features are opt-in. Include only what you need.

```ts
const queue = new Queue<Jobs>({
  connectionString: process.env.DATABASE_URL,
  batteries: {
    retries: {
      attempts: 3,
      backoff: 'exponential', // or 'linear' | 'fixed'
      baseDelay: 1000,        // ms
    },
    dlq: true,
    cron: true,
    priority: true,
    rateLimit: true,
    fanout: true,
    hooks: true,
    dashboard: {
      path: '/queue',       // mounts REST API at this path on your existing server
    },
  },
})
```

Each battery is documented below. If it's not in your config, it doesn't exist — no schema additions, no overhead.

---

### 🔁 `retries`

Automatic retry with exponential backoff and jitter. Configurable globally or per job type.

```ts
batteries: {
  retries: {
    attempts: 5,
    backoff: 'exponential',
    baseDelay: 500,
  }
}
```

Override per job at enqueue time:

```ts
await queue.enqueue('send-email', payload, {
  retries: { attempts: 10, backoff: 'linear' },
})
```

Failed jobs that exhaust their retries are moved to the dead-letter queue if `dlq` is enabled, or discarded if not.

---

### ☠️ `dlq`

Dead-letter queue. Jobs that fail all retry attempts land here for inspection and replay.

```ts
batteries: {
  dlq: true,
}
```

Inspect and replay dead jobs:

```ts
const dead = await queue.dlq.list('send-email')   // paginated
await queue.dlq.replay(dead[0].id)                 // re-enqueues with fresh retry count
await queue.dlq.discard(dead[0].id)                // permanent delete
await queue.dlq.replayAll('send-email')            // bulk replay
```

Requires `retries` battery to be enabled.

---

### ⏰ `cron`

Recurring jobs defined in code, not in a separate scheduler. Uses cron expressions with second-level precision.

```ts
batteries: {
  cron: true,
}
```

Register recurring jobs after queue creation:

```ts
queue.cron('send-email', '0 9 * * 1-5', {   // 9am weekdays
  to: 'digest@example.com',
  subject: 'Daily digest',
  body: '...',
})
```

Cron jobs are deduplicated across multiple workers using Postgres advisory locks — no double-firing in a scaled deployment.

---

### 🏆 `priority`

Priority queues. Higher priority jobs are worked first within the same queue.

```ts
batteries: {
  priority: true,
}
```

Set priority at enqueue time (higher number = higher priority, default 0):

```ts
await queue.enqueue('send-email', payload, { priority: 10 })
await queue.enqueue('send-email', payload, { priority: 1 })  // worked after
```

Workers are priority-aware automatically — no change to `.work()` or the iterator.

---

### 🚦 `rateLimit`

Limit how fast a queue is consumed, without Redis. Uses Postgres timestamptz precision.

```ts
batteries: {
  rateLimit: true,
}
```

Set rate limits per queue:

```ts
queue.setRateLimit('send-email', {
  max: 100,
  window: '1m',   // '1s' | '1m' | '1h'
})
```

Rate limits are enforced cluster-wide — safe across multiple worker processes.

---

### 📡 `fanout`

Enqueue a single job to multiple queues at once. Useful for event-driven flows where multiple systems need to react to the same event.

```ts
batteries: {
  fanout: true,
}
```

Define fanout rules:

```ts
queue.fanout('user-signed-up', ['send-welcome-email', 'create-billing-account', 'notify-slack'])
```

Then enqueue normally — Fabrikk fans it out for you:

```ts
await queue.enqueue('user-signed-up', { userId: '123' })
// → enqueues to send-welcome-email, create-billing-account, notify-slack
```

---

### 🪝 `hooks`

Structured event emitter for job lifecycle events. Wire into your own observability stack.

```ts
batteries: {
  hooks: true,
}
```

Subscribe to events:

```ts
queue.on('job:enqueued',   (event) => logger.info(event))
queue.on('job:started',    (event) => metrics.increment('job.started'))
queue.on('job:completed',  (event) => metrics.histogram('job.duration', event.durationMs))
queue.on('job:failed',     (event) => logger.error(event))
queue.on('job:retrying',   (event) => logger.warn(event))
queue.on('job:dead',       (event) => alerts.notify(event))
```

All events are typed. `event.jobName` narrows the payload type in the handler.

---

### 📊 `dashboard`

Mounts a REST API on your existing HTTP server. Bring your own UI.

```ts
batteries: {
  dashboard: {
    path: '/queue',
  }
}
```

Mount on Express, Fastify, or any Node.js HTTP server:

```ts
// Express
app.use('/queue', queue.dashboardHandler())

// Fastify
fastify.all('/queue/*', queue.dashboardHandler())
```

#### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/queue/jobs` | List jobs, filterable by queue / status / date |
| `GET` | `/queue/jobs/:id` | Get a single job |
| `POST` | `/queue/jobs/:id/replay` | Re-enqueue a dead job |
| `DELETE` | `/queue/jobs/:id` | Discard a job |
| `GET` | `/queue/queues` | List queues with stats (depth, throughput, error rate) |
| `GET` | `/queue/cron` | List cron schedules and last-run times |
| `GET` | `/queue/health` | Liveness check — returns 200 if queue is healthy |

All responses are JSON. Authentication is your responsibility — mount behind your existing auth middleware.

---

## Schema

Fabrikk manages all its own tables. On first startup it creates them. On subsequent startups it checks and repairs any schema drift — safe to run in a multi-instance deploy.

All tables are prefixed with `fabrikk_` and live in your existing database. No separate database needed.

---

## Graceful Shutdown

Every worker automatically receives an `AbortSignal`. When you call `queue.stop()`, in-flight jobs are given a grace period to complete before the process exits.

```ts
const queue = new Queue<Jobs>({
  connectionString: process.env.DATABASE_URL,
  shutdown: {
    gracePeriodMs: 30_000,  // default: 30s
  },
})

// Wire to your process signals
process.on('SIGTERM', () => queue.stop())
process.on('SIGINT',  () => queue.stop())
```

Inside your worker, respect the signal for long-running jobs:

```ts
queue.work('resize-image', async (job, signal) => {
  for (const chunk of chunks) {
    if (signal.aborted) break
    await processChunk(chunk)
  }
  await job.done()
})
```

---

## TypeScript

Fabrikk is written in TypeScript and ships its own types. Define your job payload types once at queue creation — they propagate automatically.

```ts
type Jobs = {
  'send-email': { to: string; subject: string }
  'process-payment': { orderId: string; amountCents: number }
}

const queue = new Queue<Jobs>({ connectionString: '...' })

// ✅ Payload is { to: string; subject: string } — inferred, no casting
queue.work('send-email', async (job) => {
  job.payload.to       // string ✓
  job.payload.subject  // string ✓
  job.payload.orderId  // TS error — wrong job type ✓
})

// ✅ Enqueue is type-checked too
await queue.enqueue('send-email', { to: 'a@b.com', subject: 'Hi' })  // ✓
await queue.enqueue('send-email', { amountCents: 100 })               // TS error ✓
```

---

## Configuration Reference

```ts
const queue = new Queue<Jobs>({
  // Required
  connectionString: string,        // Postgres connection string

  // Optional
  schema: string,                  // Postgres schema, default: 'public'
  poolSize: number,                // PG connection pool size, default: 10
  pollIntervalMs: number,          // How often workers poll, default: 1000
  shutdown: {
    gracePeriodMs: number,         // Grace period on stop(), default: 30000
  },

  // Batteries (all optional)
  batteries: {
    retries: {
      attempts: number,            // Max attempts including first, default: 3
      backoff: 'exponential' | 'linear' | 'fixed',
      baseDelay: number,           // ms, default: 1000
    },
    dlq: boolean,
    cron: boolean,
    priority: boolean,
    rateLimit: boolean,
    fanout: boolean,
    hooks: boolean,
    dashboard: {
      path: string,                // Mount path, default: '/queue'
    },
  },
})
```

---

## Comparison

| | Fabrikk | pg-boss | BullMQ |
|---|---|---|---|
| Backend | Postgres | Postgres | Redis |
| TypeScript | First-class, inferred | Partial | Good |
| API surface | Minimal (opt-in) | Large | Large |
| Schema management | Automatic | Automatic | N/A |
| Dashboard | API endpoint (BYO UI) | None | Paid (BullBoard) |
| Cron | Optional battery | Built-in | Built-in |
| Rate limiting | Optional battery | None | Built-in |
| Graceful shutdown | Built-in | Manual | Manual |
| Zero-dep core | ✓ | ✗ | ✗ |

---

## Contributing

Fabrikk is open source and maintained by [The Bitfoot Company](https://bitfoot.co) as part of our labs projects. Issues, PRs, and feedback are welcome.

```bash
git clone https://github.com/bitfootco/fabrikk
cd Fabrikk
npm install
npm test
```

You'll need a local Postgres instance. Copy `.env.example` to `.env` and set `DATABASE_URL`.

---

## License

MIT
