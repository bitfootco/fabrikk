import { Pool } from 'pg';

import { bootstrap } from './schema';
import { JobEntry, QueueConfig, EnqueueOptions, WorkerHandler } from './types';
import { Worker } from './worker';
import { JobIterator } from './iterator';

// Queue tracks active workers via this minimal interface to avoid binding to the generic type
interface IWorker {
  wait(): Promise<void>;
}

export class Queue<Jobs extends Record<string, unknown>> {
  private readonly pool: Pool;
  private readonly readyPromise: Promise<void>;
  private readonly abortController: AbortController;
  private readonly activeWorkers = new Set<IWorker>();
  private readonly pollIntervalMs: number;

  constructor(config: QueueConfig) {
    this.pool = new Pool({
      connectionString: config.connectionString,
      max: config.poolSize ?? 10,
    });
    this.abortController = new AbortController();
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.readyPromise = bootstrap(this.pool);
  }

  async enqueue<K extends keyof Jobs & string>(
    name: K,
    payload: Jobs[K],
    opts?: EnqueueOptions,
  ): Promise<void> {
    await this.readyPromise;
    const maxAttempts = opts?.maxAttempts ?? 3;
    await this.pool.query(
      `INSERT INTO fabrikk_jobs (name, payload, max_attempts) VALUES ($1, $2, $3)`,
      [name, JSON.stringify(payload), maxAttempts],
    );
  }

  work<K extends keyof Jobs & string>(name: K, handler: WorkerHandler<Jobs[K]>): void {
    const worker = new Worker<Jobs[K]>(
      this.pool,
      name,
      handler,
      this.abortController.signal,
      this.pollIntervalMs,
      this.readyPromise,
    );
    this.activeWorkers.add(worker);
    worker.wait().finally(() => this.activeWorkers.delete(worker));
  }

  jobs<K extends keyof Jobs & string>(name: K): AsyncIterable<JobEntry<Jobs[K]>> {
    return new JobIterator<Jobs[K]>(
      this.pool,
      name,
      this.abortController.signal,
      this.pollIntervalMs,
      this.readyPromise,
    );
  }

  async stop(gracePeriodMs = 30_000): Promise<void> {
    this.abortController.abort();

    const allDone = Promise.all([...this.activeWorkers].map((w) => w.wait()));
    const timeout = new Promise<void>((resolve) => setTimeout(resolve, gracePeriodMs));
    await Promise.race([allDone, timeout]);

    await this.pool.end();
  }
}
