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
  private readonly ownPool: boolean;
  private readonly readyPromise: Promise<void>;
  private readonly abortController: AbortController;
  private readonly activeWorkers = new Set<IWorker>();
  private readonly pollIntervalMs: number;
  private readonly batteries: QueueConfig['batteries'];

  constructor(config: QueueConfig) {
    if ('pool' in config) {
      this.pool = config.pool;
      this.ownPool = false;
    } else {
      this.pool = new Pool({
        connectionString: config.connectionString,
        max: config.poolSize ?? 10,
      });
      this.ownPool = true;
    }
    this.abortController = new AbortController();
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.batteries = config.batteries;
    this.readyPromise = bootstrap(this.pool, config.batteries);
  }

  async enqueue<K extends keyof Jobs & string>(
    name: K,
    payload: Jobs[K],
    opts?: EnqueueOptions,
  ): Promise<void> {
    await this.readyPromise;
    const retriesCfg = this.batteries?.retries;
    const maxAttempts = opts?.retries?.attempts ?? opts?.maxAttempts ?? retriesCfg?.attempts ?? 3;

    if (retriesCfg) {
      const backoff = opts?.retries?.backoff ?? retriesCfg.backoff;
      await this.pool.query(
        `INSERT INTO fabrikk_jobs (name, payload, max_attempts, backoff) VALUES ($1, $2, $3, $4)`,
        [name, JSON.stringify(payload), maxAttempts, backoff],
      );
    } else {
      await this.pool.query(
        `INSERT INTO fabrikk_jobs (name, payload, max_attempts) VALUES ($1, $2, $3)`,
        [name, JSON.stringify(payload), maxAttempts],
      );
    }
  }

  work<K extends keyof Jobs & string>(name: K, handler: WorkerHandler<Jobs[K]>): void {
    const worker = new Worker<Jobs[K]>(
      this.pool,
      name,
      handler,
      this.abortController.signal,
      this.pollIntervalMs,
      this.readyPromise,
      this.batteries?.retries,
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
      this.batteries?.retries,
    );
  }

  async stop(gracePeriodMs = 30_000): Promise<void> {
    this.abortController.abort();

    const allDone = Promise.all([...this.activeWorkers].map((w) => w.wait()));
    const timeout = new Promise<void>((resolve) => setTimeout(resolve, gracePeriodMs));
    await Promise.race([allDone, timeout]);

    if (this.ownPool) await this.pool.end();
  }
}
