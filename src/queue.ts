import { Pool } from 'pg';
import { setMaxListeners } from 'events';

import { bootstrap } from './schema';
import { JobEntry, QueueConfig, EnqueueOptions, WorkerHandler } from './types';
import { HooksEmitter, HooksEventMap } from './batteries/hooks';
import { DlqApi, moveToDlq } from './batteries/dlq';
import { Worker, DeadJobFn, deadJob } from './worker';
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
  readonly hooks?: HooksEmitter<Jobs>;
  readonly dlq?: DlqApi;

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
    setMaxListeners(0, this.abortController.signal);
    this.pollIntervalMs = config.pollIntervalMs ?? 1000;
    this.batteries = config.batteries;
    if (config.batteries?.hooks) {
      this.hooks = new HooksEmitter<Jobs>();
    }
    if (config.batteries?.dlq) {
      this.dlq = new DlqApi(this.pool);
    }
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

    let jobId: string;
    if (retriesCfg) {
      const backoff = opts?.retries?.backoff ?? retriesCfg.backoff;
      const result = await this.pool.query<{ id: string }>(
        `INSERT INTO fabrikk_jobs (name, payload, max_attempts, backoff) VALUES ($1, $2, $3, $4) RETURNING id`,
        [name, JSON.stringify(payload), maxAttempts, backoff],
      );
      jobId = result.rows[0].id;
    } else {
      const result = await this.pool.query<{ id: string }>(
        `INSERT INTO fabrikk_jobs (name, payload, max_attempts) VALUES ($1, $2, $3) RETURNING id`,
        [name, JSON.stringify(payload), maxAttempts],
      );
      jobId = result.rows[0].id;
    }

    this.hooks?.emit('job:enqueued', { jobName: name, jobId, payload });
  }

  work<K extends keyof Jobs & string>(name: K, handler: WorkerHandler<Jobs[K]>): void {
    const deadJobFn: DeadJobFn | undefined = this.batteries?.dlq
      ? moveToDlq
      : this.batteries?.retries
        ? deadJob
        : undefined;
    const worker = new Worker<Jobs[K]>(
      this.pool,
      name,
      handler,
      this.abortController.signal,
      this.pollIntervalMs,
      this.readyPromise,
      this.batteries?.retries,
      this.hooks,
      deadJobFn,
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
      this.hooks,
    );
  }

  on<E extends keyof HooksEventMap<Jobs>>(
    event: E,
    handler: (event: HooksEventMap<Jobs>[E]) => void,
  ): void {
    if (!this.hooks)
      throw new Error('Hooks battery is not enabled. Add `hooks: true` to queue config.');
    this.hooks.on(event, handler);
  }

  async stop(gracePeriodMs = 30_000): Promise<void> {
    this.abortController.abort();

    const allDone = Promise.all([...this.activeWorkers].map((w) => w.wait()));
    const timeout = new Promise<void>((resolve) => setTimeout(resolve, gracePeriodMs));
    await Promise.race([allDone, timeout]);

    if (this.ownPool) await this.pool.end();
  }
}
