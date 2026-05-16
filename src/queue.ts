import { Pool } from 'pg';
import { setMaxListeners } from 'events';

import { bootstrap } from './schema';
import {
  JobEntry,
  QueueConfig,
  EnqueueOptions,
  WorkerHandler,
  IWorker,
  WorkerContext,
} from './types';
import { HooksEmitter, HooksEventMap } from './batteries/hooks';
import { DlqApi } from './batteries/dlq';
import { CronScheduler, scheduleCron } from './batteries/cron';
import { Worker } from './worker';
import { RetriesConfig } from './batteries/retries';
import { JobIterator } from './iterator';

export class Queue<Jobs extends Record<string, unknown>> {
  private readonly pool: Pool;
  private readonly ownPool: boolean;
  private readonly readyPromise: Promise<void>;
  private readonly abortController: AbortController;
  private readonly activeWorkers = new Set<IWorker>();
  private readonly pollIntervalMs: number;
  private readonly enabled: {
    retries: RetriesConfig | undefined;
    priority: boolean;
    dlq: boolean;
    cron: boolean;
    hooks: boolean;
  };
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
    this.enabled = {
      retries: config.batteries?.retries,
      priority: config.batteries?.priority === true,
      dlq: config.batteries?.dlq === true,
      cron: config.batteries?.cron === true,
      hooks: config.batteries?.hooks === true,
    };
    if (this.enabled.hooks) {
      this.hooks = new HooksEmitter<Jobs>();
    }
    if (this.enabled.dlq) {
      this.dlq = new DlqApi(this.pool);
    }
    this.readyPromise = bootstrap(this.pool, config.batteries);

    if (this.enabled.cron) {
      const scheduler = new CronScheduler(
        this.pool,
        this.abortController.signal,
        this.pollIntervalMs,
        this.readyPromise,
        this.enabled.retries,
        this.hooks,
      );
      this.activeWorkers.add(scheduler);
      scheduler.wait().finally(() => this.activeWorkers.delete(scheduler));
    }
  }

  async enqueue<K extends keyof Jobs & string>(
    name: K,
    payload: Jobs[K],
    opts?: EnqueueOptions,
  ): Promise<void> {
    await this.readyPromise;
    const maxAttempts =
      opts?.retries?.attempts ?? opts?.maxAttempts ?? this.enabled.retries?.attempts ?? 3;

    const cols = ['name', 'payload', 'max_attempts'];
    const vals: unknown[] = [name, JSON.stringify(payload), maxAttempts];

    if (this.enabled.retries) {
      cols.push('backoff');
      vals.push(opts?.retries?.backoff ?? this.enabled.retries.backoff);
    }
    if (this.enabled.priority) {
      cols.push('priority');
      vals.push(opts?.priority ?? 0);
    }

    const placeholders = cols.map((_, i) => `$${i + 1}`).join(', ');
    const { rows } = await this.pool.query<{ id: string }>(
      `INSERT INTO fabrikk_jobs (${cols.join(', ')}) VALUES (${placeholders}) RETURNING id`,
      vals,
    );
    const jobId = rows[0].id;

    this.hooks?.emit('job:enqueued', { jobName: name, jobId, payload });
  }

  work<K extends keyof Jobs & string>(name: K, handler: WorkerHandler<Jobs[K]>): void {
    const context: WorkerContext = {
      retries: this.enabled.retries,
      hooks: this.hooks,
      priority: this.enabled.priority,
      dlq: this.enabled.dlq,
    };
    const worker = new Worker<Jobs[K]>(
      this.pool,
      name,
      handler,
      this.abortController.signal,
      this.pollIntervalMs,
      this.readyPromise,
      context,
    );
    this.activeWorkers.add(worker);
    worker.wait().finally(() => this.activeWorkers.delete(worker));
  }

  jobs<K extends keyof Jobs & string>(name: K): AsyncIterable<JobEntry<Jobs[K]>> {
    const context: WorkerContext = {
      retries: this.enabled.retries,
      hooks: this.hooks,
      priority: this.enabled.priority,
      dlq: this.enabled.dlq,
    };
    return new JobIterator<Jobs[K]>(
      this.pool,
      name,
      this.abortController.signal,
      this.pollIntervalMs,
      this.readyPromise,
      context,
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

  async cron<K extends keyof Jobs & string>(
    name: K,
    expression: string,
    payload: Jobs[K],
  ): Promise<void> {
    if (!this.enabled.cron) {
      throw new Error('Cron battery is not enabled. Add `cron: true` to queue config.');
    }
    await this.readyPromise;
    await scheduleCron(this.pool, name as string, expression, payload);
  }

  async stop(gracePeriodMs = 30_000): Promise<void> {
    this.abortController.abort();

    const allDone = Promise.all([...this.activeWorkers].map((w) => w.wait()));
    const timeout = new Promise<void>((resolve) => setTimeout(resolve, gracePeriodMs));
    await Promise.race([allDone, timeout]);

    if (this.ownPool) await this.pool.end();
  }
}
