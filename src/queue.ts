import { IncomingMessage, ServerResponse } from 'http';
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
import { RateLimitConfig, RateLimitMap } from './batteries/rate-limit';
import { FanoutRegistry, enqueueTargets } from './batteries/fanout';
import { DashboardConfig, DashboardHandler } from './batteries/dashboard';
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
    rateLimit: boolean;
    fanout: boolean;
    dashboard: DashboardConfig | undefined;
  };
  private readonly rateLimits: RateLimitMap = new Map();
  private readonly fanoutRegistry?: FanoutRegistry;
  private readonly dashboardInstance?: DashboardHandler;
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
      rateLimit: config.batteries?.rateLimit === true,
      fanout: config.batteries?.fanout === true,
      dashboard: config.batteries?.dashboard,
    };
    if (this.enabled.hooks) {
      this.hooks = new HooksEmitter<Jobs>();
    }
    if (this.enabled.dlq) {
      this.dlq = new DlqApi(this.pool);
    }
    if (this.enabled.fanout) {
      this.fanoutRegistry = new FanoutRegistry();
    }
    if (this.enabled.dashboard) {
      this.dashboardInstance = new DashboardHandler(this.pool, this.enabled.dashboard);
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
    const maxAttempts = opts?.retries?.attempts ?? this.enabled.retries?.attempts ?? 3;

    // Build cols/vals without 'name' so fanout can substitute per-target name
    const extraCols: string[] = ['payload', 'max_attempts'];
    const extraVals: unknown[] = [JSON.stringify(payload), maxAttempts];

    if (this.enabled.retries) {
      extraCols.push('backoff');
      extraVals.push(opts?.retries?.backoff ?? this.enabled.retries.backoff);
    }
    if (this.enabled.priority) {
      extraCols.push('priority');
      extraVals.push(opts?.priority ?? 0);
    }

    const fanoutTargets = this.fanoutRegistry?.get(name);
    if (fanoutTargets) {
      const ids = await enqueueTargets(this.pool, fanoutTargets, extraCols, extraVals);
      for (let i = 0; i < fanoutTargets.length; i++) {
        this.hooks?.emit('job:enqueued', { jobName: fanoutTargets[i], jobId: ids[i], payload });
      }
      return;
    }

    const cols = ['name', ...extraCols];
    const vals: unknown[] = [name, ...extraVals];
    const placeholders = cols.map((_, i) => `$${i + 1}`).join(', ');
    const { rows } = await this.pool.query<{ id: string }>(
      `INSERT INTO fabrikk_jobs (${cols.join(', ')}) VALUES (${placeholders}) RETURNING id`,
      vals,
    );
    const jobId = rows[0].id;

    this.hooks?.emit('job:enqueued', { jobName: name, jobId, payload });
  }

  setRateLimit<K extends keyof Jobs & string>(name: K, config: RateLimitConfig): void {
    if (!this.enabled.rateLimit) {
      throw new Error('RateLimit battery is not enabled. Add `rateLimit: true` to queue config.');
    }
    this.rateLimits.set(name, config);
  }

  fanout<K extends keyof Jobs & string>(source: K, targets: Array<keyof Jobs & string>): void {
    if (!this.fanoutRegistry) {
      throw new Error('Fanout battery is not enabled. Add `fanout: true` to queue config.');
    }
    this.fanoutRegistry.register(source, targets as string[]);
  }

  dashboardHandler(): (req: IncomingMessage, res: ServerResponse) => void {
    if (!this.dashboardInstance) {
      throw new Error(
        'Dashboard battery is not enabled. Add `dashboard: { path }` to queue config.',
      );
    }
    return this.dashboardInstance.handler();
  }

  work<K extends keyof Jobs & string>(name: K, handler: WorkerHandler<Jobs[K]>): void {
    const context: WorkerContext = {
      retries: this.enabled.retries,
      hooks: this.hooks,
      priority: this.enabled.priority,
      dlq: this.enabled.dlq,
      rateLimits: this.enabled.rateLimit ? this.rateLimits : undefined,
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
      rateLimits: this.enabled.rateLimit ? this.rateLimits : undefined,
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
