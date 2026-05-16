import { Pool, PoolClient } from 'pg';

import { Job, JobRow, IWorker, WorkerHandler, WorkerContext } from './types';
import { RetriesConfig, computeDelay } from './batteries/retries';
import { moveToDlq } from './batteries/dlq';

export function buildClaimQuery(hasRetries: boolean, hasPriority: boolean): string {
  const extraCols = (hasRetries ? ', backoff, run_at' : '') + (hasPriority ? ', priority' : '');
  const runAtClause = hasRetries ? '  AND run_at <= NOW()\n' : '';
  const orderBy = hasPriority
    ? '  ORDER BY priority DESC, created_at ASC\n'
    : '  ORDER BY created_at ASC\n';
  return (
    `  SELECT id, name, payload, status, attempts, max_attempts${extraCols}, error,\n` +
    `         created_at, started_at, completed_at, failed_at\n` +
    `  FROM fabrikk_jobs\n` +
    `  WHERE name = $1 AND status = 'pending'\n` +
    runAtClause +
    orderBy +
    `  LIMIT 1\n` +
    `  FOR UPDATE SKIP LOCKED\n`
  );
}

export function rowToJob<Payload>(row: JobRow): Job<Payload> {
  return {
    id: row.id,
    name: row.name,
    payload: row.payload as Payload,
    status: row.status,
    attempts: row.attempts,
    max_attempts: row.max_attempts,
    backoff: row.backoff,
    run_at: row.run_at,
    priority: row.priority ?? 0,
    error: row.error,
    created_at: row.created_at,
    started_at: row.started_at,
    completed_at: row.completed_at,
    failed_at: row.failed_at,
  };
}

// Resolves when signal fires or ms elapses, whichever comes first
export function interruptibleSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

// Claims the next pending job for `name` inside a transaction.
// Returns null (and rolls back) if none available.
// Caller is responsible for releasing the client after this returns.
export async function claimJob(
  client: PoolClient,
  name: string,
  claimQuery: string,
): Promise<JobRow | null> {
  await client.query('BEGIN');
  const result = await client.query<JobRow>(claimQuery, [name]);
  if (result.rows.length === 0) {
    await client.query('ROLLBACK');
    return null;
  }
  const row = result.rows[0];
  await client.query(
    `UPDATE fabrikk_jobs SET status = 'running', started_at = NOW(), attempts = attempts + 1 WHERE id = $1`,
    [row.id],
  );
  await client.query('COMMIT');
  return row;
}

export async function completeJob(pool: Pool, id: string): Promise<void> {
  await pool.query(
    `UPDATE fabrikk_jobs SET status = 'completed', completed_at = NOW() WHERE id = $1`,
    [id],
  );
}

export async function failJob(pool: Pool, id: string, error: unknown): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  await pool.query(
    `UPDATE fabrikk_jobs SET status = 'failed', failed_at = NOW(), error = $2 WHERE id = $1`,
    [id, message],
  );
}

export async function deadJob(pool: Pool, id: string, error: unknown): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  await pool.query(
    `UPDATE fabrikk_jobs SET status = 'dead', failed_at = NOW(), error = $2 WHERE id = $1`,
    [id, message],
  );
}

export async function retryJob(
  pool: Pool,
  id: string,
  attempt: number,
  globalConfig: RetriesConfig,
  perJobBackoff?: string | null,
): Promise<number> {
  const effectiveConfig: RetriesConfig = perJobBackoff
    ? { ...globalConfig, backoff: perJobBackoff as RetriesConfig['backoff'] }
    : globalConfig;
  const delayMs = computeDelay(attempt, effectiveConfig);
  await pool.query(
    `UPDATE fabrikk_jobs
     SET status = 'pending', failed_at = NULL, run_at = NOW() + ($2 || ' milliseconds')::INTERVAL
     WHERE id = $1`,
    [id, delayMs],
  );
  return delayMs;
}

export class Worker<Payload> implements IWorker {
  private readonly donePromise: Promise<void>;
  private readonly claimQuery: string;

  constructor(
    private readonly pool: Pool,
    private readonly jobName: string,
    private readonly handler: WorkerHandler<Payload>,
    private readonly signal: AbortSignal,
    private readonly pollIntervalMs: number,
    private readonly ready: Promise<void>,
    private readonly context: WorkerContext,
  ) {
    this.claimQuery = buildClaimQuery(context.retries !== undefined, context.priority);
    this.donePromise = this.run();
    this.donePromise.catch(() => undefined);
  }

  wait(): Promise<void> {
    return this.donePromise.catch(() => undefined);
  }

  private async run(): Promise<void> {
    await this.ready;
    while (!this.signal.aborted) {
      const client = await this.pool.connect();
      let row: JobRow | null;
      try {
        row = await claimJob(client, this.jobName, this.claimQuery);
      } catch (err) {
        client.release(true);
        throw err;
      }
      client.release();

      if (!row) {
        await interruptibleSleep(this.pollIntervalMs, this.signal);
        continue;
      }

      const job = rowToJob<Payload>(row);

      const startedPayload = {
        jobName: this.jobName,
        jobId: job.id,
        payload: job.payload,
      };
      this.context.hooks?.emit('job:started', startedPayload);

      const startMs = Date.now();
      try {
        await this.handler(job, this.signal);
        await completeJob(this.pool, job.id);
        this.context.hooks?.emit('job:completed', {
          ...startedPayload,
          durationMs: Date.now() - startMs,
        });
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        if (this.context.retries && job.attempts + 1 < job.max_attempts) {
          const delayMs = await retryJob(
            this.pool,
            job.id,
            job.attempts + 1,
            this.context.retries,
            job.backoff,
          );
          this.context.hooks?.emit('job:retrying', {
            ...startedPayload,
            error: errorMessage,
            attempt: job.attempts + 1,
            delayMs,
          });
        } else if (this.context.retries) {
          const fn = this.context.dlq ? moveToDlq : deadJob;
          await fn(this.pool, job.id, err);
          this.context.hooks?.emit('job:dead', {
            ...startedPayload,
            error: errorMessage,
          });
        } else {
          await failJob(this.pool, job.id, err);
          this.context.hooks?.emit('job:failed', {
            ...startedPayload,
            error: errorMessage,
          });
        }
      }
    }
  }
}
