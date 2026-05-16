import { Pool } from 'pg';

import { toErrorMessage } from '../db';
import { interruptibleSleep } from '../worker';
import { HooksBus } from './hooks';
import { RetriesConfig } from './retries';

export interface CronEntry {
  id: string;
  job_name: string;
  expression: string;
  payload: unknown;
  next_run: Date | null;
  last_run: Date | null;
  created_at: Date;
}

export async function scheduleCron(
  pool: Pool,
  jobName: string,
  expression: string,
  payload: unknown,
): Promise<void> {
  if (!jobName) throw new Error('cron jobName must be non-empty');
  const nextRun = getNextRun(expression, new Date());
  // Upsert keyed on (job_name, expression) — a different expression for the same name
  // creates a second row (two active schedules). Callers must explicitly delete the old one.
  await pool.query(
    `INSERT INTO fabrikk_cron (job_name, expression, payload, next_run)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (job_name, expression)
     DO UPDATE SET payload = EXCLUDED.payload, next_run = EXCLUDED.next_run`,
    [jobName, expression, JSON.stringify(payload), nextRun],
  );
}

function parseCronField(field: string): number[] | null {
  if (field === '*') return null;

  const values = new Set<number>();
  for (const part of field.split(',')) {
    if (part.includes('-')) {
      const [startStr, endStr] = part.split('-');
      if (!startStr || !endStr) {
        throw new Error(`Invalid cron field: "${field}". Malformed range.`);
      }
      const start = parseInt(startStr, 10);
      const end = parseInt(endStr, 10);
      if (isNaN(start) || isNaN(end)) {
        throw new Error(`Invalid cron field: "${field}". Non-numeric values found.`);
      }
      if (start > end) {
        throw new Error(`Invalid cron field: "${field}". Range start must be <= end.`);
      }
      for (let i = start; i <= end; i++) values.add(i);
    } else {
      const val = parseInt(part, 10);
      if (isNaN(val)) {
        throw new Error(`Invalid cron field: "${field}". Non-numeric value found.`);
      }
      values.add(val);
    }
  }

  return Array.from(values);
}

function matchesCron(value: number, constraint: number[] | null): boolean {
  return constraint === null || constraint.includes(value);
}

function validateRange(values: number[], min: number, max: number, fieldName: string): void {
  for (const val of values) {
    if (val < min || val > max) {
      throw new Error(`Invalid ${fieldName}: ${val}. Must be between ${min} and ${max}.`);
    }
  }
}

export function getNextRun(expression: string, after: Date): Date {
  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(`Invalid cron expression: ${expression}. Expected 5 fields.`);
  }

  const [minStr, hourStr, domStr, monthStr, dowStr] = parts;
  const mins = parseCronField(minStr);
  const hours = parseCronField(hourStr);
  const doms = parseCronField(domStr);
  const months = parseCronField(monthStr);
  const dows = parseCronField(dowStr);

  // Validate ranges
  if (mins) validateRange(mins, 0, 59, 'minute');
  if (hours) validateRange(hours, 0, 23, 'hour');
  if (doms) validateRange(doms, 1, 31, 'day of month');
  if (months) validateRange(months, 1, 12, 'month');
  if (dows) validateRange(dows, 0, 7, 'day of week');

  // Normalize day of week (both 0 and 7 = Sunday)
  const normalizedDows = dows?.map((v) => v % 7) ?? null;

  const candidate = new Date(after.getTime());
  candidate.setUTCSeconds(0, 0);
  candidate.setUTCMinutes(candidate.getUTCMinutes() + 1);

  // Search up to 4 years (leap year inclusive)
  const maxIterations = 366 * 24 * 60 * 4;
  for (let i = 0; i < maxIterations; i++) {
    if (
      matchesCron(candidate.getUTCMinutes(), mins) &&
      matchesCron(candidate.getUTCHours(), hours) &&
      matchesCron(candidate.getUTCDate(), doms) &&
      matchesCron(candidate.getUTCMonth() + 1, months) &&
      matchesCron(candidate.getUTCDay(), normalizedDows)
    ) {
      return new Date(candidate.getTime());
    }
    candidate.setUTCMinutes(candidate.getUTCMinutes() + 1);
  }

  throw new Error(`No next run found for expression: ${expression}`);
}

function lockIdFromString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) || 1;
}

export class CronScheduler {
  private readonly donePromise: Promise<void>;

  constructor(
    private readonly pool: Pool,
    private readonly signal: AbortSignal,
    private readonly pollIntervalMs: number,
    private readonly ready: Promise<void>,
    private readonly retriesConfig?: RetriesConfig,
    private readonly hooks?: HooksBus,
  ) {
    this.donePromise = this.run();
    this.donePromise.catch(() => undefined);
  }

  wait(): Promise<void> {
    return this.donePromise.catch(() => undefined);
  }

  private async run(): Promise<void> {
    await this.ready;
    while (!this.signal.aborted) {
      await this.processDueSchedules();
      await interruptibleSleep(this.pollIntervalMs, this.signal);
    }
  }

  async processDueSchedules(): Promise<void> {
    const client = await this.pool.connect();
    try {
      const dueResult = await client.query<CronEntry>(
        `SELECT * FROM fabrikk_cron WHERE next_run <= NOW() ORDER BY next_run ASC LIMIT 100`,
      );

      for (const entry of dueResult.rows) {
        const lockKey = lockIdFromString(entry.id);

        try {
          await client.query('BEGIN');

          // Transaction-level advisory lock: automatically released at COMMIT/ROLLBACK,
          // so it cannot leak back into the pool if the connection is reused.
          const lockResult = await client.query<{ pg_try_advisory_xact_lock: boolean }>(
            'SELECT pg_try_advisory_xact_lock($1)',
            [lockKey],
          );
          if (!lockResult.rows[0].pg_try_advisory_xact_lock) {
            await client.query('ROLLBACK');
            continue;
          }

          const freshResult = await client.query<CronEntry>(
            'SELECT * FROM fabrikk_cron WHERE id = $1',
            [entry.id],
          );
          // Row deleted between outer SELECT and lock acquisition — skip silently
          if (
            freshResult.rows.length === 0 ||
            (freshResult.rows[0].next_run && freshResult.rows[0].next_run > new Date())
          ) {
            await client.query('ROLLBACK');
            continue;
          }

          const fresh = freshResult.rows[0];

          const maxAttempts = this.retriesConfig?.attempts ?? 3;
          if (this.retriesConfig) {
            await client.query(
              `INSERT INTO fabrikk_jobs (name, payload, max_attempts, backoff)
               VALUES ($1, $2, $3, $4)`,
              [
                fresh.job_name,
                JSON.stringify(fresh.payload),
                maxAttempts,
                this.retriesConfig.backoff,
              ],
            );
          } else {
            await client.query(
              `INSERT INTO fabrikk_jobs (name, payload, max_attempts)
               VALUES ($1, $2, $3)`,
              [fresh.job_name, JSON.stringify(fresh.payload), maxAttempts],
            );
          }

          const nextRun = getNextRun(fresh.expression, new Date());
          await client.query(
            'UPDATE fabrikk_cron SET last_run = NOW(), next_run = $1 WHERE id = $2',
            [nextRun, fresh.id],
          );

          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK').catch(() => undefined);
          if (this.hooks) {
            this.hooks.emit('cron:error', { entryId: entry.id, error: toErrorMessage(err) });
          } else {
            console.error(`Failed to process cron schedule ${entry.id}:`, err);
          }
        }
      }
    } finally {
      client.release();
    }
  }
}
