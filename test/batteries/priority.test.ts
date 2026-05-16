import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Queue } from '../../src/index';
import { clearJobs, getJobRows, makeTestPool } from '../setup';
import type { Pool } from 'pg';

type TestJobs = {
  'send-email': { to: string };
};

let pool: Pool;

beforeEach(async () => {
  pool = makeTestPool();
  await clearJobs(pool);
});

afterEach(async () => {
  await clearJobs(pool);
  await pool.end();
});

describe('zero footprint when priority battery is absent', () => {
  it('enqueues jobs without a priority column when battery is absent', async () => {
    const queue = new Queue<TestJobs>({ pool, pollIntervalMs: 20 });
    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows).toHaveLength(1);
  });
});

describe('priority battery — column and index created', () => {
  it('adds priority column when battery is active', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { priority: true },
    });
    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.stop();

    const result = await pool.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name = 'fabrikk_jobs'
         AND column_name = 'priority'`,
    );
    expect(result.rows).toHaveLength(1);
  });
});

describe('priority battery — ordering', () => {
  it('processes higher-priority jobs before lower-priority jobs', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { priority: true },
    });

    // Enqueue three jobs with different priorities
    await queue.enqueue('send-email', { to: 'low@b.com' }, { priority: 1 });
    await queue.enqueue('send-email', { to: 'high@b.com' }, { priority: 10 });
    await queue.enqueue('send-email', { to: 'mid@b.com' }, { priority: 5 });

    const processed: string[] = [];
    queue.work('send-email', async (job) => {
      processed.push((job.payload as { to: string }).to);
    });

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const rows = await getJobRows(pool, 'send-email');
        if (rows.every((r) => r.status === 'completed')) {
          clearInterval(check);
          resolve();
        }
      }, 30);
    });

    await queue.stop();

    expect(processed[0]).toBe('high@b.com');
    expect(processed[1]).toBe('mid@b.com');
    expect(processed[2]).toBe('low@b.com');
  });

  it('falls back to created_at ordering for equal priorities', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { priority: true },
    });

    await queue.enqueue('send-email', { to: 'first@b.com' }, { priority: 5 });
    // Small delay so created_at differs
    await new Promise((r) => setTimeout(r, 10));
    await queue.enqueue('send-email', { to: 'second@b.com' }, { priority: 5 });

    const processed: string[] = [];
    queue.work('send-email', async (job) => {
      processed.push((job.payload as { to: string }).to);
    });

    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const rows = await getJobRows(pool, 'send-email');
        if (rows.every((r) => r.status === 'completed')) {
          clearInterval(check);
          resolve();
        }
      }, 30);
    });

    await queue.stop();

    expect(processed[0]).toBe('first@b.com');
    expect(processed[1]).toBe('second@b.com');
  });

  it('defaults to priority 0 when not specified', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { priority: true },
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].priority).toBe(0);
  });

  it('stores the priority value in the job row', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { priority: true },
    });

    await queue.enqueue('send-email', { to: 'a@b.com' }, { priority: 42 });
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].priority).toBe(42);
  });
});
