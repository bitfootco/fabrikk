import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Queue } from '../../src/index';
import { clearJobs, getJobRows, makeTestPool } from '../setup';
import type { Pool } from 'pg';

type TestJobs = {
  'send-email': { to: string };
  'process-image': { url: string };
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

describe('zero footprint when retries battery is absent', () => {
  it('marks job failed on first failure when retries battery is absent', async () => {
    const queue = new Queue<TestJobs>({ pool, pollIntervalMs: 20 });

    queue.work('send-email', async () => {
      throw new Error('boom');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });

    await new Promise((resolve) => setTimeout(resolve, 200));
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].status).toBe('failed');
    expect(rows[0].error).toBe('boom');
    expect(rows[0].attempts).toBe(1);
  });

  it('does not retry when battery is absent — max_attempts is ignored', async () => {
    const queue = new Queue<TestJobs>({ pool, pollIntervalMs: 20 });

    queue.work('send-email', async () => {
      throw new Error('boom');
    });

    // maxAttempts: 5, but without retries battery it should fail immediately
    await queue.enqueue('send-email', { to: 'a@b.com' }, { maxAttempts: 5 });

    await new Promise((resolve) => setTimeout(resolve, 200));
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].status).toBe('failed');
    expect(rows[0].attempts).toBe(1);
  });
});

describe('retries battery — exponential backoff', () => {
  it('retries a failing job and eventually completes it', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 3, backoff: 'exponential', baseDelay: 10 },
      },
    });

    let callCount = 0;

    queue.work('send-email', async () => {
      callCount++;
      if (callCount < 3) throw new Error('not yet');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });

    // Poll until the job completes
    await new Promise<void>((resolve) => {
      const check = setInterval(async () => {
        const rows = await getJobRows(pool, 'send-email');
        if (rows[0]?.status === 'completed') {
          clearInterval(check);
          resolve();
        }
      }, 30);
    });

    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].status).toBe('completed');
    expect(rows[0].attempts).toBe(3);
  });

  it('marks job dead after exhausting all attempts', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 2, backoff: 'fixed', baseDelay: 10 },
      },
    });

    queue.work('send-email', async () => {
      throw new Error('always fails');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });

    // Wait for retries to exhaust
    await new Promise((resolve) => setTimeout(resolve, 500));
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].status).toBe('dead');
    expect(rows[0].attempts).toBe(2);
    expect(rows[0].error).toBe('always fails');
  });

  it('adds run_at and backoff columns when retries battery is active', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 3, backoff: 'exponential', baseDelay: 10 },
      },
    });
    // Wait for bootstrap
    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.stop();

    const result = await pool.query<{ column_name: string }>(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name = 'fabrikk_jobs'
         AND column_name IN ('run_at', 'backoff')
       ORDER BY column_name`,
    );
    const cols = result.rows.map((r) => r.column_name);
    expect(cols).toContain('backoff');
    expect(cols).toContain('run_at');
  });
});

describe('retries battery — per-job override', () => {
  it('respects per-job attempts override at enqueue time', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 5, backoff: 'fixed', baseDelay: 10 },
      },
    });

    queue.work('send-email', async () => {
      throw new Error('always fails');
    });

    // Override: only 1 attempt
    await queue.enqueue('send-email', { to: 'a@b.com' }, { retries: { attempts: 1 } });

    await new Promise((resolve) => setTimeout(resolve, 300));
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].status).toBe('dead');
    expect(rows[0].attempts).toBe(1);
    expect(rows[0].max_attempts).toBe(1);
  });

  it('respects per-job backoff override at enqueue time', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 2, backoff: 'exponential', baseDelay: 10 },
      },
    });

    queue.work('send-email', async () => {
      throw new Error('always fails');
    });

    // Override backoff to linear
    await queue.enqueue('send-email', { to: 'a@b.com' }, { retries: { backoff: 'linear' } });

    await new Promise((resolve) => setTimeout(resolve, 400));
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].backoff).toBe('linear');
    expect(rows[0].status).toBe('dead');
  });
});

describe('retries battery — run_at delay respected', () => {
  it('does not pick up a job before its run_at delay elapses', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        // Use a long baseDelay so we can observe the pending state
        retries: { attempts: 3, backoff: 'fixed', baseDelay: 5000 },
      },
    });

    let callCount = 0;
    queue.work('send-email', async () => {
      callCount++;
      if (callCount === 1) throw new Error('first fail');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });

    // Wait enough for one attempt + scheduling, but not for the 5s delay
    await new Promise((resolve) => setTimeout(resolve, 300));
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    // Should have been attempted once, now sitting pending with a future run_at
    expect(rows[0].status).toBe('pending');
    expect(rows[0].attempts).toBe(1);
    expect(callCount).toBe(1);

    const runAt = rows[0].run_at as Date;
    expect(runAt.getTime()).toBeGreaterThan(Date.now());
  });
});
