import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { Queue } from '../../src/index';
import { clearJobs, getJobRows, makeTestPool } from '../setup';
import type { Pool } from 'pg';

type TestJobs = {
  'send-email': { to: string };
  'resize-image': { imageId: string };
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

describe('zero footprint when rateLimit battery is absent', () => {
  it('throws when setRateLimit is called without battery', async () => {
    const queue = new Queue<TestJobs>({ pool, pollIntervalMs: 20 });
    expect(() => queue.setRateLimit('send-email', { max: 10, window: '1m' })).toThrow(
      'RateLimit battery is not enabled',
    );
    await queue.stop();
  });
});

describe('rateLimit battery — basic enforcement', () => {
  it('processes jobs normally when under the rate limit', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { rateLimit: true },
    });
    queue.setRateLimit('send-email', { max: 10, window: '1m' });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.enqueue('send-email', { to: 'b@b.com' });

    queue.work('send-email', async () => {});

    await vi.waitFor(
      async () => {
        const rows = await getJobRows(pool, 'send-email');
        expect(rows.every((r) => r.status === 'completed')).toBe(true);
      },
      { timeout: 5000, interval: 30 },
    );

    await queue.stop();
    const rows = await getJobRows(pool, 'send-email');
    expect(rows.every((r) => r.status === 'completed')).toBe(true);
  });

  it('stops claiming once the rate limit is reached', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { rateLimit: true },
    });
    // Set limit to 1 per minute
    queue.setRateLimit('send-email', { max: 1, window: '1m' });

    await queue.enqueue('send-email', { to: 'first@b.com' });
    await queue.enqueue('send-email', { to: 'second@b.com' });

    let processed = 0;
    queue.work('send-email', async () => {
      processed++;
    });

    // Wait enough time for at least one poll cycle
    await new Promise((r) => setTimeout(r, 200));
    await queue.stop();

    // Only one job should have been processed (limit: 1 per minute)
    expect(processed).toBe(1);

    const rows = await getJobRows(pool, 'send-email');
    const completed = rows.filter((r) => r.status === 'completed');
    const pending = rows.filter((r) => r.status === 'pending');
    expect(completed).toHaveLength(1);
    expect(pending).toHaveLength(1);
  });

  it('rate limit on one queue does not affect another queue', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { rateLimit: true },
    });
    // Strictly limit send-email but not resize-image
    queue.setRateLimit('send-email', { max: 1, window: '1m' });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.enqueue('send-email', { to: 'b@b.com' });
    await queue.enqueue('resize-image', { imageId: 'img-1' });
    await queue.enqueue('resize-image', { imageId: 'img-2' });

    const resizeProcessed: string[] = [];
    queue.work('send-email', async () => {});
    queue.work('resize-image', async (job) => {
      resizeProcessed.push(job.payload.imageId);
    });

    await vi.waitFor(
      async () => {
        const rows = await getJobRows(pool, 'resize-image');
        expect(rows.every((r) => r.status === 'completed')).toBe(true);
      },
      { timeout: 5000, interval: 30 },
    );

    await queue.stop();

    // All resize-image jobs should complete regardless of send-email rate limit
    expect(resizeProcessed).toHaveLength(2);
  });

  it('enforces cluster-wide via Postgres — second instance respects limit set by first', async () => {
    const pool2 = makeTestPool();

    // queue1 processes one job, exhausting the 1/min limit
    const queue1 = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { rateLimit: true },
    });
    queue1.setRateLimit('send-email', { max: 1, window: '1m' });
    await queue1.enqueue('send-email', { to: 'a@b.com' });

    let q1Processed = 0;
    queue1.work('send-email', async () => {
      q1Processed++;
    });

    // Wait for queue1 to complete the first job (so started_at is recorded in DB)
    await vi.waitFor(
      async () => {
        const rows = await pool.query("SELECT * FROM fabrikk_jobs WHERE name = 'send-email'");
        expect(rows.rows.some((r: { status: string }) => r.status === 'completed')).toBe(true);
      },
      { timeout: 5000, interval: 30 },
    );

    await queue1.stop();
    expect(q1Processed).toBe(1);

    // Enqueue a second job — the limit is already hit for this window
    await pool.query(
      'INSERT INTO fabrikk_jobs (name, payload, max_attempts) VALUES (\'send-email\', \'{"to":"b@b.com"}\', 3)',
    );

    // queue2 should not pick it up
    const queue2 = new Queue<TestJobs>({
      pool: pool2,
      pollIntervalMs: 20,
      batteries: { rateLimit: true },
    });
    queue2.setRateLimit('send-email', { max: 1, window: '1m' });

    let q2Processed = 0;
    queue2.work('send-email', async () => {
      q2Processed++;
    });

    await new Promise((r) => setTimeout(r, 150));
    await queue2.stop();
    await pool2.end();

    expect(q2Processed).toBe(0);
  });
});
