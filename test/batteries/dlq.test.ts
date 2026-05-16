import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

import { Queue } from '../../src/index';
import { clearJobs, makeTestPool } from '../setup';

type TestJobs = {
  'send-email': { to: string };
  'process-image': { url: string };
};

let pool: Pool;

beforeEach(async () => {
  pool = makeTestPool();
  await clearJobs(pool);
  await pool.query('DROP TABLE IF EXISTS fabrikk_dlq');
});

afterEach(async () => {
  await clearJobs(pool);
  await pool.query('DROP TABLE IF EXISTS fabrikk_dlq');
  await pool.end();
});

describe('zero footprint when dlq battery is absent', () => {
  it('marks job dead on exhaustion when retries is active but dlq is absent', async () => {
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
    await new Promise((resolve) => setTimeout(resolve, 500));
    await queue.stop();

    const result = await pool.query('SELECT * FROM fabrikk_jobs WHERE name = $1', ['send-email']);
    expect(result.rows[0].status).toBe('dead');
  });
});

describe('dlq battery — dead letter queue', () => {
  it('moves exhausted job to dlq table', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 2, backoff: 'fixed', baseDelay: 10 },
        dlq: true,
      },
    });

    queue.work('send-email', async () => {
      throw new Error('always fails');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await new Promise((resolve) => setTimeout(resolve, 500));
    await queue.stop();

    const jobsResult = await pool.query('SELECT * FROM fabrikk_jobs WHERE name = $1', [
      'send-email',
    ]);
    expect(jobsResult.rowCount).toBe(0);

    const dlqResult = await pool.query('SELECT * FROM fabrikk_dlq');
    expect(dlqResult.rowCount).toBe(1);
    expect(dlqResult.rows[0].name).toBe('send-email');
    expect(dlqResult.rows[0].error).toBe('always fails');
    expect(dlqResult.rows[0].status).toBe('dead');
    expect(dlqResult.rows[0].attempts).toBe(2);
    expect(dlqResult.rows[0].dead_at).toBeTruthy();
  });

  it('lists all dead letters', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 1, backoff: 'fixed', baseDelay: 10 },
        dlq: true,
      },
    });

    queue.work('send-email', async () => {
      throw new Error('email fail');
    });

    queue.work('process-image', async () => {
      throw new Error('image fail');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.enqueue('process-image', { url: 'https://example.com/img.png' });
    await new Promise((resolve) => setTimeout(resolve, 500));
    await queue.stop();

    const all = await queue.dlq!.list();
    expect(all).toHaveLength(2);

    const filtered = await queue.dlq!.list('send-email');
    expect(filtered).toHaveLength(1);
    expect(filtered[0].name).toBe('send-email');
  });

  it('replays a dead letter back into the main queue', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 1, backoff: 'fixed', baseDelay: 10 },
        dlq: true,
      },
    });

    queue.work('send-email', async () => {
      throw new Error('fail');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await new Promise((resolve) => setTimeout(resolve, 300));
    await queue.stop();

    const dead = await queue.dlq!.list('send-email');
    expect(dead).toHaveLength(1);

    await queue.dlq!.replay(dead[0].id);

    const dlqAfter = await queue.dlq!.list('send-email');
    expect(dlqAfter).toHaveLength(0);

    const jobsResult = await pool.query('SELECT * FROM fabrikk_jobs WHERE name = $1', [
      'send-email',
    ]);
    expect(jobsResult.rows[0].attempts).toBe(0);
    expect(jobsResult.rows[0].status).toBe('pending');
  });

  it('discards a dead letter permanently', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 1, backoff: 'fixed', baseDelay: 10 },
        dlq: true,
      },
    });

    queue.work('send-email', async () => {
      throw new Error('fail');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await new Promise((resolve) => setTimeout(resolve, 300));
    await queue.stop();

    const dead = await queue.dlq!.list('send-email');
    expect(dead).toHaveLength(1);

    await queue.dlq!.discard(dead[0].id);

    const dlqAfter = await queue.dlq!.list('send-email');
    expect(dlqAfter).toHaveLength(0);
  });

  it('replays all dead letters for a given job name', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        retries: { attempts: 1, backoff: 'fixed', baseDelay: 10 },
        dlq: true,
      },
    });

    queue.work('send-email', async () => {
      throw new Error('email fail');
    });

    queue.work('process-image', async () => {
      throw new Error('image fail');
    });

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.enqueue('send-email', { to: 'b@b.com' });
    await queue.enqueue('process-image', { url: 'https://example.com/img.png' });
    await new Promise((resolve) => setTimeout(resolve, 300));
    await queue.stop();

    await queue.dlq!.replayAll('send-email');

    const remaining = await queue.dlq!.list('send-email');
    expect(remaining).toHaveLength(0);

    const stillDead = await queue.dlq!.list('process-image');
    expect(stillDead).toHaveLength(1);

    const jobsResult = await pool.query(
      "SELECT * FROM fabrikk_jobs WHERE name = 'send-email' AND status = 'pending'",
    );
    expect(jobsResult.rowCount).toBe(2);
  });
});
