import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Queue } from '../src/index';
import { clearJobs, deferred, getJobRows, makeTestPool } from './setup';
import type { Pool } from 'pg';

type TestJobs = {
  'send-email': { to: string; subject: string };
  'resize-image': { url: string; width: number };
};

let pool: Pool;
let queue: Queue<TestJobs>;

beforeEach(async () => {
  pool = makeTestPool();
  queue = new Queue<TestJobs>({ pool, pollIntervalMs: 50 });
  await clearJobs(pool);
});

afterEach(async () => {
  await queue.stop();
  await clearJobs(pool);
  await pool.end();
});

describe('enqueue', () => {
  it('inserts a pending job with correct fields', async () => {
    await queue.enqueue('send-email', { to: 'alice@example.com', subject: 'Hello' });

    const rows = await getJobRows(pool, 'send-email');
    expect(rows).toHaveLength(1);
    const row = rows[0];
    expect(row.name).toBe('send-email');
    expect(row.payload).toEqual({ to: 'alice@example.com', subject: 'Hello' });
    expect(row.status).toBe('pending');
    expect(row.attempts).toBe(0);
    expect(row.max_attempts).toBe(3);
    expect(row.error).toBeNull();
    expect(row.started_at).toBeNull();
    expect(row.completed_at).toBeNull();
    expect(row.failed_at).toBeNull();
    expect(row.id).toBeTruthy();
    expect(row.created_at).toBeInstanceOf(Date);
  });

  it('respects the per-job retries.attempts override', async () => {
    await queue.enqueue(
      'send-email',
      { to: 'bob@example.com', subject: 'Retry me' },
      { retries: { attempts: 7 } },
    );

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].max_attempts).toBe(7);
  });
});

describe('stop', () => {
  it('resolves immediately when no workers have been started', async () => {
    await expect(queue.stop()).resolves.toBeUndefined();
  });

  it('resolves when a worker is idle (no pending jobs)', async () => {
    queue.work('send-email', async () => {});
    await expect(queue.stop()).resolves.toBeUndefined();
  });
});

describe('jobs() iterator', () => {
  it('yields a pending job and done() marks it completed', async () => {
    await queue.enqueue('send-email', { to: 'carol@example.com', subject: 'Iterator test' });

    const iter = queue.jobs('send-email');
    const iterResult = deferred<void>();

    (async () => {
      for await (const entry of iter) {
        expect(entry.job.name).toBe('send-email');
        expect(entry.job.payload).toEqual({ to: 'carol@example.com', subject: 'Iterator test' });
        await entry.done();
        iterResult.resolve();
        break;
      }
    })().catch(iterResult.reject);

    await iterResult.promise;
    await queue.stop();

    const rows = await getJobRows(pool, 'send-email');
    expect(rows[0].status).toBe('completed');
    expect(rows[0].completed_at).toBeInstanceOf(Date);
  });
});
