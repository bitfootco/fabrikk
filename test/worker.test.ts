import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Queue } from '../src/index';
import { Job } from '../src/types';
import { clearJobs, deferred, getJobRows, makeTestPool } from './setup';
import type { Pool } from 'pg';

type TestJobs = {
  task: { value: string };
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

describe('worker', () => {
  it('claims a job and marks it completed', async () => {
    const handled = deferred<Job<{ value: string }>>();

    queue.work('task', async (job) => {
      handled.resolve(job);
    });

    await queue.enqueue('task', { value: 'hello' });
    const job = await handled.promise;

    expect(job.name).toBe('task');
    expect(job.payload).toEqual({ value: 'hello' });

    await queue.stop();

    const rows = await getJobRows(pool, 'task');
    expect(rows[0].status).toBe('completed');
    expect(rows[0].completed_at).toBeInstanceOf(Date);
  });

  it('marks a job failed when the handler throws', async () => {
    const handled = deferred<void>();

    queue.work('task', async () => {
      handled.resolve();
      throw new Error('boom');
    });

    await queue.enqueue('task', { value: 'fail-me' });
    await handled.promise;

    await queue.stop();

    const rows = await getJobRows(pool, 'task');
    expect(rows[0].status).toBe('failed');
    expect(rows[0].error).toBe('boom');
    expect(rows[0].failed_at).toBeInstanceOf(Date);
  });

  it('two workers claim separate jobs (SKIP LOCKED)', async () => {
    await queue.enqueue('task', { value: 'job1' });
    await queue.enqueue('task', { value: 'job2' });

    const claimedIds = new Set<string>();
    const bothClaimed = deferred<void>();

    const handler = async (job: Job<{ value: string }>) => {
      claimedIds.add(job.id);
      if (claimedIds.size === 2) bothClaimed.resolve();
    };

    queue.work('task', handler);
    queue.work('task', handler);

    await bothClaimed.promise;

    expect(claimedIds.size).toBe(2);
  });

  it('stop() waits for an in-flight handler before resolving', async () => {
    const handlerStarted = deferred<void>();
    const allowComplete = deferred<void>();
    let completedBeforeStop = false;

    queue.work('task', async () => {
      handlerStarted.resolve();
      await allowComplete.promise;
      completedBeforeStop = true;
    });

    await queue.enqueue('task', { value: 'in-flight' });
    await handlerStarted.promise;

    const stopPromise = queue.stop();
    allowComplete.resolve();
    await stopPromise;

    expect(completedBeforeStop).toBe(true);

    const rows = await getJobRows(pool, 'task');
    expect(rows[0].status).toBe('completed');
  });
});
