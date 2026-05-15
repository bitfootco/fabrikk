import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

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

describe('hooks battery — zero footprint when absent', () => {
  it('queue.on() throws when hooks battery is not enabled', () => {
    const queue = new Queue<TestJobs>({ pool });
    expect(() => queue.on('job:enqueued', vi.fn())).toThrow('Hooks battery is not enabled');
  });

  it('hooks property is undefined when absent', () => {
    const queue = new Queue<TestJobs>({ pool });
    expect(queue.hooks).toBeUndefined();
  });
});

describe('hooks battery — job:enqueued', () => {
  it('emits job:enqueued with jobName, jobId, and payload', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      batteries: { hooks: true },
    });

    const handler = vi.fn();
    queue.on('job:enqueued', handler);

    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.stop();

    expect(handler).toHaveBeenCalledTimes(1);
    const event = handler.mock.calls[0][0];
    expect(event.jobName).toBe('send-email');
    expect(event.jobId).toEqual(expect.any(String));
    expect(event.payload).toEqual({ to: 'a@b.com' });
  });

  it('narrows payload type via event.jobName check', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      batteries: { hooks: true },
    });

    const enqueuedSpy = vi.fn();
    queue.on('job:enqueued', (event) => {
      if (event.jobName === 'send-email') {
        enqueuedSpy(event.payload.to);
      }
    });

    await queue.enqueue('send-email', { to: 'user@test.com' });
    await queue.stop();

    expect(enqueuedSpy).toHaveBeenCalledWith('user@test.com');
  });
});

describe('hooks battery — job lifecycle', () => {
  it('emits started then completed when handler succeeds', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { hooks: true },
    });

    const started = vi.fn();
    const completed = vi.fn();
    queue.on('job:started', started);
    queue.on('job:completed', completed);

    queue.work('send-email', async () => {});
    await queue.enqueue('send-email', { to: 'a@b.com' });

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

    expect(started).toHaveBeenCalledTimes(1);
    const startedEvent = started.mock.calls[0][0];
    expect(startedEvent.jobName).toBe('send-email');
    expect(startedEvent.payload).toEqual({ to: 'a@b.com' });

    expect(completed).toHaveBeenCalledTimes(1);
    const completedEvent = completed.mock.calls[0][0];
    expect(completedEvent.jobName).toBe('send-email');
    expect(completedEvent.durationMs).toEqual(expect.any(Number));
    expect(completedEvent.durationMs).toBeGreaterThanOrEqual(0);
  });

  it('emits failed when handler throws and retries are absent', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { hooks: true },
    });

    const failed = vi.fn();
    queue.on('job:failed', failed);

    queue.work('send-email', async () => {
      throw new Error('fail');
    });
    await queue.enqueue('send-email', { to: 'a@b.com' });

    await new Promise((resolve) => setTimeout(resolve, 200));
    await queue.stop();

    expect(failed).toHaveBeenCalledTimes(1);
    const event = failed.mock.calls[0][0];
    expect(event.error).toBe('fail');
    expect(event.jobName).toBe('send-email');
    expect(event.payload).toEqual({ to: 'a@b.com' });
  });

  it('emits retrying and then completed when retries succeed', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        hooks: true,
        retries: { attempts: 3, backoff: 'fixed', baseDelay: 10 },
      },
    });

    const retrying = vi.fn();
    queue.on('job:retrying', retrying);

    let callCount = 0;
    queue.work('send-email', async () => {
      callCount++;
      if (callCount < 3) throw new Error('not yet');
    });
    await queue.enqueue('send-email', { to: 'a@b.com' });

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

    expect(retrying).toHaveBeenCalledTimes(2);
    const firstRetry = retrying.mock.calls[0][0];
    expect(firstRetry.attempt).toBe(1);
    expect(firstRetry.error).toBe('not yet');
    expect(firstRetry.delayMs).toEqual(expect.any(Number));
    expect(firstRetry.jobName).toBe('send-email');
    expect(firstRetry.payload).toEqual({ to: 'a@b.com' });
  });

  it('emits dead when retries are exhausted', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        hooks: true,
        retries: { attempts: 2, backoff: 'fixed', baseDelay: 10 },
      },
    });

    const dead = vi.fn();
    queue.on('job:dead', dead);

    queue.work('send-email', async () => {
      throw new Error('dead');
    });
    await queue.enqueue('send-email', { to: 'a@b.com' });

    await new Promise((resolve) => setTimeout(resolve, 500));
    await queue.stop();

    expect(dead).toHaveBeenCalledTimes(1);
    const event = dead.mock.calls[0][0];
    expect(event.error).toBe('dead');
    expect(event.jobName).toBe('send-email');
  });
});
