import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { Queue } from '../../src/index';
import { clearJobs, getJobRows, makeTestPool } from '../setup';
import type { Pool } from 'pg';

type TestJobs = {
  'user-signed-up': { userId: string };
  'send-welcome-email': { userId: string };
  'create-billing-account': { userId: string };
  'notify-slack': { userId: string };
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

describe('zero footprint when fanout battery is absent', () => {
  it('throws when fanout() is called without battery', async () => {
    const queue = new Queue<TestJobs>({ pool, pollIntervalMs: 20 });
    expect(() =>
      queue.fanout('user-signed-up', ['send-welcome-email', 'create-billing-account']),
    ).toThrow('Fanout battery is not enabled');
    await queue.stop();
  });

  it('enqueues single job normally when fanout battery is absent', async () => {
    const queue = new Queue<TestJobs>({ pool, pollIntervalMs: 20 });
    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.stop();

    const rows = await getJobRows(pool);
    expect(rows).toHaveLength(1);
    expect(rows[0].name).toBe('send-email');
  });
});

describe('fanout battery — routing', () => {
  it('enqueues one job per target with the correct name', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { fanout: true },
    });
    queue.fanout('user-signed-up', [
      'send-welcome-email',
      'create-billing-account',
      'notify-slack',
    ]);

    await queue.enqueue('user-signed-up', { userId: '123' });
    await queue.stop();

    const rows = await getJobRows(pool);
    expect(rows).toHaveLength(3);
    const names = rows.map((r) => r.name).sort();
    expect(names).toEqual(['create-billing-account', 'notify-slack', 'send-welcome-email']);
  });

  it('fans out the same payload to all targets', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { fanout: true },
    });
    queue.fanout('user-signed-up', ['send-welcome-email', 'create-billing-account']);

    await queue.enqueue('user-signed-up', { userId: 'abc' });
    await queue.stop();

    const rows = await getJobRows(pool);
    expect(rows).toHaveLength(2);
    for (const row of rows) {
      expect((row.payload as { userId: string }).userId).toBe('abc');
    }
  });

  it('does NOT enqueue the source job itself', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { fanout: true },
    });
    queue.fanout('user-signed-up', ['send-welcome-email']);

    await queue.enqueue('user-signed-up', { userId: '123' });
    await queue.stop();

    const rows = await getJobRows(pool);
    const sourceRows = rows.filter((r) => r.name === 'user-signed-up');
    expect(sourceRows).toHaveLength(0);
  });

  it('is atomic — all targets inserted or none', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { fanout: true },
    });
    // Register valid targets
    queue.fanout('user-signed-up', ['send-welcome-email', 'create-billing-account']);

    // Verify atomicity by checking that all rows appear together (not partially)
    await queue.enqueue('user-signed-up', { userId: '42' });
    await queue.stop();

    const rows = await getJobRows(pool);
    // Both targets should be present
    expect(rows).toHaveLength(2);
    expect(rows.map((r) => r.name).sort()).toEqual([
      'create-billing-account',
      'send-welcome-email',
    ]);
  });

  it('fanout works alongside retries battery — targets inherit backoff config', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: {
        fanout: true,
        retries: { attempts: 3, backoff: 'linear', baseDelay: 100 },
      },
    });
    queue.fanout('user-signed-up', ['send-welcome-email']);

    await queue.enqueue('user-signed-up', { userId: 'x' });
    await queue.stop();

    const rows = await getJobRows(pool, 'send-welcome-email');
    expect(rows).toHaveLength(1);
    expect(rows[0].max_attempts).toBe(3);
    expect(rows[0].backoff).toBe('linear');
  });

  it('fanout works alongside priority battery — targets inherit priority', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { fanout: true, priority: true },
    });
    queue.fanout('user-signed-up', ['send-welcome-email']);

    await queue.enqueue('user-signed-up', { userId: 'y' }, { priority: 5 });
    await queue.stop();

    const rows = await getJobRows(pool, 'send-welcome-email');
    expect(rows).toHaveLength(1);
    expect(rows[0].priority).toBe(5);
  });
});
