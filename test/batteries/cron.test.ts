import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Pool } from 'pg';

import { Queue } from '../../src/index';
import { CronScheduler, getNextRun } from '../../src/batteries/cron';
import { clearJobs, makeTestPool } from '../setup';

type TestJobs = {
  'send-email': { to: string };
  'nightly-report': { recipients: string[] };
};

let pool: Pool;

beforeEach(async () => {
  pool = makeTestPool();
  await clearJobs(pool);
  await pool.query('DROP TABLE IF EXISTS fabrikk_cron');
});

afterEach(async () => {
  await clearJobs(pool);
  await pool.query('DROP TABLE IF EXISTS fabrikk_cron');
  await pool.end();
});

// Helper: create a scheduler that never polls on its own (signal pre-aborted),
// so tests drive it deterministically via processDueSchedules().
function makeScheduler(retriesConfig?: ConstructorParameters<typeof CronScheduler>[4]) {
  const ac = new AbortController();
  ac.abort();
  return new CronScheduler(pool, ac.signal, 1_000_000, Promise.resolve(), retriesConfig);
}

describe('zero footprint when cron battery is absent', () => {
  it('does not create fabrikk_cron table when cron is absent', async () => {
    const queue = new Queue<TestJobs>({ pool });
    await queue.stop();

    const result = await pool.query(
      "SELECT 1 FROM information_schema.tables WHERE table_name = 'fabrikk_cron'",
    );
    expect(result.rowCount).toBe(0);
  });

  it('throws when calling queue.cron without battery enabled', async () => {
    const queue = new Queue<TestJobs>({ pool });

    await expect(queue.cron('send-email', '* * * * *', { to: 'a@b.com' })).rejects.toThrow(
      'Cron battery is not enabled',
    );

    await queue.stop();
  });
});

describe('cron battery — scheduling', () => {
  it('creates fabrikk_cron table when cron is enabled', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      batteries: { cron: true },
    });
    await queue.stop();

    const result = await pool.query(
      "SELECT 1 FROM information_schema.tables WHERE table_name = 'fabrikk_cron'",
    );
    expect(result.rowCount).toBe(1);
  });

  it('inserts a cron schedule on queue.cron()', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      batteries: { cron: true },
    });

    await queue.cron('send-email', '0 9 * * 1-5', { to: 'hi@example.com' });
    await queue.stop();

    const result = await pool.query('SELECT * FROM fabrikk_cron WHERE job_name = $1', [
      'send-email',
    ]);
    expect(result.rowCount).toBe(1);
    expect(result.rows[0].expression).toBe('0 9 * * 1-5');
    expect(result.rows[0].payload).toEqual({ to: 'hi@example.com' });
    expect(result.rows[0].next_run).toBeTruthy();
  });

  it('updates schedule payload on duplicate (name, expression)', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      batteries: { cron: true },
    });

    await queue.cron('send-email', '0 9 * * *', { to: 'a@example.com' });
    await queue.cron('send-email', '0 9 * * *', { to: 'b@example.com' });
    await queue.stop();

    const result = await pool.query('SELECT * FROM fabrikk_cron WHERE job_name = $1', [
      'send-email',
    ]);
    expect(result.rowCount).toBe(1);
    expect(result.rows[0].payload).toEqual({ to: 'b@example.com' });
  });
});

describe('cron battery — enqueue on due schedule', () => {
  it('enqueues a job when a schedule is due', async () => {
    // Bootstrap schema, then stop auto-polling
    const queue = new Queue<TestJobs>({ pool, batteries: { cron: true } });
    await queue.stop();

    const past = new Date(Date.now() - 60_000);
    await pool.query(
      `INSERT INTO fabrikk_cron (job_name, expression, payload, next_run)
       VALUES ($1, $2, $3, $4)`,
      ['send-email', '* * * * *', JSON.stringify({ to: 'cron@example.com' }), past],
    );

    await makeScheduler().processDueSchedules();

    const jobs = await pool.query('SELECT * FROM fabrikk_jobs WHERE name = $1', ['send-email']);
    expect(jobs.rowCount).toBe(1);
    expect(jobs.rows[0].payload).toEqual({ to: 'cron@example.com' });
    expect(jobs.rows[0].status).toBe('pending');

    const schedules = await pool.query('SELECT * FROM fabrikk_cron WHERE job_name = $1', [
      'send-email',
    ]);
    expect(schedules.rowCount).toBe(1);
    expect(schedules.rows[0].next_run).toBeTruthy();
    expect(new Date(schedules.rows[0].next_run).getTime()).toBeGreaterThan(Date.now());
  });

  it('respects retries battery backoff configuration', async () => {
    const queue = new Queue<TestJobs>({
      pool,
      batteries: {
        cron: true,
        retries: { attempts: 5, backoff: 'linear', baseDelay: 2000 },
      },
    });
    await queue.stop();

    const past = new Date(Date.now() - 60_000);
    await pool.query(
      `INSERT INTO fabrikk_cron (job_name, expression, payload, next_run)
       VALUES ($1, $2, $3, $4)`,
      ['send-email', '* * * * *', JSON.stringify({ to: 'test@example.com' }), past],
    );

    await makeScheduler({ attempts: 5, backoff: 'linear', baseDelay: 2000 }).processDueSchedules();

    const jobs = await pool.query('SELECT * FROM fabrikk_jobs WHERE name = $1', ['send-email']);
    expect(jobs.rowCount).toBe(1);
    expect(jobs.rows[0].max_attempts).toBe(5);
    expect(jobs.rows[0].backoff).toBe('linear');
  });
});

describe('getNextRun — cron expression parser', () => {
  it('matches simple every-minute expression', () => {
    const after = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
    const result = getNextRun('* * * * *', after);
    expect(result).toEqual(new Date(Date.UTC(2024, 0, 1, 0, 1, 0)));
  });

  it('respects fixed minute and hour', () => {
    const after = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
    const result = getNextRun('0 9 * * *', after);
    expect(result).toEqual(new Date(Date.UTC(2024, 0, 1, 9, 0, 0)));
  });

  it('advances to the next hour if minute already passed', () => {
    const after = new Date(Date.UTC(2024, 0, 1, 9, 5, 0));
    const result = getNextRun('0 9 * * *', after);
    expect(result).toEqual(new Date(Date.UTC(2024, 0, 2, 9, 0, 0)));
  });

  it('handles weekday ranges', () => {
    const after = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
    // Mon-Fri at 9am
    const result = getNextRun('0 9 * * 1-5', after);
    // Jan 1 2024 is Monday
    expect(result).toEqual(new Date(Date.UTC(2024, 0, 1, 9, 0, 0)));
  });

  it('jumps from Sunday to Monday for weekday-only', () => {
    const after = new Date(Date.UTC(2024, 0, 7, 10, 0, 0)); // Sun Jan 7
    const result = getNextRun('0 9 * * 1-5', after);
    expect(result).toEqual(new Date(Date.UTC(2024, 0, 8, 9, 0, 0))); // Mon Jan 8
  });

  it('handles exact month constraint', () => {
    const after = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
    const result = getNextRun('0 0 1 1 *', after);
    // Already Jan 1 00:00, should skip to next year
    expect(result).toEqual(new Date(Date.UTC(2025, 0, 1, 0, 0, 0)));
  });

  it('throws on invalid 6-field expression', () => {
    expect(() => getNextRun('* * * * * *', new Date())).toThrow('Expected 5 fields');
  });

  it('throws on non-numeric field values', () => {
    expect(() => getNextRun('foo * * * *', new Date())).toThrow('Non-numeric value found');
  });

  it('throws on malformed ranges', () => {
    expect(() => getNextRun('1- * * * *', new Date())).toThrow('Malformed range');
    expect(() => getNextRun('-5 * * * *', new Date())).toThrow('Malformed range');
  });

  it('throws on invalid range order', () => {
    expect(() => getNextRun('30-10 * * * *', new Date())).toThrow('Range start must be <= end');
  });

  it('throws on out-of-range minutes', () => {
    expect(() => getNextRun('60 * * * *', new Date())).toThrow('Invalid minute: 60');
  });

  it('throws on out-of-range hours', () => {
    expect(() => getNextRun('* 24 * * *', new Date())).toThrow('Invalid hour: 24');
  });

  it('throws on out-of-range day of month', () => {
    expect(() => getNextRun('* * 0 * *', new Date())).toThrow('Invalid day of month: 0');
    expect(() => getNextRun('* * 32 * *', new Date())).toThrow('Invalid day of month: 32');
  });

  it('throws on out-of-range month', () => {
    expect(() => getNextRun('* * * 0 *', new Date())).toThrow('Invalid month: 0');
    expect(() => getNextRun('* * * 13 *', new Date())).toThrow('Invalid month: 13');
  });

  it('throws on out-of-range day of week', () => {
    expect(() => getNextRun('* * * * 8', new Date())).toThrow('Invalid day of week: 8');
  });

  it('handles comma-separated values', () => {
    const after = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
    const result = getNextRun('15,30,45 * * * *', after);
    expect(result).toEqual(new Date(Date.UTC(2024, 0, 1, 0, 15, 0)));
  });

  it('handles mixed comma and range values', () => {
    const after = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
    const result = getNextRun('5,10-15,50 * * * *', after);
    expect(result).toEqual(new Date(Date.UTC(2024, 0, 1, 0, 5, 0)));
  });
});
