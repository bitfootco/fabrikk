import { Pool } from 'pg';

import { JobRow } from '../src/types';

export function makeTestPool(): Pool {
  return new Pool({ connectionString: process.env.DATABASE_URL });
}

export async function clearJobs(pool: Pool): Promise<void> {
  await pool.query('DELETE FROM fabrikk_jobs');
}

export async function getJobRows(pool: Pool, name?: string): Promise<JobRow[]> {
  const result = name
    ? await pool.query<JobRow>(
        'SELECT * FROM fabrikk_jobs WHERE name = $1 ORDER BY created_at ASC',
        [name],
      )
    : await pool.query<JobRow>('SELECT * FROM fabrikk_jobs ORDER BY created_at ASC');
  return result.rows;
}

export interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

export function deferred<T = void>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
