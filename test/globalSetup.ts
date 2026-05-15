import { Pool } from 'pg';

import { bootstrap } from '../src/schema';

// Bootstraps the fabrikk schema once before all test files run.
// Individual tests just DELETE FROM fabrikk_jobs in beforeEach.
export async function setup(): Promise<void> {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  try {
    await bootstrap(pool);
  } finally {
    await pool.end();
  }
}
