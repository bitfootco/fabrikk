import { Pool, PoolClient } from 'pg';
import type { RetriesConfig } from './batteries/retries';

// Arbitrary stable bigint — uniquely identifies Fabrikk's schema bootstrap lock cluster-wide
const FABRIKK_LOCK_KEY = 7482910423;

const CREATE_JOBS_TABLE = `
  CREATE TABLE IF NOT EXISTS fabrikk_jobs (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}',
    status       TEXT        NOT NULL DEFAULT 'pending',
    attempts     INTEGER     NOT NULL DEFAULT 0,
    max_attempts INTEGER     NOT NULL DEFAULT 3,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failed_at    TIMESTAMPTZ,
    CONSTRAINT fabrikk_jobs_status_check
      CHECK (status IN ('pending','running','completed','failed','dead'))
  )
`;

const CREATE_CLAIM_INDEX = `
  CREATE INDEX IF NOT EXISTS fabrikk_jobs_claim_idx
    ON fabrikk_jobs (status, created_at)
    WHERE status = 'pending'
`;

interface ColumnDef {
  name: string;
  ddl: string;
}

const BASE_COLUMNS: ColumnDef[] = [
  { name: 'id', ddl: 'id UUID NOT NULL DEFAULT gen_random_uuid()' },
  { name: 'name', ddl: 'name TEXT NOT NULL' },
  { name: 'payload', ddl: "payload JSONB NOT NULL DEFAULT '{}'" },
  { name: 'status', ddl: "status TEXT NOT NULL DEFAULT 'pending'" },
  { name: 'attempts', ddl: 'attempts INTEGER NOT NULL DEFAULT 0' },
  { name: 'max_attempts', ddl: 'max_attempts INTEGER NOT NULL DEFAULT 3' },
  { name: 'error', ddl: 'error TEXT' },
  { name: 'created_at', ddl: 'created_at TIMESTAMPTZ NOT NULL DEFAULT now()' },
  { name: 'started_at', ddl: 'started_at TIMESTAMPTZ' },
  { name: 'completed_at', ddl: 'completed_at TIMESTAMPTZ' },
  { name: 'failed_at', ddl: 'failed_at TIMESTAMPTZ' },
];

const RETRIES_COLUMNS: ColumnDef[] = [
  { name: 'run_at', ddl: 'run_at TIMESTAMPTZ NOT NULL DEFAULT now()' },
  { name: 'backoff', ddl: "backoff TEXT NOT NULL DEFAULT 'exponential'" },
];

interface BootstrapBatteries {
  retries?: RetriesConfig;
}

export async function bootstrap(pool: Pool, batteries?: BootstrapBatteries): Promise<void> {
  const client = await pool.connect();
  try {
    const lockResult = await client.query<{ pg_try_advisory_lock: boolean }>(
      'SELECT pg_try_advisory_lock($1)',
      [FABRIKK_LOCK_KEY],
    );

    if (!lockResult.rows[0].pg_try_advisory_lock) {
      // Another instance is bootstrapping — safe to skip, table will exist when they finish
      return;
    }

    try {
      await client.query(CREATE_JOBS_TABLE);
      await client.query(CREATE_CLAIM_INDEX);
      const expectedColumns = batteries?.retries
        ? [...BASE_COLUMNS, ...RETRIES_COLUMNS]
        : BASE_COLUMNS;
      await selfHeal(client, expectedColumns);
    } finally {
      await client.query('SELECT pg_advisory_unlock($1)', [FABRIKK_LOCK_KEY]);
    }
  } finally {
    client.release();
  }
}

async function selfHeal(client: PoolClient, expectedColumns: ColumnDef[]): Promise<void> {
  const existing = await client.query<{ column_name: string }>(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = current_schema()
       AND table_name = 'fabrikk_jobs'`,
  );

  const existingNames = new Set(existing.rows.map((r: { column_name: string }) => r.column_name));

  for (const col of expectedColumns) {
    if (!existingNames.has(col.name)) {
      await client.query(`ALTER TABLE fabrikk_jobs ADD COLUMN IF NOT EXISTS ${col.ddl}`);
    }
  }
}
