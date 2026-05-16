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

const CREATE_DLQ_TABLE = `
  CREATE TABLE IF NOT EXISTS fabrikk_dlq (
    id           UUID        PRIMARY KEY,
    name         TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}',
    status       TEXT        NOT NULL DEFAULT 'dead',
    attempts     INTEGER     NOT NULL DEFAULT 0,
    max_attempts INTEGER     NOT NULL DEFAULT 3,
    backoff      TEXT,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failed_at    TIMESTAMPTZ,
    dead_at      TIMESTAMPTZ NOT NULL DEFAULT now()
  )
`;

const CREATE_DLQ_NAME_INDEX = `
  CREATE INDEX IF NOT EXISTS fabrikk_dlq_name_idx
    ON fabrikk_dlq (name)
`;

const CREATE_CRON_TABLE = `
  CREATE TABLE IF NOT EXISTS fabrikk_cron (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_name     TEXT        NOT NULL,
    expression   TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}',
    next_run     TIMESTAMPTZ,
    last_run     TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Upsert key: (job_name, expression). Changing expression for the same job name
    -- creates a second active row — callers must delete the old schedule explicitly.
    CONSTRAINT fabrikk_cron_job_expression UNIQUE (job_name, expression)
  )
`;

const CREATE_CRON_NEXT_RUN_INDEX = `
  CREATE INDEX IF NOT EXISTS fabrikk_cron_next_run_idx
    ON fabrikk_cron (next_run)
    WHERE next_run IS NOT NULL
`;

interface BootstrapBatteries {
  retries?: RetriesConfig;
  dlq?: boolean;
  cron?: boolean;
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

      if (batteries?.dlq) {
        await client.query(CREATE_DLQ_TABLE);
        await client.query(CREATE_DLQ_NAME_INDEX);
      }

      if (batteries?.cron) {
        await client.query(CREATE_CRON_TABLE);
        await client.query(CREATE_CRON_NEXT_RUN_INDEX);
      }
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
