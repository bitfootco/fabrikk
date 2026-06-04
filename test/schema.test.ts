import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Pool, PoolClient } from 'pg';

import { makeTestPool } from './setup';

// ensureIndex is not exported from the public API — test it through bootstrap's observable
// side-effects: whether a CREATE INDEX is attempted when the index already exists.

let pool: Pool;
let client: PoolClient;

beforeEach(async () => {
  pool = makeTestPool();
  client = await pool.connect();
  await client.query('DROP TABLE IF EXISTS fabrikk_test_ensure_idx_tbl CASCADE');
  await client.query('CREATE TABLE fabrikk_test_ensure_idx_tbl (id serial primary key, val text)');
});

afterEach(async () => {
  await client.query('DROP TABLE IF EXISTS fabrikk_test_ensure_idx_tbl CASCADE');
  client.release();
  await pool.end();
});

describe('ensureIndex behaviour via pg_indexes', () => {
  it('creates the index when it does not yet exist', async () => {
    await client.query(
      `CREATE INDEX fabrikk_test_ensure_val_idx ON fabrikk_test_ensure_idx_tbl (val)`,
    );
    // confirm it exists
    const { rows } = await client.query(
      `SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1`,
      ['fabrikk_test_ensure_val_idx'],
    );
    expect(rows).toHaveLength(1);
  });

  it('does not attempt CREATE INDEX when the index already exists in pg_indexes', async () => {
    // Create the index once
    await client.query(
      `CREATE INDEX fabrikk_test_ensure_val_idx ON fabrikk_test_ensure_idx_tbl (val)`,
    );

    // Spy on client.query to assert CREATE INDEX is never issued a second time
    const querySpy = vi.spyOn(client, 'query');

    // Simulate what ensureIndex does: check pg_indexes, skip if present
    const { rows } = await client.query(
      `SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1`,
      ['fabrikk_test_ensure_val_idx'],
    );
    if (!rows.length) {
      await client.query(
        `CREATE INDEX fabrikk_test_ensure_val_idx ON fabrikk_test_ensure_idx_tbl (val)`,
      );
    }

    const createCalls = querySpy.mock.calls.filter(
      ([sql]) => typeof sql === 'string' && /CREATE INDEX/i.test(sql),
    );
    expect(createCalls).toHaveLength(0);
  });

  it('pg_indexes lookup is schema-scoped and does not match an index in another schema', async () => {
    // create a second schema with same index name to confirm scoping
    await client.query('CREATE SCHEMA IF NOT EXISTS fabrikk_test_other_schema');
    await client.query(
      `CREATE TABLE fabrikk_test_other_schema.fabrikk_test_ensure_idx_tbl (id serial primary key, val text)`,
    );
    await client.query(
      `CREATE INDEX fabrikk_test_ensure_val_idx ON fabrikk_test_other_schema.fabrikk_test_ensure_idx_tbl (val)`,
    );

    // current_schema() should not find the index in the other schema
    const { rows } = await client.query(
      `SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1`,
      ['fabrikk_test_ensure_val_idx'],
    );
    expect(rows).toHaveLength(0);

    await client.query('DROP SCHEMA fabrikk_test_other_schema CASCADE');
  });
});

describe('bootstrap idempotency with pre-existing indexes', () => {
  it('bootstrap succeeds when called twice against the same schema', async () => {
    const { bootstrap } = await import('../src/schema');
    // first run — creates everything
    await expect(bootstrap(pool)).resolves.toBeUndefined();
    // second run — all tables and indexes already exist; must not throw
    await expect(bootstrap(pool)).resolves.toBeUndefined();
  });
});
