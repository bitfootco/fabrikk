import { Pool, PoolClient } from 'pg';

export function toErrorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export async function withTransaction<T>(
  pool: Pool,
  fn: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  let done = false;
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    done = true;
    return result;
  } catch (err) {
    if (!done) {
      await client.query('ROLLBACK').catch(() => undefined);
    }
    throw err;
  } finally {
    client.release();
  }
}
