import { Pool } from 'pg';

export interface DlqEntry {
  id: string;
  name: string;
  payload: unknown;
  status: string;
  attempts: number;
  max_attempts: number;
  backoff: string | null;
  error: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
  failed_at: Date | null;
  dead_at: Date;
}

export async function moveToDlq(pool: Pool, id: string, error: unknown): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  const client = await pool.connect();
  let done = false;
  try {
    await client.query('BEGIN');
    const result = await client.query<DlqEntry>(
      'SELECT * FROM fabrikk_jobs WHERE id = $1 FOR UPDATE',
      [id],
    );
    if (result.rows.length === 0) {
      await client.query('ROLLBACK');
      done = true;
      return;
    }
    const job = result.rows[0];
    await client.query(
      `INSERT INTO fabrikk_dlq (id, name, payload, status, attempts, max_attempts, backoff, error, created_at, started_at, completed_at, failed_at, dead_at)
       VALUES ($1, $2, $3, 'dead', $4, $5, $6, $7, $8, $9, $10, $11, NOW())`,
      [
        job.id,
        job.name,
        job.payload,
        job.attempts,
        job.max_attempts,
        job.backoff,
        message,
        job.created_at,
        job.started_at,
        job.completed_at,
        job.failed_at,
      ],
    );
    await client.query('DELETE FROM fabrikk_jobs WHERE id = $1', [id]);
    await client.query('COMMIT');
    done = true;
  } catch (err) {
    if (!done) {
      await client.query('ROLLBACK').catch(() => undefined);
    }
    throw err;
  } finally {
    client.release();
  }
}

export class DlqApi {
  constructor(private readonly pool: Pool) {}

  async list(name?: string): Promise<DlqEntry[]> {
    if (name) {
      const result = await this.pool.query<DlqEntry>(
        'SELECT * FROM fabrikk_dlq WHERE name = $1 ORDER BY dead_at ASC',
        [name],
      );
      return result.rows;
    }
    const result = await this.pool.query<DlqEntry>(
      'SELECT * FROM fabrikk_dlq ORDER BY dead_at ASC',
    );
    return result.rows;
  }

  async replay(id: string): Promise<void> {
    const client = await this.pool.connect();
    let done = false;
    try {
      await client.query('BEGIN');
      const result = await client.query<DlqEntry>(
        'SELECT * FROM fabrikk_dlq WHERE id = $1 FOR UPDATE',
        [id],
      );
      if (result.rows.length === 0) {
        await client.query('ROLLBACK');
        done = true;
        throw new Error(`DLQ entry ${id} not found`);
      }
      const entry = result.rows[0];
      await client.query(
        `INSERT INTO fabrikk_jobs (id, name, payload, status, attempts, max_attempts, error, created_at)
         VALUES ($1, $2, $3, 'pending', 0, $4, NULL, $5)`,
        [entry.id, entry.name, entry.payload, entry.max_attempts, entry.created_at],
      );
      await client.query('DELETE FROM fabrikk_dlq WHERE id = $1', [entry.id]);
      await client.query('COMMIT');
      done = true;
    } catch (err) {
      if (!done) {
        await client.query('ROLLBACK').catch(() => undefined);
      }
      throw err;
    } finally {
      client.release();
    }
  }

  async discard(id: string): Promise<void> {
    await this.pool.query('DELETE FROM fabrikk_dlq WHERE id = $1', [id]);
  }

  async replayAll(name?: string): Promise<void> {
    const entries = name ? await this.list(name) : await this.list();
    for (const entry of entries) {
      await this.replay(entry.id);
    }
  }
}
