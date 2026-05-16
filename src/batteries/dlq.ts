import { Pool } from 'pg';

import { toErrorMessage, withTransaction } from '../db';

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
  const message = toErrorMessage(error);
  await withTransaction(pool, async (client) => {
    const result = await client.query<DlqEntry>(
      'SELECT * FROM fabrikk_jobs WHERE id = $1 FOR UPDATE',
      [id],
    );
    if (result.rows.length === 0) {
      // Job already gone — early return causes withTransaction to COMMIT an empty
      // transaction, which is harmless. Any write added before this check must
      // precede it or be moved outside the guard.
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
  });
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
    await withTransaction(this.pool, async (client) => {
      const result = await client.query<DlqEntry>(
        'SELECT * FROM fabrikk_dlq WHERE id = $1 FOR UPDATE',
        [id],
      );
      if (result.rows.length === 0) {
        throw new Error(`DLQ entry ${id} not found`);
      }
      const entry = result.rows[0];
      await client.query(
        `INSERT INTO fabrikk_jobs (id, name, payload, status, attempts, max_attempts, backoff, error, created_at)
         VALUES ($1, $2, $3, 'pending', 0, $4, $5, NULL, $6)`,
        [entry.id, entry.name, entry.payload, entry.max_attempts, entry.backoff, entry.created_at],
      );
      await client.query('DELETE FROM fabrikk_dlq WHERE id = $1', [entry.id]);
    });
  }

  async discard(id: string): Promise<void> {
    await this.pool.query('DELETE FROM fabrikk_dlq WHERE id = $1', [id]);
  }

  async replayAll(name?: string): Promise<void> {
    const entries = name ? await this.list(name) : await this.list();
    for (const entry of entries) {
      try {
        await this.replay(entry.id);
      } catch (err) {
        // Entry was already replayed or discarded by another process — skip it
        if (err instanceof Error && err.message === `DLQ entry ${entry.id} not found`) continue;
        throw err;
      }
    }
  }
}
