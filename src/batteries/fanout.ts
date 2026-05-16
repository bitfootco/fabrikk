import { Pool } from 'pg';

export class FanoutRegistry {
  private readonly rules = new Map<string, string[]>();

  register(source: string, targets: string[]): void {
    if (this.rules.has(source)) {
      throw new Error(
        `Fanout rule for '${source}' is already registered. Call fanout() only once per source job.`,
      );
    }
    this.rules.set(source, targets);
  }

  get(source: string): string[] | undefined {
    return this.rules.get(source);
  }
}

// Atomically inserts one job row per target using the same cols/vals built in Queue.enqueue.
// `cols` must not include 'name' — it is substituted per-target.
// `vals` must not include the name value — it is prepended per-target.
// Returns the inserted job IDs in the same order as `targets`.
export async function enqueueTargets(
  pool: Pool,
  targets: string[],
  cols: string[],
  vals: unknown[],
): Promise<string[]> {
  const ids: string[] = [];
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const colList = ['name', ...cols].join(', ');
    for (const target of targets) {
      const rowVals = [target, ...vals];
      const placeholders = rowVals.map((_, i) => `$${i + 1}`).join(', ');
      const result = await client.query<{ id: string }>(
        `INSERT INTO fabrikk_jobs (${colList}) VALUES (${placeholders}) RETURNING id`,
        rowVals,
      );
      ids.push(result.rows[0].id);
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
  return ids;
}
