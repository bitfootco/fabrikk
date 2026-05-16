import { IncomingMessage, ServerResponse } from 'http';
import { Pool } from 'pg';

export interface DashboardConfig {
  path: string;
}

interface QueueStat {
  name: string;
  depth: number;
  throughput: number;
  error_count: number;
}

export class DashboardHandler {
  constructor(
    private readonly pool: Pool,
    private readonly config: DashboardConfig,
  ) {}

  handler(): (req: IncomingMessage, res: ServerResponse) => void {
    return (req, res) => {
      this.dispatch(req, res).catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        this.json(res, 500, { error: message });
      });
    };
  }

  private async dispatch(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const rawUrl = req.url ?? '/';
    const mountPath = this.config.path.replace(/\/$/, '');

    // Strip mount prefix if present (Fastify passes full URL; Express strips it already)
    let relative = rawUrl.startsWith(mountPath) ? rawUrl.slice(mountPath.length) : rawUrl;
    if (!relative.startsWith('/')) relative = '/' + relative;

    const qmark = relative.indexOf('?');
    const pathname = qmark === -1 ? relative : relative.slice(0, qmark);
    const search = qmark === -1 ? '' : relative.slice(qmark + 1);
    const params = new URLSearchParams(search);

    const method = req.method ?? 'GET';

    // GET /health
    if (method === 'GET' && pathname === '/health') {
      return this.json(res, 200, { status: 'ok' });
    }

    // GET /queues
    if (method === 'GET' && pathname === '/queues') {
      return this.getQueues(res);
    }

    // GET /cron
    if (method === 'GET' && pathname === '/cron') {
      return this.getCron(res);
    }

    // GET /jobs
    if (method === 'GET' && pathname === '/jobs') {
      return this.listJobs(res, params);
    }

    // /jobs/:id routes
    const jobMatch = pathname.match(/^\/jobs\/([^/]+)(\/replay)?$/);
    if (jobMatch) {
      const id = jobMatch[1];
      const isReplay = jobMatch[2] === '/replay';

      if (method === 'GET' && !isReplay) return this.getJob(res, id);
      if (method === 'POST' && isReplay) return this.replayJob(res, id);
      if (method === 'DELETE' && !isReplay) return this.deleteJob(res, id);
    }

    this.json(res, 404, { error: 'Not found' });
  }

  private async listJobs(res: ServerResponse, params: URLSearchParams): Promise<void> {
    const name = params.get('name');
    const status = params.get('status');
    const limit = Math.min(parseInt(params.get('limit') ?? '50', 10), 200);

    const VALID_STATUSES = new Set(['pending', 'running', 'completed', 'failed', 'dead']);
    if (status && !VALID_STATUSES.has(status)) {
      return this.json(res, 400, {
        error: `Invalid status '${status}'. Must be one of: ${[...VALID_STATUSES].join(', ')}`,
      });
    }

    const conditions: string[] = [];
    const vals: unknown[] = [];

    if (name) {
      conditions.push(`name = $${vals.length + 1}`);
      vals.push(name);
    }
    if (status) {
      conditions.push(`status = $${vals.length + 1}`);
      vals.push(status);
    }
    vals.push(limit);

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const result = await this.pool.query(
      `SELECT * FROM fabrikk_jobs ${where} ORDER BY created_at DESC LIMIT $${vals.length}`,
      vals,
    );
    this.json(res, 200, result.rows);
  }

  private async getJob(res: ServerResponse, id: string): Promise<void> {
    const result = await this.pool.query('SELECT * FROM fabrikk_jobs WHERE id = $1', [id]);
    if (result.rows.length === 0) return this.json(res, 404, { error: 'Job not found' });
    this.json(res, 200, result.rows[0]);
  }

  private async replayJob(res: ServerResponse, id: string): Promise<void> {
    const result = await this.pool.query(
      `UPDATE fabrikk_jobs
       SET status = 'pending', error = NULL, failed_at = NULL, attempts = 0
       WHERE id = $1 AND status IN ('dead', 'failed')
       RETURNING id`,
      [id],
    );
    if (result.rows.length === 0)
      return this.json(res, 404, { error: 'Job not found or not replayable' });
    this.json(res, 200, { id: result.rows[0].id });
  }

  private async deleteJob(res: ServerResponse, id: string): Promise<void> {
    const result = await this.pool.query('DELETE FROM fabrikk_jobs WHERE id = $1 RETURNING id', [
      id,
    ]);
    if (result.rows.length === 0) return this.json(res, 404, { error: 'Job not found' });
    this.json(res, 200, { id: result.rows[0].id });
  }

  private async getQueues(res: ServerResponse): Promise<void> {
    const result = await this.pool.query<QueueStat>(
      `SELECT
         name,
         COUNT(*) FILTER (WHERE status = 'pending')::int AS depth,
         COUNT(*) FILTER (WHERE status = 'completed' AND completed_at >= NOW() - INTERVAL '1 hour')::int AS throughput,
         COUNT(*) FILTER (WHERE status IN ('failed', 'dead'))::int AS error_count
       FROM fabrikk_jobs
       GROUP BY name
       ORDER BY name`,
    );
    this.json(res, 200, result.rows);
  }

  private async getCron(res: ServerResponse): Promise<void> {
    try {
      const result = await this.pool.query('SELECT * FROM fabrikk_cron ORDER BY next_run ASC');
      this.json(res, 200, result.rows);
    } catch (err: unknown) {
      // 42P01 = undefined_table — cron battery is absent, return empty list
      if (err instanceof Error && (err as Error & { code?: string }).code === '42P01') {
        return this.json(res, 200, []);
      }
      throw err;
    }
  }

  private json(res: ServerResponse, status: number, body: unknown): void {
    const payload = JSON.stringify(body);
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(payload);
  }
}
