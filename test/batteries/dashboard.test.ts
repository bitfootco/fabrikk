import * as http from 'http';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { Queue } from '../../src/index';
import { clearJobs, makeTestPool } from '../setup';
import type { Pool } from 'pg';

type TestJobs = {
  'send-email': { to: string };
  'resize-image': { imageId: string };
};

async function httpRequest(
  baseUrl: string,
  path: string,
  method = 'GET',
): Promise<{ status: number; body: unknown }> {
  return new Promise((resolve, reject) => {
    const url = new URL(path, baseUrl);
    const req = http.request(
      { hostname: url.hostname, port: url.port, path: url.pathname + url.search, method },
      (res) => {
        let data = '';
        res.on('data', (chunk: string) => (data += chunk));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode ?? 0, body: JSON.parse(data) });
          } catch {
            resolve({ status: res.statusCode ?? 0, body: data });
          }
        });
      },
    );
    req.on('error', reject);
    req.end();
  });
}

async function startTestServer(
  queue: Queue<TestJobs>,
): Promise<{ url: string; server: http.Server }> {
  const handler = queue.dashboardHandler();
  const server = http.createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const addr = server.address() as { port: number };
  return { url: `http://127.0.0.1:${addr.port}`, server };
}

async function closeServer(server: http.Server): Promise<void> {
  await new Promise<void>((resolve) => {
    // closeAllConnections available in Node 18.2+
    if ('closeAllConnections' in server) {
      (server as { closeAllConnections(): void }).closeAllConnections();
    }
    server.close(() => resolve());
  });
}

// ─── zero footprint ──────────────────────────────────────────────────────────

describe('zero footprint when dashboard battery is absent', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = makeTestPool();
    await clearJobs(pool);
  });

  afterAll(async () => {
    await clearJobs(pool);
    await pool.end();
  });

  it('throws when dashboardHandler() is called without battery', async () => {
    const queue = new Queue<TestJobs>({ pool, pollIntervalMs: 20 });
    expect(() => queue.dashboardHandler()).toThrow('Dashboard battery is not enabled');
    await queue.stop();
  });
});

// ─── health ──────────────────────────────────────────────────────────────────

describe('dashboard battery — health endpoint', () => {
  let pool: Pool;
  let queue: Queue<TestJobs>;
  let server: http.Server;
  let url: string;

  beforeAll(async () => {
    pool = makeTestPool();
    await clearJobs(pool);
    queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { dashboard: { path: '/queue' } },
    });
    ({ url, server } = await startTestServer(queue));
  });

  afterAll(async () => {
    await queue.stop();
    await closeServer(server);
    await clearJobs(pool);
    await pool.end();
  });

  it('GET /queue/health returns { status: ok }', async () => {
    const res = await httpRequest(url, '/queue/health');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: 'ok' });
  });
});

// ─── jobs endpoints ──────────────────────────────────────────────────────────

describe('dashboard battery — jobs endpoints', () => {
  let pool: Pool;
  let queue: Queue<TestJobs>;
  let server: http.Server;
  let url: string;

  beforeAll(async () => {
    pool = makeTestPool();
    queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { dashboard: { path: '/queue' } },
    });
    ({ url, server } = await startTestServer(queue));
  });

  beforeEach(async () => {
    await clearJobs(pool);
  });

  afterAll(async () => {
    await queue.stop();
    await closeServer(server);
    await clearJobs(pool);
    await pool.end();
  });

  it('GET /queue/jobs returns list of jobs', async () => {
    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.enqueue('send-email', { to: 'b@b.com' });

    const res = await httpRequest(url, '/queue/jobs');
    expect(res.status).toBe(200);
    expect(Array.isArray(res.body)).toBe(true);
    expect((res.body as unknown[]).length).toBe(2);
  });

  it('GET /queue/jobs?name=X filters by queue name', async () => {
    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.enqueue('resize-image', { imageId: 'img-1' });

    const res = await httpRequest(url, '/queue/jobs?name=send-email');
    expect(res.status).toBe(200);
    const jobs = res.body as Array<{ name: string }>;
    expect(jobs).toHaveLength(1);
    expect(jobs[0].name).toBe('send-email');
  });

  it('GET /queue/jobs/:id returns a single job', async () => {
    await queue.enqueue('send-email', { to: 'a@b.com' });
    const listRes = await httpRequest(url, '/queue/jobs');
    const jobs = listRes.body as Array<{ id: string }>;
    const id = jobs[0].id;

    const res = await httpRequest(url, `/queue/jobs/${id}`);
    expect(res.status).toBe(200);
    expect((res.body as { id: string }).id).toBe(id);
  });

  it('GET /queue/jobs/:id returns 404 for unknown id', async () => {
    const res = await httpRequest(url, '/queue/jobs/00000000-0000-0000-0000-000000000000');
    expect(res.status).toBe(404);
  });

  it('POST /queue/jobs/:id/replay re-enqueues a dead job', async () => {
    await queue.enqueue('send-email', { to: 'a@b.com' });
    const listRes = await httpRequest(url, '/queue/jobs');
    const jobs = listRes.body as Array<{ id: string }>;
    const id = jobs[0].id;

    await pool.query(`UPDATE fabrikk_jobs SET status = 'dead' WHERE id = $1`, [id]);

    const replayRes = await httpRequest(url, `/queue/jobs/${id}/replay`, 'POST');
    expect(replayRes.status).toBe(200);

    const jobRes = await httpRequest(url, `/queue/jobs/${id}`);
    expect((jobRes.body as { status: string }).status).toBe('pending');
  });

  it('DELETE /queue/jobs/:id removes the job', async () => {
    await queue.enqueue('send-email', { to: 'a@b.com' });
    const listRes = await httpRequest(url, '/queue/jobs');
    const jobs = listRes.body as Array<{ id: string }>;
    const id = jobs[0].id;

    const deleteRes = await httpRequest(url, `/queue/jobs/${id}`, 'DELETE');
    expect(deleteRes.status).toBe(200);

    const getRes = await httpRequest(url, `/queue/jobs/${id}`);
    expect(getRes.status).toBe(404);
  });
});

// ─── queues endpoint ─────────────────────────────────────────────────────────

describe('dashboard battery — queues endpoint', () => {
  let pool: Pool;
  let queue: Queue<TestJobs>;
  let server: http.Server;
  let url: string;

  beforeAll(async () => {
    pool = makeTestPool();
    queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { dashboard: { path: '/queue' } },
    });
    ({ url, server } = await startTestServer(queue));
  });

  beforeEach(async () => {
    await clearJobs(pool);
  });

  afterAll(async () => {
    await queue.stop();
    await closeServer(server);
    await clearJobs(pool);
    await pool.end();
  });

  it('GET /queue/queues returns per-queue stats', async () => {
    await queue.enqueue('send-email', { to: 'a@b.com' });
    await queue.enqueue('send-email', { to: 'b@b.com' });
    await queue.enqueue('resize-image', { imageId: 'img-1' });

    const res = await httpRequest(url, '/queue/queues');
    expect(res.status).toBe(200);
    const stats = res.body as Array<{ name: string; depth: number }>;
    expect(Array.isArray(stats)).toBe(true);
    const emailStats = stats.find((s) => s.name === 'send-email');
    expect(emailStats?.depth).toBe(2);
  });
});

// ─── cron endpoint ───────────────────────────────────────────────────────────

describe('dashboard battery — cron endpoint', () => {
  let pool: Pool;
  let server: http.Server;
  let url: string;
  let queue: Queue<TestJobs>;

  beforeAll(async () => {
    pool = makeTestPool();
    await clearJobs(pool);
  });

  afterAll(async () => {
    await closeServer(server);
    await clearJobs(pool);
    await pool.end();
  });

  afterEach(async () => {
    await queue.stop();
  });

  it('GET /queue/cron returns empty array when cron battery is absent', async () => {
    queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { dashboard: { path: '/queue' } },
    });
    ({ url, server } = await startTestServer(queue));

    const res = await httpRequest(url, '/queue/cron');
    expect(res.status).toBe(200);
    expect(res.body).toEqual([]);
  });

  it('GET /queue/cron returns schedules when cron battery is enabled', async () => {
    // Close previous server first
    if (server) await closeServer(server);

    queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { cron: true, dashboard: { path: '/queue' } },
    });
    ({ url, server } = await startTestServer(queue));
    await queue.cron('send-email', '0 9 * * 1-5', { to: 'digest@example.com' });

    const res = await httpRequest(url, '/queue/cron');
    expect(res.status).toBe(200);
    const schedules = res.body as Array<{ job_name: string }>;
    expect(schedules.length).toBeGreaterThan(0);
    expect(schedules[0].job_name).toBe('send-email');
  });
});

// ─── unknown routes ──────────────────────────────────────────────────────────

describe('dashboard battery — unknown routes', () => {
  let pool: Pool;
  let queue: Queue<TestJobs>;
  let server: http.Server;
  let url: string;

  beforeAll(async () => {
    pool = makeTestPool();
    await clearJobs(pool);
    queue = new Queue<TestJobs>({
      pool,
      pollIntervalMs: 20,
      batteries: { dashboard: { path: '/queue' } },
    });
    ({ url, server } = await startTestServer(queue));
  });

  afterAll(async () => {
    await queue.stop();
    await closeServer(server);
    await clearJobs(pool);
    await pool.end();
  });

  it('returns 404 for unrecognized paths', async () => {
    const res = await httpRequest(url, '/queue/unknown-route');
    expect(res.status).toBe(404);
  });
});
