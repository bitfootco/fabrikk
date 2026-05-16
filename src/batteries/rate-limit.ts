import { PoolClient } from 'pg';

export interface RateLimitConfig {
  max: number;
  window: '1s' | '1m' | '1h';
}

export type RateLimitMap = Map<string, RateLimitConfig>;

function windowToInterval(window: '1s' | '1m' | '1h'): string {
  if (window === '1s') return '1 second';
  if (window === '1m') return '1 minute';
  if (window === '1h') return '1 hour';
  throw new Error(`Unknown rate-limit window: ${window as string}`);
}

// Returns true if the rate limit has not been reached (safe to claim).
// Must be called inside an open transaction — the count and the subsequent claim must be atomic.
export async function checkRateLimit(
  client: PoolClient,
  name: string,
  config: RateLimitConfig,
): Promise<boolean> {
  const interval = windowToInterval(config.window);
  const result = await client.query<{ count: string }>(
    `SELECT COUNT(*) AS count FROM fabrikk_jobs
     WHERE name = $1 AND started_at >= NOW() - ($2)::INTERVAL`,
    [name, interval],
  );
  return parseInt(result.rows[0].count, 10) < config.max;
}
