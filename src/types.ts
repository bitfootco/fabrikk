export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dead';

export interface Job<Payload> {
  id: string;
  name: string;
  payload: Payload;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  backoff: string | null;
  run_at: Date | null;
  priority: number;
  error: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
  failed_at: Date | null;
}

// Raw shape returned by pg — payload is untyped until converted to Job<P>
export interface JobRow {
  id: string;
  name: string;
  payload: unknown;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  backoff: string | null;
  run_at: Date | null;
  priority: number;
  error: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
  failed_at: Date | null;
}

import { Pool } from 'pg';
import type { RetriesConfig, RetriesJobOptions } from './batteries/retries';
import type { HooksBus } from './batteries/hooks';
import type { RateLimitConfig, RateLimitMap } from './batteries/rate-limit';
import type { DashboardConfig } from './batteries/dashboard';

export type { RetriesConfig, RetriesJobOptions };
export type { RateLimitConfig };
export type { DashboardConfig };

interface BatteriesConfig {
  retries?: RetriesConfig;
  hooks?: boolean;
  dlq?: boolean;
  cron?: boolean;
  priority?: boolean;
  rateLimit?: boolean;
  fanout?: boolean;
  dashboard?: DashboardConfig;
}

type QueueConfigBase = { pollIntervalMs?: number; batteries?: BatteriesConfig };

export type QueueConfig =
  | (QueueConfigBase & { connectionString: string; poolSize?: number })
  | (QueueConfigBase & { pool: Pool });

export interface EnqueueOptions {
  maxAttempts?: number;
  retries?: RetriesJobOptions;
  priority?: number;
}

export type WorkerHandler<Payload> = (job: Job<Payload>, signal: AbortSignal) => Promise<void>;

export interface IWorker {
  wait(): Promise<void>;
}

export interface WorkerContext {
  retries?: RetriesConfig;
  hooks?: HooksBus;
  priority: boolean;
  dlq: boolean;
  rateLimits?: RateLimitMap;
}

export interface JobEntry<Payload> {
  job: Job<Payload>;
  done(): Promise<void>;
}
