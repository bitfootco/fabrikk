export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'dead';

export interface Job<Payload> {
  id: string;
  name: string;
  payload: Payload;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
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
  error: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
  failed_at: Date | null;
}

import { Pool } from 'pg';

type QueueConfigBase = { pollIntervalMs?: number };

export type QueueConfig =
  | (QueueConfigBase & { connectionString: string; poolSize?: number })
  | (QueueConfigBase & { pool: Pool });

export interface EnqueueOptions {
  maxAttempts?: number;
}

export type WorkerHandler<Payload> = (job: Job<Payload>, signal: AbortSignal) => Promise<void>;

export interface JobEntry<Payload> {
  job: Job<Payload>;
  done(): Promise<void>;
}
