export interface RetriesConfig {
  attempts?: number;
  backoff: 'exponential' | 'linear' | 'fixed';
  baseDelay?: number;
}

export interface RetriesJobOptions {
  attempts?: number;
  backoff?: 'exponential' | 'linear' | 'fixed';
}

export function computeDelay(attempt: number, config: RetriesConfig): number {
  const base = config.baseDelay ?? 1000;
  let delay: number;
  switch (config.backoff) {
    case 'exponential':
      delay = base * Math.pow(2, attempt - 1);
      break;
    case 'linear':
      delay = base * attempt;
      break;
    case 'fixed':
    default:
      delay = base;
  }
  const jitter = delay * 0.2 * (Math.random() * 2 - 1);
  return Math.max(0, Math.round(delay + jitter));
}
