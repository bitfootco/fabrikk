export interface HooksBus {
  emit(event: string, payload: unknown): void;
}

type JobEvent<Jobs, Extra extends Record<string, unknown> = Record<string, never>> = {
  [K in keyof Jobs & string]: {
    jobName: K;
    jobId: string;
    payload: Jobs[K];
  } & Extra;
}[keyof Jobs & string];

export type HooksEventMap<Jobs extends Record<string, unknown>> = {
  'job:enqueued': JobEvent<Jobs>;
  'job:started': JobEvent<Jobs>;
  'job:completed': JobEvent<Jobs, { durationMs: number }>;
  'job:retrying': JobEvent<Jobs, { error: string; attempt: number; delayMs: number }>;
  'job:failed': JobEvent<Jobs, { error: string }>;
  'job:dead': JobEvent<Jobs, { error: string }>;
};

export class HooksEmitter<Jobs extends Record<string, unknown>> implements HooksBus {
  private handlers = new Map<string, Set<(event: unknown) => void>>();

  on<E extends keyof HooksEventMap<Jobs>>(
    event: E,
    handler: (event: HooksEventMap<Jobs>[E]) => void,
  ): void {
    const key = event as string;
    const set = this.handlers.get(key) ?? new Set();
    set.add(handler as (event: unknown) => void);
    this.handlers.set(key, set);
  }

  emit(event: string, payload: unknown): void {
    this.handlers.get(event)?.forEach((h) => h(payload));
  }

  get size(): number {
    let count = 0;
    for (const set of this.handlers.values()) count += set.size;
    return count;
  }
}
