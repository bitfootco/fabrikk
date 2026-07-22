import { getEventListeners } from 'node:events';

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { interruptibleSleep } from '../src/worker';

const abortListeners = (signal: AbortSignal): number => getEventListeners(signal, 'abort').length;

describe('interruptibleSleep', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('removes the abort listener after the timer completes', async () => {
    const controller = new AbortController();
    const sleep = interruptibleSleep(100, controller.signal);

    expect(abortListeners(controller.signal)).toBe(1);
    await vi.advanceTimersByTimeAsync(100);
    await sleep;

    expect(abortListeners(controller.signal)).toBe(0);
  });

  it('clears the timer and removes the listener when aborted', async () => {
    const controller = new AbortController();
    const sleep = interruptibleSleep(100, controller.signal);

    controller.abort();
    await sleep;

    expect(vi.getTimerCount()).toBe(0);
    expect(abortListeners(controller.signal)).toBe(0);
  });

  it('resolves a pre-aborted signal without adding a listener', async () => {
    const controller = new AbortController();
    controller.abort();

    await interruptibleSleep(100, controller.signal);

    expect(vi.getTimerCount()).toBe(0);
    expect(abortListeners(controller.signal)).toBe(0);
  });

  it('does not accumulate listeners across repeated completed sleeps', async () => {
    const controller = new AbortController();

    for (let index = 0; index < 100; index += 1) {
      const sleep = interruptibleSleep(100, controller.signal);
      await vi.advanceTimersByTimeAsync(100);
      await sleep;
    }

    expect(abortListeners(controller.signal)).toBe(0);
  });
});
