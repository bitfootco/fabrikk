import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    passWithNoTests: true,
    globalSetup: ['./test/globalSetup.ts'],
    fileParallelism: false,
  },
});
