# Roadmap

## Fabrikk — Postgres-native job queue for TypeScript

Goal: implement the full public API defined in `README.md`, with every commit and push guarded by rigorous quality gates.

- [x] M1: Project scaffolding and quality gates — `package.json`, `tsconfig.json`, `.env.example`, `eslint.config.js`, `prettier.config.js`, `vitest.config.ts`, `scripts/check.sh` (Prettier + ESLint + tsc + Vitest), Husky pre-commit (`lint-staged`) and pre-push (full `check.sh`) hooks
- [x] M2: Schema bootstrap — `src/schema.ts` with `fabrikk_jobs` table, advisory-lock-guarded bootstrap, idempotent self-heal {depends: M1}
- [x] M3: Core queue — `src/types.ts`, `src/queue.ts` (`enqueue`/`work`/`stop`), `src/worker.ts` (AbortSignal loop, `SELECT … FOR UPDATE SKIP LOCKED`), `src/iterator.ts` (async iterator), `src/index.ts` (public exports) {depends: M2}
- [x] M4: Core tests — `test/setup.ts`, `test/queue.test.ts`, `test/worker.test.ts` against real Postgres; enqueue, claim, complete, stop, and iterator all green {depends: M3}
- [x] M5: Battery — retries — exponential/linear/fixed backoff with jitter, per-job override at enqueue time, zero footprint when absent {depends: M4}
- [x] M6: Battery — hooks — typed lifecycle events (`job:enqueued`, `job:started`, `job:completed`, `job:failed`, `job:retrying`, `job:dead`), `event.jobName` narrows payload type {depends: M4}
- [x] M7: Battery — dlq — `fabrikk_dlq` table, `queue.dlq.list/replay/discard/replayAll`, requires retries battery {depends: M5}
- [x] M8: Battery — cron — `fabrikk_cron` table, `queue.cron(name, expression, payload)`, advisory-lock deduplication across workers {depends: M4}
- [ ] M9: Battery — priority — `priority` column on `fabrikk_jobs`, `ORDER BY priority DESC, created_at ASC` in worker claim query {depends: M4}
- [ ] M10: Battery — rateLimit — `queue.setRateLimit(name, { max, window })`, cluster-wide enforcement via Postgres timestamptz window {depends: M4}
- [ ] M11: Battery — fanout — `queue.fanout(source, [targets])`, atomic multi-enqueue on source job insert {depends: M4}
- [ ] M12: Battery — dashboard — `queue.dashboardHandler()` Node.js handler, all REST endpoints, Express + Fastify compatible {depends: M4}
- [ ] M13: Publish prep — `npm run build` green, `dist/` has `.js` + `.d.ts` pairs, `package.json` `name=@bitfootco/fabrikk`, `main`/`exports`/`types`/`files` fields correct, `npm pack` produces a valid tarball {depends: M5, M6, M7, M8, M9, M10, M11, M12}
