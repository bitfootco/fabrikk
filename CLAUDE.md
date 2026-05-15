# CLAUDE.md — Fabrikk

This file gives you everything you need to work on this codebase autonomously. Read it fully before writing any code.

---

## What is Fabrikk?

Fabrikk is a Postgres-native job queue for TypeScript. The guiding principle is **bare by default, powerful by choice** — the core install is three concepts (define, enqueue, work), and every advanced feature is an opt-in battery via config.

The README.md is the source of truth for the public API. When in doubt, implement what the README describes — don't invent surface area.

---

## Stack

- **Language:** TypeScript (strict mode, no `any`)
- **Runtime:** Node.js 18+
- **Database:** Postgres 14+ via the `pg` package (the only allowed runtime dependency in core)
- **Test runner:** Vitest
- **Package manager:** npm

---

## Project Structure

```
fabrikk/
├── src/
│   ├── index.ts              # Public exports only
│   ├── queue.ts              # Queue class — core enqueue/work/stop
│   ├── worker.ts             # Worker loop, AbortSignal wiring
│   ├── iterator.ts           # Async iterator interface
│   ├── schema.ts             # Schema bootstrap and self-heal logic
│   ├── types.ts              # Shared internal types
│   └── batteries/
│       ├── retries.ts
│       ├── dlq.ts
│       ├── cron.ts
│       ├── priority.ts
│       ├── rate-limit.ts
│       ├── fanout.ts
│       ├── hooks.ts
│       └── dashboard.ts
├── test/
│   ├── setup.ts              # DB setup/teardown helpers
│   ├── queue.test.ts
│   ├── worker.test.ts
│   └── batteries/
│       └── *.test.ts
├── README.md
├── CLAUDE.md                 # This file
├── package.json
├── tsconfig.json
└── .env.example
```

Batteries live under `src/batteries/`. Each battery is self-contained: it may register additional schema, attach event listeners, or extend the Queue class — but it must not affect anything if it is not included in config.

---

## Core Design Principles

### 1. The Jobs generic is the source of type truth

The `Queue<Jobs>` generic flows types from definition through to worker handlers and enqueue calls. This is the most important TypeScript constraint in the project. Never break it.

```ts
type Jobs = {
  'send-email': { to: string; subject: string }
}

const queue = new Queue<Jobs>({ connectionString: '...' })

queue.work('send-email', async (job) => {
  job.payload // { to: string; subject: string } — no casting
})
```

### 2. Batteries must be zero-footprint when absent

If a battery is not in config, it must contribute nothing — no schema tables, no event listeners, no polling. Use a plugin registration pattern internally so core never imports battery code unconditionally.

### 3. The library owns its schema

On `new Queue()`, Fabrikk runs schema bootstrap before resolving. It creates any missing tables and repairs any schema drift idempotently. It must be safe to run in a multi-instance deploy — use `CREATE TABLE IF NOT EXISTS` and advisory locks for any operations that must not run concurrently.

All tables are prefixed `fabrikk_`. Never touch tables outside this prefix.

### 4. Graceful shutdown is not optional

Every worker receives an `AbortSignal`. `queue.stop()` signals all workers, waits for in-flight jobs up to `gracePeriodMs`, then closes the PG pool. This must work correctly even if `stop()` is called before any workers are started.

### 5. Strict TypeScript, no exceptions

- `strict: true` in tsconfig
- No `any`, no `as unknown as X` casts unless absolutely unavoidable with a comment explaining why
- All public API types must be exported from `src/index.ts`

---

## Database Conventions

- All tables: `fabrikk_jobs`, `fabrikk_cron`, `fabrikk_dlq`, etc.
- Use `pg` (node-postgres) directly — no ORM, no query builder
- Use parameterised queries everywhere — never string-interpolate user data into SQL
- Use `SELECT ... FOR UPDATE SKIP LOCKED` for job claiming — this is the standard Postgres queue pattern
- Use Postgres advisory locks (`pg_try_advisory_lock`) for operations that must be cluster-singleton (cron scheduling, schema bootstrap)
- Timestamps: always `timestamptz`, always UTC

---

## Testing

Tests require a real Postgres database. The connection string is read from `DATABASE_URL` in `.env` (copy `.env.example` to get started).

```bash
npm test           # run all tests
npm run test:watch # watch mode
```

### Test conventions

- Each test file gets its own isolated queue instance
- Use the `setup.ts` helpers to create and drop test schemas between runs — never share state between tests
- Integration tests (hitting a real DB) live in `test/`. Unit tests for pure logic can live alongside source files as `*.unit.test.ts`
- Test the public API, not internals — if you need to test something internal, consider whether it should be a separate exported utility

---

## Build

```bash
npm run build        # tsc → dist/
npm run lint         # eslint
npm run typecheck    # tsc --noEmit
bash scripts/check.sh  # full validation: Prettier + ESLint + tsc + Vitest (run before pushing)
```

The `dist/` directory is what gets published. It must contain `.d.ts` files alongside `.js` — `declaration: true` in tsconfig.

Husky guards are wired up automatically after `npm install`:
- **pre-commit**: runs `lint-staged` (Prettier + ESLint on staged files only)
- **pre-push**: runs the full `scripts/check.sh` suite

---

## Adding a Battery

1. Create `src/batteries/your-battery.ts`
2. Export a `register(queue: QueueInternal, config: YourBatteryConfig): void` function
3. In `schema.ts`, add any new tables behind a battery-guard — only created if the battery is registered
4. In `queue.ts`, call `register()` during init if the battery is present in config
5. Export any public types from `src/index.ts`
6. Add tests under `test/batteries/your-battery.test.ts`
7. Document in README.md under the Batteries section

---

## What NOT to Do

- Do not add runtime dependencies beyond `pg` to the core package. Batteries may have their own optional peer dependencies but must not require them unconditionally.
- Do not add a CLI. Fabrikk is a library.
- Do not invent API surface not described in README.md without flagging it first.
- Do not use `setTimeout`-based polling in tests — use Vitest's fake timers or trigger worker ticks directly via exported test helpers.
- Do not catch and swallow errors silently. Surface them via the `hooks` battery if enabled, or re-throw.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | Postgres connection string |

---

## Current Status

The project is greenfield. Nothing is built yet. The README.md defines the target API. Start with:

1. `package.json` + `tsconfig.json` scaffolding
2. `src/schema.ts` — schema bootstrap and self-heal
3. `src/queue.ts` — `Queue<Jobs>` class with `enqueue` and `work`
4. `src/worker.ts` — worker loop with `AbortSignal`
5. `src/iterator.ts` — async iterator interface
6. Core tests passing against a real DB
7. Then batteries, one at a time, in this order: `retries` → `hooks` → `dlq` → `cron` → `priority` → `rateLimit` → `fanout` → `dashboard`
