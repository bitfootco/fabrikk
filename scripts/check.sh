#!/usr/bin/env bash
set -euo pipefail

echo "→ Prettier..."
npx prettier --check .

echo "→ ESLint..."
npx eslint --no-error-on-unmatched-pattern src test

echo "→ tsc..."
npx tsc --noEmit

echo "→ Vitest..."
npx vitest run

echo "✓ All checks passed"
