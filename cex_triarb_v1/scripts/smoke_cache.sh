#!/usr/bin/env bash
set -euo pipefail

REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"
PATTERN="${PATTERN:-md:l1:*}"

echo "[smoke_cache] Checking Redis keys at $REDIS_URL matching $PATTERN"
keys=$($REDIS_CLI -u "$REDIS_URL" --scan --pattern "$PATTERN" | head -n 5 || true)
if [[ -z "$keys" ]]; then
  echo "[smoke_cache] FAIL: no keys found"
  exit 1
fi
echo "[smoke_cache] OK"
echo "$keys"
