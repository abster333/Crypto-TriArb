#!/usr/bin/env bash
set -euo pipefail

HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8085/healthz}"

echo "[smoke_ws] Checking health at $HEALTH_URL"
resp=$(curl -fsSL "$HEALTH_URL" || true)
if [[ -z "$resp" ]]; then
  echo "[smoke_ws] FAIL: no response"
  exit 1
fi
echo "[smoke_ws] OK"
echo "$resp"
