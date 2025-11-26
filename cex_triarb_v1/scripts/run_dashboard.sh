#!/usr/bin/env bash
set -euo pipefail

export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
export SNAPSHOT_SUBJECT="${SNAPSHOT_SUBJECT:-md.snapshot}"
export UI_PORT="${UI_PORT:-8086}"

python -m cex_triarb_v1.ui_main "$@"
