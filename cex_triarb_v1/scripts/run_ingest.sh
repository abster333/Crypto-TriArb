#!/usr/bin/env bash
set -euo pipefail

export SYMBOLS="${SYMBOLS:-BTCUSD,ETHUSD,ETHBTC}"
export EXCHANGES="${EXCHANGES:-COINBASE,KRAKEN}"
export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
export HEALTH_PORT="${HEALTH_PORT:-8085}"

python -m cex_triarb_v1.main "$@"
