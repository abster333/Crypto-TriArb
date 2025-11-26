#!/usr/bin/env bash
set -euo pipefail

export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
export SNAPSHOT_SUBJECT="${SNAPSHOT_SUBJECT:-md.snapshot}"
export STRAT_ROI_BPS="${STRAT_ROI_BPS:-5.0}"
export STRAT_START_NOTIONAL="${STRAT_START_NOTIONAL:-1000}"

# Example cycles:
# STRAT_CYCLES="COINBASE:BTCUSD:BUY,COINBASE:ETHBTC:BUY,COINBASE:ETHUSD:SELL"

python -m cex_triarb_v1.strategy_main "$@"
