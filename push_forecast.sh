#!/usr/bin/env bash
# Root shim — keeps the NAS crontab working after reorg.
# Real script: scripts/shell/push_forecast.sh
exec "$(dirname "$0")/scripts/shell/push_forecast.sh" "$@"
