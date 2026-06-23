#!/usr/bin/env bash
# Shim — the real push_forecast.sh now lives at the repo root.
# Kept so anything referencing this path keeps working.
exec "$(dirname "${BASH_SOURCE[0]}")/../../push_forecast.sh" "$@"
