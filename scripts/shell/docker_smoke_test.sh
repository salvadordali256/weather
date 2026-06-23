#!/usr/bin/env bash
#
# Docker smoke test for the forecast dashboard.
#
# Builds the image, starts a container, waits for the dashboard to come up,
# and asserts that the key routes return HTTP 200. Tears everything down on
# exit (success or failure). Run this locally where a Docker daemon exists —
# it is NOT part of the NAS cron pipeline.
#
# Usage:
#   bash scripts/shell/docker_smoke_test.sh            # build + test
#   KEEP_RUNNING=1 bash scripts/shell/docker_smoke_test.sh   # leave it up
#
# Exit codes: 0 = all checks passed, non-zero = a step failed.

set -euo pipefail

# --- Run from the repo root (this script lives at scripts/shell/) -------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

IMAGE="snowforecast-dashboard:smoke"
CONTAINER="snowforecast-smoke"
HOST_PORT="${HOST_PORT:-5005}"     # host port (avoids clashing with a dev :5000)
URL="http://127.0.0.1:${HOST_PORT}"

# --- Pick docker compose flavor (plugin vs legacy) ---------------------------
if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not installed or not on PATH." >&2
  exit 127
fi

cleanup() {
  if [ "${KEEP_RUNNING:-0}" = "1" ]; then
    echo
    echo "KEEP_RUNNING=1 set — leaving container '${CONTAINER}' up at ${URL}"
    echo "Stop it with: docker rm -f ${CONTAINER}"
    return
  fi
  echo
  echo "--- Cleaning up ---"
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== [1/4] Building image (${IMAGE}) ==="
docker build -t "${IMAGE}" .

echo
echo "=== [2/4] Starting container (${CONTAINER}) on ${URL} ==="
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" -p "${HOST_PORT}:5000" "${IMAGE}" >/dev/null

echo
echo "=== [3/4] Waiting for the dashboard to respond ==="
ATTEMPTS=30
ok=0
for i in $(seq 1 "${ATTEMPTS}"); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "${URL}/api/forecast" || true)"
  if [ "${code}" = "200" ]; then
    echo "  up after ${i}s (HTTP 200 on /api/forecast)"
    ok=1
    break
  fi
  # If the container died, surface its logs and bail early.
  if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "ERROR: container exited during startup. Logs:" >&2
    docker logs "${CONTAINER}" >&2 || true
    exit 1
  fi
  sleep 1
done

if [ "${ok}" != "1" ]; then
  echo "ERROR: dashboard did not return HTTP 200 within ${ATTEMPTS}s. Logs:" >&2
  docker logs "${CONTAINER}" >&2 || true
  exit 1
fi

echo
echo "=== [4/4] Checking routes ==="
fail=0
check() {
  local path="$1" expected="$2"
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' "${URL}${path}" || echo 000)"
  if [ "${code}" = "${expected}" ]; then
    printf "  OK    %-18s -> HTTP %s\n" "${path}" "${code}"
  else
    printf "  FAIL  %-18s -> HTTP %s (expected %s)\n" "${path}" "${code}" "${expected}"
    fail=1
  fi
}

check "/"                 200
check "/api/forecast"     200
check "/api/history"      200
check "/about"            200

# The seeded forecast JSON should be valid and contain a 'forecasts' array.
if curl -s "${URL}/api/forecast" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert isinstance(d.get("forecasts"), list) and d["forecasts"], "no forecasts array"' 2>/dev/null; then
  echo "  OK    /api/forecast JSON shape (forecasts[] present)"
else
  echo "  FAIL  /api/forecast JSON shape"
  fail=1
fi

echo
if [ "${fail}" = "0" ]; then
  echo "SMOKE TEST PASSED ✅"
else
  echo "SMOKE TEST FAILED ❌" >&2
  docker logs "${CONTAINER}" >&2 || true
fi
exit "${fail}"
