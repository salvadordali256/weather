#!/bin/bash
# Audit today's pipeline run. Writes PIPELINE_STATUS.md and pipeline_status.json
# at the repo root so push_forecast.sh commits them alongside the forecast.
#
# Run from cron after the pipeline scripts, before push_forecast.sh:
#   ...daily_automated_forecast.py >>logs/cron.log 2>&1; \
#   bash scripts/shell/check_pipeline_health.sh >>logs/cron.log 2>&1; \
#   bash scripts/shell/push_forecast.sh >>logs/cron.log 2>&1

set -u
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

if [ -f .env ]; then
    set -a; . ./.env; set +a
fi
DB="${DB_PATH:-global_snowfall.db}"
FORECAST_DIR="${FORECAST_OUTPUT_DIR:-forecast_output}"
TODAY="$(date +%Y-%m-%d)"
NOW_ISO="$(date -Iseconds 2>/dev/null || date +%Y-%m-%dT%H:%M:%S%z)"

# 1. Tracebacks in today's logs (cron_*.log get appended to all day, so we
#    consider any traceback in a log modified today a fresh failure).
traceback_files=""
traceback_count=0
if [ -d logs ]; then
    for f in logs/cron_*.log; do
        [ -f "$f" ] || continue
        mod_date=$(date -r "$f" +%Y-%m-%d 2>/dev/null || stat -c %y "$f" 2>/dev/null | cut -d' ' -f1)
        [ "$mod_date" = "$TODAY" ] || continue
        count=$(grep -c "^Traceback" "$f" 2>/dev/null || echo 0)
        if [ "$count" -gt 0 ]; then
            traceback_count=$((traceback_count + count))
            traceback_files="${traceback_files}- \`$f\`: $count\n"
        fi
    done
fi

# 2. Expected outputs fresh today
expected="$FORECAST_DIR/latest_forecast.json \
$FORECAST_DIR/station_data.json \
$FORECAST_DIR/daily_report.json \
web/public/latest_forecast.json \
web/public/station_data.json \
web/public/daily_report.json"
stale_files=""
stale_count=0
for f in $expected; do
    if [ ! -f "$f" ]; then
        stale_files="${stale_files}- \`$f\`: missing\n"
        stale_count=$((stale_count + 1))
    else
        mod_date=$(date -r "$f" +%Y-%m-%d 2>/dev/null || stat -c %y "$f" 2>/dev/null | cut -d' ' -f1)
        if [ "$mod_date" != "$TODAY" ]; then
            stale_files="${stale_files}- \`$f\`: last modified $mod_date\n"
            stale_count=$((stale_count + 1))
        fi
    fi
done

# 3. Did collectors write rows for today?
today_rows=0
if [ -f "$DB" ]; then
    today_rows=$(sqlite3 "$DB" "SELECT COUNT(*) FROM snowfall_daily WHERE date = '$TODAY';" 2>/dev/null || echo 0)
fi

# 4. Last "Update forecast" commit (used by GH Action; we surface locally too)
last_forecast_commit=$(git log -1 --grep='^Update forecast' --format='%ai' 2>/dev/null || echo unknown)

# Overall verdict
overall=ok
[ "$traceback_count" -gt 0 ] && overall=fail
[ "$stale_count" -gt 0 ] && overall=fail
# today_rows==0 is a real warning in winter; treat as warn so it shows up but
# doesn't drown out true failures during the off-season.
[ "$today_rows" = "0" ] && [ "$overall" = "ok" ] && overall=warn

# pipeline_status.json — machine readable, used by GH Action
cat > pipeline_status.json <<JSON
{
  "checked_at": "$NOW_ISO",
  "date": "$TODAY",
  "overall": "$overall",
  "tracebacks": $traceback_count,
  "stale_outputs": $stale_count,
  "today_db_rows": $today_rows,
  "last_forecast_commit": "$last_forecast_commit",
  "db_path": "$DB"
}
JSON

# PIPELINE_STATUS.md — human readable, lives at repo root
{
    echo "# Pipeline Status"
    echo
    echo "Checked: \`$NOW_ISO\`"
    echo "Date: \`$TODAY\`"
    echo "Overall: **$overall**"
    echo
    echo "## Checks"
    echo "- Tracebacks in today's cron logs: **$traceback_count**"
    echo "- Stale or missing output files: **$stale_count**"
    echo "- Rows in \`snowfall_daily\` for $TODAY: **$today_rows** (db: \`$DB\`)"
    echo "- Last forecast commit: \`$last_forecast_commit\`"
    if [ "$traceback_count" -gt 0 ]; then
        echo
        echo "### Tracebacks"
        printf "%b" "$traceback_files"
    fi
    if [ "$stale_count" -gt 0 ]; then
        echo
        echo "### Stale outputs"
        printf "%b" "$stale_files"
    fi
} > PIPELINE_STATUS.md

echo "pipeline health: $overall (tracebacks=$traceback_count, stale=$stale_count, today_rows=$today_rows)"
