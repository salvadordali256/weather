#!/usr/bin/env python3
"""
Retroactive Forecast Demo
Shows how the system would have predicted a major historical snow event
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def get_snow_on_date(station_id, date_str):
    """Get snowfall for a station on a specific date"""
    conn = sqlite3.connect('demo_global_snowfall.db')

    query = """
    SELECT snowfall_mm FROM snowfall_daily
    WHERE station_id = ?
      AND date LIKE ?
    """

    df = pd.read_sql_query(query, conn, params=(station_id, f"{date_str}%"))
    conn.close()

    if df.empty:
        return None
    return df.iloc[0]['snowfall_mm']

# Major event: December 15, 2022 - 157.5mm!
EVENT_DATE = "2022-12-15"
EVENT_SNOW = 157.5

print(f"\n{'='*80}")
print(f"RETROACTIVE FORECAST DEMONSTRATION")
print(f"{'='*80}")
print(f"Historical Major Snow Event: {EVENT_DATE}")
print(f"Actual Snowfall (Eagle River, WI): {EVENT_SNOW}mm (6.2 inches)")
print(f"Event Type: MAJOR WINTER STORM")
print(f"{'='*80}\n")

print(f"QUESTION: Would our global system have predicted this event?")
print(f"{'─'*80}\n")

# Parse date
event_dt = datetime.strptime(EVENT_DATE, '%Y-%m-%d')

# Check global predictors with their lag patterns
predictors = [
    {'name': 'Thunder Bay, ON', 'station': 'thunder_bay_on', 'lag': 0, 'weight': 0.40},
    {'name': 'Sapporo, Japan', 'station': 'sapporo_japan', 'lag': 6, 'weight': 0.10},
    {'name': 'Niigata, Japan', 'station': 'niigata_japan', 'lag': 3, 'weight': 0.05},
    {'name': 'Irkutsk, Russia', 'station': 'irkutsk_russia', 'lag': 7, 'weight': 0.04},
    {'name': 'Chamonix, France', 'station': 'chamonix_france', 'lag': 5, 'weight': 0.08},
]

print(f"GLOBAL PREDICTOR CHECK:")
print(f"{'─'*80}\n")

ensemble_score = 0.0
active_signals = []

for pred in predictors:
    # Calculate the date to check based on lag
    check_date = event_dt - timedelta(days=pred['lag'])
    check_date_str = check_date.strftime('%Y-%m-%d')

    snow = get_snow_on_date(pred['station'], check_date_str)

    if snow is not None and snow > 0:
        if snow >= 25.0:
            status = "HEAVY"
            activity = 1.0
            icon = "🔴"
        elif snow >= 15.0:
            status = "MODERATE"
            activity = 0.6
            icon = "🟡"
        elif snow >= 5.0:
            status = "LIGHT"
            activity = 0.3
            icon = "🟢"
        else:
            status = "Trace"
            activity = 0.0
            icon = "⚪"

        contribution = pred['weight'] * activity
        ensemble_score += contribution

        if activity > 0:
            active_signals.append({
                'name': pred['name'],
                'snow': snow,
                'date': check_date_str,
                'lag': pred['lag'],
                'weight': pred['weight'],
                'contribution': contribution
            })

        lag_text = f"{pred['lag']}d before" if pred['lag'] > 0 else "SAME DAY"
        print(f"{icon} {pred['name']:25s} on {check_date_str}: {snow:6.1f}mm ({status}) | {lag_text:12s} | +{contribution:.1%}")
    else:
        print(f"⚪ {pred['name']:25s} on {check_date_str}: NO DATA")

print(f"\n{'─'*80}")
print(f"RETROACTIVE ENSEMBLE ANALYSIS:")
print(f"{'─'*80}\n")

print(f"Total Ensemble Score: {ensemble_score:.1%}")
print(f"Active Signals: {len(active_signals)}/{len(predictors)}")

if active_signals:
    print(f"\nACTIVE SIGNALS BREAKDOWN:")
    for sig in sorted(active_signals, key=lambda x: x['contribution'], reverse=True):
        lag_info = f"{sig['lag']}d lead" if sig['lag'] > 0 else "same-day"
        print(f"  • {sig['name']:25s} {sig['snow']:6.1f}mm on {sig['date']} ({lag_info:12s}) → +{sig['contribution']:.1%}")

print(f"\n{'='*80}")
print(f"RETROACTIVE FORECAST (What System Would Have Said):")
print(f"{'='*80}\n")

if ensemble_score >= 0.70:
    verdict = "🔴 HIGH ALERT - Major snow event VERY LIKELY"
    confidence = "VERY HIGH (85-95%)"
elif ensemble_score >= 0.50:
    verdict = "🟡 WATCH - Snow event PROBABLE"
    confidence = "HIGH (65-85%)"
elif ensemble_score >= 0.30:
    verdict = "🟢 ADVISORY - Snow POSSIBLE"
    confidence = "MODERATE (40-65%)"
else:
    verdict = "⚪ QUIET - Low probability"
    confidence = "LOW (<40%)"

print(f"Forecast Verdict: {verdict}")
print(f"Confidence Level: {confidence}")
print(f"Ensemble Score: {ensemble_score:.1%}")

print(f"\n{'─'*80}")
print(f"VERIFICATION:")
print(f"{'─'*80}\n")

print(f"FORECASTED: {verdict}")
print(f"ACTUAL: {EVENT_SNOW}mm major winter storm ✓")

if ensemble_score >= 0.50:
    print(f"\n✅ SUCCESS: System would have CORRECTLY predicted this major event!")
    print(f"   High ensemble score ({ensemble_score:.1%}) correctly signaled major snowfall.")
elif ensemble_score >= 0.30:
    print(f"\n⚠️  PARTIAL: System showed moderate signal ({ensemble_score:.1%})")
    print(f"   Would have given elevated probability but not high alert.")
else:
    print(f"\n❌ MISS: Low ensemble score ({ensemble_score:.1%}) - system would have missed this event")
    print(f"   Suggests predictors may not have been active or data gaps exist.")

print(f"\n{'='*80}\n")
