import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def test_event(event_date, event_snow):
    """Test retroactive forecast for an event"""
    conn = sqlite3.connect('demo_global_snowfall.db')
    
    event_dt = datetime.strptime(event_date.split()[0], '%Y-%m-%d')
    
    ensemble_score = 0.0
    
    # Thunder Bay (same day)
    tb = pd.read_sql_query(f"""
        SELECT snowfall_mm FROM snowfall_daily
        WHERE station_id = 'thunder_bay_on' AND date LIKE '{event_date.split()[0]}%'
    """, conn)
    if not tb.empty and tb.iloc[0]['snowfall_mm'] >= 20:
        ensemble_score += 0.40
    elif not tb.empty and tb.iloc[0]['snowfall_mm'] >= 10:
        ensemble_score += 0.24
    
    # Sapporo (6 days before)
    sapporo_date = (event_dt - timedelta(days=6)).strftime('%Y-%m-%d')
    sp = pd.read_sql_query(f"""
        SELECT snowfall_mm FROM snowfall_daily
        WHERE station_id = 'sapporo_japan' AND date LIKE '{sapporo_date}%'
    """, conn)
    if not sp.empty and sp.iloc[0]['snowfall_mm'] >= 25:
        ensemble_score += 0.10
    elif not sp.empty and sp.iloc[0]['snowfall_mm'] >= 15:
        ensemble_score += 0.06
    
    conn.close()
    return ensemble_score

# Test top events
events = [
    ("2022-12-15", 157.5),
    ("2025-03-05", 156.1),
    ("2023-04-01", 147.7),
    ("2022-03-23", 118.3),
    ("2023-01-19", 107.1),
]

print("\nTESTING RETROACTIVE FORECASTS FOR TOP 5 MAJOR EVENTS:")
print("="*70)
for date, snow in events:
    score = test_event(date, snow)
    if score >= 0.50:
        result = "✅ SUCCESS (would have predicted)"
    elif score >= 0.30:
        result = "⚠️  PARTIAL (moderate signal)"
    else:
        result = "❌ MISS (weak signal)"
    print(f"{date}: {snow:6.1f}mm | Ensemble: {score:.1%} | {result}")

print("="*70)
