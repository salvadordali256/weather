#!/usr/bin/env python3
"""
Find a historical major snow event in Wisconsin to demonstrate
how the global system would have predicted it
"""

import sqlite3
import pandas as pd

conn = sqlite3.connect('demo_global_snowfall.db')

# Find major snow events in Wisconsin (Eagle River)
query = """
SELECT date, snowfall_mm
FROM snowfall_daily
WHERE station_id = 'eagle_river_wi'
  AND snowfall_mm > 30.0
  AND date >= '2020-01-01'
  AND date <= '2025-12-31'
ORDER BY snowfall_mm DESC
LIMIT 10
"""

events = pd.read_sql_query(query, conn)
print("TOP 10 MAJOR SNOW EVENTS (Wisconsin):")
print("="*50)
for _, row in events.iterrows():
    print(f"{row['date']:12s} {row['snowfall_mm']:6.1f}mm")

# Pick one major event
if not events.empty:
    major_date = events.iloc[0]['date']
    major_snow = events.iloc[0]['snowfall_mm']
    
    print(f"\n\nANALYZING MAJOR EVENT: {major_date} ({major_snow:.1f}mm)")
    print("="*50)
    
    # Check global predictors around this date
    from datetime import datetime, timedelta
    
    event_dt = datetime.strptime(major_date, '%Y-%m-%d')
    
    # Check Thunder Bay (same day)
    tb_query = f"""
    SELECT snowfall_mm FROM snowfall_daily
    WHERE station_id = 'thunder_bay_on'
      AND date = '{major_date}'
    """
    tb = pd.read_sql_query(tb_query, conn)
    
    # Check Sapporo (6 days before)
    sapporo_date = (event_dt - timedelta(days=6)).strftime('%Y-%m-%d')
    sapporo_query = f"""
    SELECT snowfall_mm FROM snowfall_daily
    WHERE station_id = 'sapporo_japan'
      AND date = '{sapporo_date}'
    """
    sapporo = pd.read_sql_query(sapporo_query, conn)
    
    # Check Irkutsk (7 days before)
    irkutsk_date = (event_dt - timedelta(days=7)).strftime('%Y-%m-%d')
    irkutsk_query = f"""
    SELECT snowfall_mm FROM snowfall_daily
    WHERE station_id = 'irkutsk_russia'
      AND date = '{irkutsk_date}'
    """
    irkutsk = pd.read_sql_query(irkutsk_query, conn)
    
    print(f"\nGLOBAL PREDICTOR STATUS (Would Have Shown):")
    print("-"*50)
    
    if not tb.empty:
        tb_snow = tb.iloc[0]['snowfall_mm']
        print(f"Thunder Bay (same day {major_date}): {tb_snow:.1f}mm")
    
    if not sapporo.empty:
        sapporo_snow = sapporo.iloc[0]['snowfall_mm']
        print(f"Sapporo 6 days before ({sapporo_date}): {sapporo_snow:.1f}mm")
    
    if not irkutsk.empty:
        irkutsk_snow = irkutsk.iloc[0]['snowfall_mm']
        print(f"Irkutsk 7 days before ({irkutsk_date}): {irkutsk_snow:.1f}mm")

conn.close()
