#!/usr/bin/env python3
"""
Data inventory report: how many years of snowfall_daily history exist,
per station, and how many stations clear a usable threshold for
climatology (see analysis/seasonal_climatology.py, which recommends
>= 10 years for a stable tercile split).

This does not filter to "ski areas only" -- there's no such flag in the
schema (Station.significance is a free-text teleconnection note, not a
resort/non-resort tag). Station names are printed so you can judge by eye
which rows are actual ski resorts vs. generic reference/NOAA stations.

Usage:
    python scripts/collect/data_inventory_report.py
    python scripts/collect/data_inventory_report.py --min-years 10
"""

import argparse

from sqlalchemy import text

from snowforecast.storage.db import add_db_path_arg, describe_engine, get_engine


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--min-years", type=int, default=10, help="Threshold to flag as 'climatology-viable'")
    add_db_path_arg(parser)
    args = parser.parse_args()

    engine = get_engine(args.db_path)
    print(f"Target database: {describe_engine(engine)}")

    query = text(
        """
        SELECT
            sd.station_id,
            s.name,
            s.region,
            MIN(sd.date) as first_date,
            MAX(sd.date) as last_date,
            COUNT(*) as row_count,
            COUNT(DISTINCT substr(sd.date, 1, 4)) as distinct_years
        FROM snowfall_daily sd
        LEFT JOIN stations s ON sd.station_id = s.station_id
        GROUP BY sd.station_id
        ORDER BY distinct_years DESC, row_count DESC
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    if not rows:
        print("No stations found in snowfall_daily.")
        return

    viable = [r for r in rows if r.distinct_years >= args.min_years]

    print("=" * 100)
    print(f"DATA INVENTORY: {len(rows)} stations with snowfall_daily history")
    print(f"  {len(viable)} stations have >= {args.min_years} distinct years (climatology-viable threshold)")
    print("=" * 100)
    print()
    print(f"{'station_id':<25} {'name':<30} {'years':>6} {'rows':>8} {'first_date':<12} {'last_date':<12}")
    print("-" * 100)
    for r in rows:
        flag = "*" if r.distinct_years >= args.min_years else " "
        name = (r.name or "?")[:30]
        print(f"{flag}{r.station_id:<24} {name:<30} {r.distinct_years:>6} {r.row_count:>8} {r.first_date:<12} {r.last_date:<12}")

    print()
    print(f"* = >= {args.min_years} years of data (usable for a stable climatology outlook)")


if __name__ == "__main__":
    main()
