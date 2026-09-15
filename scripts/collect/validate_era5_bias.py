#!/usr/bin/env python3
"""
Validate whether Open-Meteo/ERA5 snowfall data is systematically biased,
using dual-sourced rows already in this DB rather than hand-typed
comparison numbers.

snowfall_daily.data_source distinguishes 'noaa' (real station observations)
from 'open-meteo' (ERA5 reanalysis -- a ~31km-grid model estimate, not a
direct measurement; see migrate_data_source.py). Where a station has rows
from BOTH sources on the SAME date, that's a genuine same-day, same-location
comparison: NOAA is ground truth, Open-Meteo is the model estimate this
system's forecasts and any climatology are built on.

This does NOT call any external API or use remembered/hand-typed figures --
every number here comes from a query against this DB.

Usage:
    python scripts/collect/validate_era5_bias.py
    python scripts/collect/validate_era5_bias.py --station eagle_river_wi
"""

import argparse

from sqlalchemy import text

from snowforecast.storage.db import add_db_path_arg, describe_engine, get_engine


def report_source_breakdown(engine):
    """Per-station row counts by data_source -- reveals which stations even
    have dual sourcing worth comparing, and flags any station whose total
    row count looks inflated by duplicate un-deduplicated sourcing."""
    query = text(
        """
        SELECT station_id, data_source, COUNT(*) as row_count
        FROM snowfall_daily
        GROUP BY station_id, data_source
        ORDER BY station_id, data_source
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    by_station = {}
    for r in rows:
        by_station.setdefault(r.station_id, {})[r.data_source or "(null)"] = r.row_count

    dual_sourced = {sid: sources for sid, sources in by_station.items() if len(sources) > 1}

    print("=" * 90)
    print(f"DATA SOURCE BREAKDOWN: {len(by_station)} stations total, {len(dual_sourced)} have >1 data_source")
    print("=" * 90)
    if not dual_sourced:
        print("No station has rows from more than one data_source.")
        print("Cannot do a same-day NOAA-vs-Open-Meteo comparison with what's in this DB --")
        print("that would require pulling real NOAA records separately for comparison.")
        return {}

    for sid, sources in sorted(dual_sourced.items()):
        print(f"  {sid:<25} {sources}")
    print()
    return dual_sourced


def compare_same_day(engine, station_id):
    """For a station with both noaa and open-meteo rows, compare values on
    dates where BOTH sources reported -- a direct same-day, same-location
    comparison."""
    query = text(
        """
        SELECT date, data_source, snowfall_mm
        FROM snowfall_daily
        WHERE station_id = :station_id
          AND data_source IN ('noaa', 'open-meteo')
        ORDER BY date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"station_id": station_id}).fetchall()

    by_date = {}
    for r in rows:
        by_date.setdefault(r.date, {})[r.data_source] = r.snowfall_mm

    overlap = {d: v for d, v in by_date.items() if "noaa" in v and "open-meteo" in v}

    print("-" * 90)
    print(f"{station_id}: {len(overlap)} dates with both noaa and open-meteo rows")
    print("-" * 90)

    if not overlap:
        print("No same-date overlap between sources for this station -- likely means the")
        print("two sources cover different, non-overlapping date ranges (e.g. one is a")
        print("historical backfill, the other current collection), not a direct duplicate.")
        return

    noaa_total = sum(v["noaa"] or 0.0 for v in overlap.values())
    om_total = sum(v["open-meteo"] or 0.0 for v in overlap.values())
    ratio = (noaa_total / om_total) if om_total else float("nan")

    print(f"  NOAA total over overlap:       {noaa_total:.1f} mm")
    print(f"  Open-Meteo total over overlap: {om_total:.1f} mm")
    print(f"  Ratio (NOAA / Open-Meteo):     {ratio:.2f}x")
    if ratio > 1.15:
        print(f"  -> Open-Meteo appears to UNDERSTATE actual snowfall by ~{(ratio - 1) * 100:.0f}% for this station")
    elif ratio < 0.87:
        print(f"  -> Open-Meteo appears to OVERSTATE actual snowfall by ~{(1 - ratio) * 100:.0f}% for this station")
    else:
        print("  -> Sources roughly agree (within ~15%) for this station")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--station", help="Check one specific station_id instead of the full dual-sourced list")
    add_db_path_arg(parser)
    args = parser.parse_args()

    engine = get_engine(args.db_path)
    print(f"Target database: {describe_engine(engine)}")

    if args.station:
        compare_same_day(engine, args.station)
        return

    dual_sourced = report_source_breakdown(engine)
    for sid in sorted(dual_sourced):
        compare_same_day(engine, sid)


if __name__ == "__main__":
    main()
