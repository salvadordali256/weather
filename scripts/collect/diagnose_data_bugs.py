#!/usr/bin/env python3
"""
Diagnose the exact shape of two known data bugs before writing any fix:

1. eagle_river_wi has ~2x the row count of peer 87-year stations (63,034
   vs ~31,665) and its first_date showed a full timestamp format
   ('1940-01-01 00:00:00') instead of the plain 'YYYY-MM-DD' every other
   station uses -- suggests either genuine duplicate inserts, or a mixed
   date-format bug where the same logical date exists as two different
   string values (so GROUP BY / exact-match queries silently double-count
   or miss rows). Since eagle_river_wi has no NOAA mapping at all (100%
   open-meteo), this isn't explained by legitimate dual-sourcing.

2. land_o_lakes_wi (87yr, 31,665 rows) and land_o'lakes_wi (32yr, 11,328
   rows) are almost certainly the same physical station split by an
   apostrophe inconsistency. Before merging them, need to know whether
   their date ranges actually overlap (if so, a straight rename would hit
   primary-key conflicts on station_id+date and needs a merge policy).

Read-only. Does not modify anything.
"""

from sqlalchemy import text

from snowforecast.storage.db import get_engine


def diagnose_eagle_river(engine):
    print("=" * 90)
    print("EAGLE_RIVER_WI DUPLICATE INVESTIGATION")
    print("=" * 90)

    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) as n FROM snowfall_daily WHERE station_id = 'eagle_river_wi'")
        ).fetchone()
        print(f"Total rows: {total.n}")

        by_source = conn.execute(
            text(
                """
                SELECT data_source, COUNT(*) as n
                FROM snowfall_daily WHERE station_id = 'eagle_river_wi'
                GROUP BY data_source
                """
            )
        ).fetchall()
        print(f"By data_source: {list(by_source)}")

        # Date string length distribution -- reveals mixed formats
        # (10 chars = 'YYYY-MM-DD', 19 chars = 'YYYY-MM-DD HH:MM:SS')
        by_len = conn.execute(
            text(
                """
                SELECT LENGTH(date) as date_len, COUNT(*) as n
                FROM snowfall_daily WHERE station_id = 'eagle_river_wi'
                GROUP BY date_len
                """
            )
        ).fetchall()
        print(f"Date string length distribution: {list(by_len)}")

        # Exact duplicate (same station_id, same raw date string) count
        exact_dupes = conn.execute(
            text(
                """
                SELECT COUNT(*) as n FROM (
                    SELECT date, COUNT(*) as c
                    FROM snowfall_daily WHERE station_id = 'eagle_river_wi'
                    GROUP BY date HAVING c > 1
                )
                """
            )
        ).fetchone()
        print(f"Dates with exact-duplicate rows (same raw date string): {exact_dupes.n}")

        # Normalized (first 10 chars) duplicate count -- catches the
        # mixed-format case where '1940-01-01' and '1940-01-01 00:00:00'
        # both exist as separate rows for the same real date
        normalized_dupes = conn.execute(
            text(
                """
                SELECT COUNT(*) as n FROM (
                    SELECT SUBSTR(date, 1, 10) as norm_date, COUNT(*) as c
                    FROM snowfall_daily WHERE station_id = 'eagle_river_wi'
                    GROUP BY norm_date HAVING c > 1
                )
                """
            )
        ).fetchone()
        print(f"Dates with duplicate rows after normalizing to first 10 chars: {normalized_dupes.n}")

        # A few example rows to see the actual duplicate pattern
        sample = conn.execute(
            text(
                """
                SELECT date, snowfall_mm, data_source FROM snowfall_daily
                WHERE station_id = 'eagle_river_wi'
                  AND SUBSTR(date, 1, 10) IN (
                      SELECT SUBSTR(date, 1, 10) FROM snowfall_daily
                      WHERE station_id = 'eagle_river_wi'
                      GROUP BY SUBSTR(date, 1, 10) HAVING COUNT(*) > 1
                      LIMIT 3
                  )
                ORDER BY date
                LIMIT 20
                """
            )
        ).fetchall()
        print("\nSample duplicate rows:")
        for r in sample:
            print(f"  date={r.date!r} snowfall_mm={r.snowfall_mm} data_source={r.data_source!r}")


def diagnose_land_o_lakes(engine):
    print()
    print("=" * 90)
    print("LAND_O_LAKES_WI / LAND_O'LAKES_WI OVERLAP INVESTIGATION")
    print("=" * 90)

    with engine.connect() as conn:
        for sid in ("land_o_lakes_wi", "land_o'lakes_wi"):
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(*) as n, MIN(date) as first, MAX(date) as last
                    FROM snowfall_daily WHERE station_id = :sid
                    """
                ),
                {"sid": sid},
            ).fetchone()
            print(f"{sid!r}: {row.n} rows, {row.first} to {row.last}")

        overlap = conn.execute(
            text(
                """
                SELECT COUNT(*) as n FROM (
                    SELECT date FROM snowfall_daily WHERE station_id = 'land_o_lakes_wi'
                    INTERSECT
                    SELECT date FROM snowfall_daily WHERE station_id = 'land_o''lakes_wi'
                )
                """
            )
        ).fetchone()
        print(f"\nDates present in BOTH variants (would conflict on a straight rename): {overlap.n}")

        # Also check the 'stations' metadata table for both ids
        stations = conn.execute(
            text(
                "SELECT station_id, name, latitude, longitude FROM stations WHERE station_id IN ('land_o_lakes_wi', 'land_o''lakes_wi')"
            )
        ).fetchall()
        print(f"\n'stations' table entries: {list(stations)}")


def main():
    engine = get_engine()
    diagnose_eagle_river(engine)
    diagnose_land_o_lakes(engine)


if __name__ == "__main__":
    main()
