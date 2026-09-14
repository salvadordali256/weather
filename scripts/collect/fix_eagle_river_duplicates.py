#!/usr/bin/env python3
"""
Fix eagle_river_wi's duplicate rows: the same logical date exists twice,
once as 'YYYY-MM-DD' and once as 'YYYY-MM-DD HH:MM:SS' -- a date-format
bug, not a real second measurement. Confirmed via diagnose_data_bugs.py:
31,367 of 31,368 duplicate-date pairs have identical snowfall_mm between
the two formats; only 1 disagrees.

Deletes the 19-char-format row wherever a 10-char-format row already
exists for the same date (10-char is the convention every other station
and every query in this codebase uses). Never touches a 19-char row that
has no 10-char counterpart -- that's real data, not a duplicate, and
stays untouched either way.

The one disagreeing date is reported explicitly and left untouched by
default (--force-disagreeing to include it anyway) -- a single
discrepancy out of 31,368 doesn't get resolved by an automatic script
picking a side silently.

SAFETY: dry-run by default. Nothing is deleted unless --execute is
passed. Back up the database before running with --execute:
    cp global_snowfall.db global_snowfall.db.bak-$(date +%Y%m%d)

Usage:
    python scripts/collect/fix_eagle_river_duplicates.py                # dry run
    python scripts/collect/fix_eagle_river_duplicates.py --execute      # actually delete
"""

import argparse

from sqlalchemy import text

from snowforecast.storage.db import get_engine

STATION_ID = "eagle_river_wi"


def find_disagreeing_dates(conn):
    rows = conn.execute(
        text(
            """
            SELECT SUBSTR(date, 1, 10) as norm_date
            FROM snowfall_daily WHERE station_id = :sid
            GROUP BY norm_date
            HAVING COUNT(*) > 1 AND COUNT(DISTINCT snowfall_mm) > 1
            """
        ),
        {"sid": STATION_ID},
    ).fetchall()
    return [r.norm_date for r in rows]


def count_safe_to_delete(conn, exclude_dates):
    exclude_clause = ""
    params = {"sid": STATION_ID}
    if exclude_dates:
        placeholders = ", ".join(f":ex{i}" for i in range(len(exclude_dates)))
        exclude_clause = f"AND SUBSTR(date, 1, 10) NOT IN ({placeholders})"
        for i, d in enumerate(exclude_dates):
            params[f"ex{i}"] = d

    query = text(
        f"""
        SELECT COUNT(*) as n FROM snowfall_daily
        WHERE station_id = :sid
          AND LENGTH(date) = 19
          AND SUBSTR(date, 1, 10) IN (
              SELECT SUBSTR(date, 1, 10) FROM snowfall_daily
              WHERE station_id = :sid AND LENGTH(date) = 10
          )
          {exclude_clause}
        """
    )
    return conn.execute(query, params).fetchone().n


def delete_duplicates(conn, exclude_dates):
    exclude_clause = ""
    params = {"sid": STATION_ID}
    if exclude_dates:
        placeholders = ", ".join(f":ex{i}" for i in range(len(exclude_dates)))
        exclude_clause = f"AND SUBSTR(date, 1, 10) NOT IN ({placeholders})"
        for i, d in enumerate(exclude_dates):
            params[f"ex{i}"] = d

    query = text(
        f"""
        DELETE FROM snowfall_daily
        WHERE station_id = :sid
          AND LENGTH(date) = 19
          AND SUBSTR(date, 1, 10) IN (
              SELECT SUBSTR(date, 1, 10) FROM snowfall_daily
              WHERE station_id = :sid AND LENGTH(date) = 10
          )
          {exclude_clause}
        """
    )
    result = conn.execute(query, params)
    return result.rowcount


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--execute", action="store_true", help="Actually delete rows (default: dry run, no changes)")
    parser.add_argument("--force-disagreeing", action="store_true", help="Also delete the disagreeing date's duplicate (picks the 10-char row's value)")
    args = parser.parse_args()

    engine = get_engine()

    with engine.connect() as conn:
        disagreeing = find_disagreeing_dates(conn)
        print(f"Disagreeing duplicate dates found: {len(disagreeing)}")
        for d in disagreeing:
            rows = conn.execute(
                text("SELECT date, snowfall_mm FROM snowfall_daily WHERE station_id = :sid AND SUBSTR(date,1,10) = :d ORDER BY date"),
                {"sid": STATION_ID, "d": d},
            ).fetchall()
            print(f"  {d}: {[(r.date, r.snowfall_mm) for r in rows]}")

        exclude = [] if args.force_disagreeing else disagreeing
        if exclude:
            print(f"\nExcluding {len(exclude)} disagreeing date(s) from deletion (use --force-disagreeing to include).")

        to_delete = count_safe_to_delete(conn, exclude)
        print(f"\nRows that would be deleted: {to_delete}")

        if not args.execute:
            print("\nDRY RUN -- nothing deleted. Re-run with --execute to actually apply this.")
            return

        deleted = delete_duplicates(conn, exclude)
        conn.commit()
        print(f"\nDeleted {deleted} duplicate rows.")

        remaining = conn.execute(
            text("SELECT COUNT(*) as n FROM snowfall_daily WHERE station_id = :sid"), {"sid": STATION_ID}
        ).fetchone()
        print(f"eagle_river_wi row count after cleanup: {remaining.n}")


if __name__ == "__main__":
    main()
