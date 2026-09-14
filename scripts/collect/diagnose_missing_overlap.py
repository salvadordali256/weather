#!/usr/bin/env python3
"""
One-off diagnostic: why did iron_mountain_mi and granite_peak_wi show zero
overlapping dates in validate_against_noaa_external.py? Checks both sides
independently -- does NOAA return data for this station/window at all, and
does our own DB have ANY rows (any data_source) for it in this window.
"""

from sqlalchemy import text

from snowforecast.storage.db import get_engine
from validate_against_noaa_external import fetch_noaa_snow

STATIONS = [
    ("iron_mountain_mi", "USW00094892"),
    ("granite_peak_wi", "USC00478905"),
]

START, END = "2023-11-01", "2024-04-30"


def main():
    engine = get_engine()
    for station_id, ghcnd_id in STATIONS:
        noaa = fetch_noaa_snow(ghcnd_id, START, END)
        print(f"{station_id}: NOAA returned {len(noaa)} days")

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT data_source, COUNT(*) as n
                    FROM snowfall_daily
                    WHERE station_id = :sid AND date >= :start AND date <= :end
                    GROUP BY data_source
                    """
                ),
                {"sid": station_id, "start": START, "end": END},
            ).fetchall()
        print(f"{station_id}: our DB rows in this window by source: {list(rows)}")
        print()


if __name__ == "__main__":
    main()
