"""
Regression test for the off-season data-quality guard.

The pipeline aborts publishing when an engine forecast reports
data_quality='no_data' for every day (a guard meant to catch a missing/empty
DB). The bug: data_quality was keyed off snow *signals*, so a fully-populated
database reporting zero snow (the entire off-season) looked identical to an
empty DB and blocked publishing all summer.

These tests assert the engine now keys data_quality off DB rows returned, not
snow activity:
  - a populated-but-snowless DB  -> data_quality == 'ok'   (publishable)
  - an empty DB                  -> data_quality == 'no_data' (guard holds)
"""

import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from snowforecast.engines.enhanced_regional_forecast_system import (
    EnhancedRegionalForecastSystem,
)

EVENT_DATE = datetime(2026, 7, 2)  # mid-summer: every station has zero snow


def _make_db(path: str, *, seed_rows: bool) -> None:
    """Create a snowfall_daily table; optionally seed zero-snow rows for every
    predictor station across the lag window the engine inspects."""
    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.execute(
            """
            CREATE TABLE snowfall_daily (
                station_id   TEXT,
                date         TEXT,
                snowfall_mm  REAL
            )
            """
        )
        if seed_rows:
            engine = EnhancedRegionalForecastSystem(path)
            stations = list(engine.global_predictors) + list(engine.regional_predictors)
            # Cover event_date minus the largest lag (7), with +/-1 day windows.
            rows = []
            for station_id in stations:
                for offset in range(-10, 2):
                    d = (EVENT_DATE + timedelta(days=offset)).strftime("%Y-%m-%d")
                    rows.append((station_id, d, 0.0))  # zero snow, but real rows
            conn.executemany(
                "INSERT INTO snowfall_daily VALUES (?, ?, ?)", rows
            )
        conn.commit()
    finally:
        conn.close()


def test_snowless_but_populated_db_is_ok():
    """Zero snow everywhere must still count as data -> publishable."""
    with tempfile.TemporaryDirectory() as tmp:
        db = str(Path(tmp) / "snow.db")
        _make_db(db, seed_rows=True)

        engine = EnhancedRegionalForecastSystem(db)
        forecast = engine.generate_ensemble_forecast(EVENT_DATE)

        assert forecast["data_quality"] == "ok"
        # And the predictor checks report rows returned, not snow signals.
        assert engine.check_global_predictors(EVENT_DATE)["stations_with_data"] > 0
        assert engine.check_regional_predictors(EVENT_DATE)["stations_with_data"] > 0


def test_empty_db_is_no_data():
    """A populated table with no rows must still trip the empty-DB guard."""
    with tempfile.TemporaryDirectory() as tmp:
        db = str(Path(tmp) / "snow.db")
        _make_db(db, seed_rows=False)

        engine = EnhancedRegionalForecastSystem(db)
        forecast = engine.generate_ensemble_forecast(EVENT_DATE)

        assert forecast["data_quality"] == "no_data"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
