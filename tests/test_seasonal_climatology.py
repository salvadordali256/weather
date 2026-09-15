"""
Tests for the climatological seasonal outlook (snowforecast.analysis.seasonal_climatology).

Backend-neutral -- runs on a temp SQLite DB via DATABASE_URL, same pattern as
test_storage_layer.py, so it works in CI with no Postgres.
"""

import os
import tempfile
from pathlib import Path

import pytest

from snowforecast.analysis.seasonal_climatology import (
    classify_value,
    compute_seasonal_outlook,
)
from snowforecast.storage import db as dbmod
from snowforecast.storage.models import Base, SnowfallDaily


@pytest.fixture
def seeded_db():
    """Temp SQLite DB with multi-year February data for two stations."""
    tmp_dir = tempfile.mkdtemp()
    db_path = Path(tmp_dir) / "test_climatology.db"
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    dbmod.reset_engine()

    engine = dbmod.get_engine()
    Base.metadata.create_all(engine)

    # 10 years of February data, two stations, deliberately varied totals
    # so mean/median/tercile math has something real to compute.
    yearly_feb_totals = {
        2015: 100.0, 2016: 120.0, 2017: 80.0, 2018: 200.0, 2019: 60.0,
        2020: 150.0, 2021: 90.0, 2022: 300.0, 2023: 40.0, 2024: 110.0,
    }
    with dbmod.session_scope() as session:
        for year, total in yearly_feb_totals.items():
            for station in ("phelps_wi", "land_o_lakes_wi"):
                # Spread the monthly total across a few days for realism.
                session.add(SnowfallDaily(station_id=station, date=f"{year}-02-01", snowfall_mm=total / 2))
                session.add(SnowfallDaily(station_id=station, date=f"{year}-02-15", snowfall_mm=total / 2))
        # Noise from a different month/station that must NOT leak into the February result.
        session.add(SnowfallDaily(station_id="phelps_wi", date="2020-03-01", snowfall_mm=9999.0))
        session.add(SnowfallDaily(station_id="duluth_mn", date="2020-02-01", snowfall_mm=9999.0))

    yield yearly_feb_totals

    dbmod.reset_engine()
    os.environ.pop("DATABASE_URL", None)


def test_compute_seasonal_outlook_matches_seeded_totals(seeded_db):
    outlook = compute_seasonal_outlook(2, station_ids=("phelps_wi", "land_o_lakes_wi"))

    assert outlook.years_used == sorted(seeded_db.keys())
    # Both seeded stations have identical totals each year, so the
    # cross-station average should equal the seeded per-station total exactly.
    for year, expected_total in seeded_db.items():
        assert outlook.yearly_totals_mm[year] == pytest.approx(expected_total)

    assert outlook.mean_mm == pytest.approx(sum(seeded_db.values()) / len(seeded_db))
    assert outlook.min_mm == pytest.approx(40.0)
    assert outlook.max_mm == pytest.approx(300.0)


def test_excludes_other_months_and_stations(seeded_db):
    outlook = compute_seasonal_outlook(2, station_ids=("phelps_wi", "land_o_lakes_wi"))
    # The 9999mm March/duluth noise rows must not appear anywhere.
    assert outlook.max_mm < 9999.0
    assert "duluth_mn" not in outlook.station_ids


def test_no_data_raises(seeded_db):
    # Schema exists (via the fixture) but this station has zero rows --
    # should raise ValueError, not silently return a bogus empty result.
    with pytest.raises(ValueError):
        compute_seasonal_outlook(2, station_ids=("nonexistent_station",))


def test_classify_value(seeded_db):
    outlook = compute_seasonal_outlook(2, station_ids=("phelps_wi", "land_o_lakes_wi"))
    lower, upper = outlook.tercile_bounds_mm

    assert classify_value(lower - 1, outlook) == "below normal"
    assert classify_value(upper + 1, outlook) == "above normal"
    assert classify_value((lower + upper) / 2, outlook) == "near normal"


def test_invalid_month_raises():
    with pytest.raises(ValueError):
        compute_seasonal_outlook(13)
    with pytest.raises(ValueError):
        compute_seasonal_outlook(0)
