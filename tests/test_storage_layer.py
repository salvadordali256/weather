"""
Tests for the central storage layer (Phase 2/3 of the Postgres migration):
the DATABASE_URL-driven engine/session in ``snowforecast.storage.db`` and the
ORM models in ``snowforecast.storage.models``.

Backend-neutral — everything runs on in-memory / temp SQLite, so it works in
CI with no Postgres.
"""

import os
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect

from snowforecast.storage import db as dbmod
from snowforecast.storage.models import Base, Station, SnowfallDaily

EXPECTED_TABLES = {
    "atmospheric_daily",
    "backfill_progress",
    "hourly_observations",
    "noaa_collection_progress",
    "radiosonde_soundings",
    "radiosonde_stations",
    "resort_conditions",
    "snowfall_daily",
    "stations",
}


def test_models_build_full_schema():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    insp = inspect(eng)
    assert set(insp.get_table_names()) == EXPECTED_TABLES


def test_snowfall_daily_shape_matches_live():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    insp = inspect(eng)
    cols = [c["name"] for c in insp.get_columns("snowfall_daily")]
    assert len(cols) == 20
    assert insp.get_pk_constraint("snowfall_daily")["constrained_columns"] == [
        "station_id",
        "date",
    ]


def test_atmospheric_index_and_resort_unique():
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    insp = inspect(eng)
    idx = [i["name"] for i in insp.get_indexes("atmospheric_daily")]
    assert "idx_atmospheric_station_date" in idx
    uniq = [u["column_names"] for u in insp.get_unique_constraints("resort_conditions")]
    assert ["collected_at", "resort_id"] in uniq


def test_database_url_prefers_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://u:p@host/db")
    assert dbmod.database_url() == "postgresql+psycopg2://u:p@host/db"


def test_database_url_falls_back_to_sqlite(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_PATH", "/tmp/example.db")
    assert dbmod.database_url() == "sqlite:////tmp/example.db"


def test_session_scope_roundtrip(monkeypatch):
    """The engine/session honours DATABASE_URL and commits real rows."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "s.db"
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("DB_PATH", str(db_path))
        dbmod.reset_engine()
        Base.metadata.create_all(dbmod.get_engine())

        with dbmod.session_scope() as s:
            s.add(Station(station_id="phelps_wi", name="Phelps, WI"))
            s.add(SnowfallDaily(station_id="phelps_wi", date="2026-06-30", snowfall_mm=0.0))

        with dbmod.session_scope() as s:
            st = s.get(Station, "phelps_wi")
            assert st is not None and st.name == "Phelps, WI"
            row = s.get(SnowfallDaily, ("phelps_wi", "2026-06-30"))
            assert row is not None and row.snowfall_mm == 0.0
            # server_default applied
            assert row.data_source == "open-meteo"

        dbmod.reset_engine()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
