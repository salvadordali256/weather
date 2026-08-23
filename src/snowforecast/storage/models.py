"""
SQLAlchemy ORM models for the snowforecast datastore.

These mirror the live SQLite schema exactly (dumped 2026-06-30) so the same
models drive both the SQLite dev/test backend and the production Postgres
database. Dates/datetimes are kept as TEXT (``str``) to match the millions of
existing rows that store ``'YYYY-MM-DD'`` strings; converting to native
DATE/TIMESTAMP types is a deliberate follow-up, not part of the cutover.

Backend-neutral types only (String/Integer/Float) so ``Base.metadata`` builds
the schema on either engine.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base; ``Base.metadata`` owns the whole schema."""


class Station(Base):
    __tablename__ = "stations"

    station_id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    region: Mapped[Optional[str]] = mapped_column(Text)
    significance: Mapped[Optional[str]] = mapped_column(Text)


class SnowfallDaily(Base):
    __tablename__ = "snowfall_daily"

    station_id: Mapped[str] = mapped_column(Text, primary_key=True)
    date: Mapped[str] = mapped_column(Text, primary_key=True)
    snowfall_mm: Mapped[Optional[float]] = mapped_column(Float)
    temp_mean_celsius: Mapped[Optional[float]] = mapped_column(Float)
    precipitation_mm: Mapped[Optional[float]] = mapped_column(Float)
    rain_mm: Mapped[Optional[float]] = mapped_column(Float)
    temp_max_celsius: Mapped[Optional[float]] = mapped_column(Float)
    temp_min_celsius: Mapped[Optional[float]] = mapped_column(Float)
    apparent_temp_max: Mapped[Optional[float]] = mapped_column(Float)
    apparent_temp_min: Mapped[Optional[float]] = mapped_column(Float)
    wind_speed_max: Mapped[Optional[float]] = mapped_column(Float)
    wind_gusts_max: Mapped[Optional[float]] = mapped_column(Float)
    wind_direction_dominant: Mapped[Optional[int]] = mapped_column(Integer)
    radiation_sum: Mapped[Optional[float]] = mapped_column(Float)
    sunshine_duration: Mapped[Optional[float]] = mapped_column(Float)
    precipitation_hours: Mapped[Optional[float]] = mapped_column(Float)
    weather_code: Mapped[Optional[int]] = mapped_column(Integer)
    evapotranspiration: Mapped[Optional[float]] = mapped_column(Float)
    snow_depth_mm: Mapped[Optional[float]] = mapped_column(Float)
    data_source: Mapped[Optional[str]] = mapped_column(
        Text, server_default=text("'open-meteo'"), default="open-meteo"
    )


class AtmosphericDaily(Base):
    __tablename__ = "atmospheric_daily"
    __table_args__ = (
        Index("idx_atmospheric_station_date", "station_id", "date"),
    )

    station_id: Mapped[str] = mapped_column(
        Text, ForeignKey("stations.station_id"), primary_key=True
    )
    date: Mapped[str] = mapped_column(Text, primary_key=True)
    temp_min_c: Mapped[Optional[float]] = mapped_column(Float)
    temp_max_c: Mapped[Optional[float]] = mapped_column(Float)
    temp_mean_c: Mapped[Optional[float]] = mapped_column(Float)
    pressure_msl_hpa: Mapped[Optional[float]] = mapped_column(Float)
    pressure_surface_hpa: Mapped[Optional[float]] = mapped_column(Float)
    wind_speed_max_kmh: Mapped[Optional[float]] = mapped_column(Float)
    wind_direction_dominant: Mapped[Optional[int]] = mapped_column(Integer)
    wind_gusts_max_kmh: Mapped[Optional[float]] = mapped_column(Float)
    relative_humidity_mean: Mapped[Optional[float]] = mapped_column(Float)
    dewpoint_mean_c: Mapped[Optional[float]] = mapped_column(Float)
    cloud_cover_mean: Mapped[Optional[float]] = mapped_column(Float)
    precipitation_sum_mm: Mapped[Optional[float]] = mapped_column(Float)
    rain_sum_mm: Mapped[Optional[float]] = mapped_column(Float)


class HourlyObservation(Base):
    __tablename__ = "hourly_observations"

    station_id: Mapped[str] = mapped_column(Text, primary_key=True)
    datetime: Mapped[str] = mapped_column(Text, primary_key=True)
    temperature_2m: Mapped[Optional[float]] = mapped_column(Float)
    relative_humidity_2m: Mapped[Optional[float]] = mapped_column(Float)
    dew_point_2m: Mapped[Optional[float]] = mapped_column(Float)
    apparent_temperature: Mapped[Optional[float]] = mapped_column(Float)
    pressure_msl: Mapped[Optional[float]] = mapped_column(Float)
    surface_pressure: Mapped[Optional[float]] = mapped_column(Float)
    precipitation: Mapped[Optional[float]] = mapped_column(Float)
    rain: Mapped[Optional[float]] = mapped_column(Float)
    snowfall: Mapped[Optional[float]] = mapped_column(Float)
    snow_depth: Mapped[Optional[float]] = mapped_column(Float)
    cloud_cover: Mapped[Optional[float]] = mapped_column(Float)
    cloud_cover_low: Mapped[Optional[float]] = mapped_column(Float)
    cloud_cover_mid: Mapped[Optional[float]] = mapped_column(Float)
    cloud_cover_high: Mapped[Optional[float]] = mapped_column(Float)
    wind_speed_10m: Mapped[Optional[float]] = mapped_column(Float)
    wind_direction_10m: Mapped[Optional[float]] = mapped_column(Float)
    wind_gusts_10m: Mapped[Optional[float]] = mapped_column(Float)
    visibility: Mapped[Optional[float]] = mapped_column(Float)
    weather_code: Mapped[Optional[int]] = mapped_column(Integer)
    cape: Mapped[Optional[float]] = mapped_column(Float)
    freezing_level_height: Mapped[Optional[float]] = mapped_column(Float)
    soil_temperature_0cm: Mapped[Optional[float]] = mapped_column(Float)
    soil_moisture_0_to_1cm: Mapped[Optional[float]] = mapped_column(Float)


class RadiosondeStation(Base):
    __tablename__ = "radiosonde_stations"

    station_wmo: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[Optional[str]] = mapped_column(Text)
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)


class RadiosondeSounding(Base):
    __tablename__ = "radiosonde_soundings"

    station_wmo: Mapped[str] = mapped_column(Text, primary_key=True)
    datetime: Mapped[str] = mapped_column(Text, primary_key=True)
    pressure_hpa: Mapped[float] = mapped_column(Float, primary_key=True)
    station_name: Mapped[Optional[str]] = mapped_column(Text)
    height_m: Mapped[Optional[float]] = mapped_column(Float)
    temperature_c: Mapped[Optional[float]] = mapped_column(Float)
    dewpoint_c: Mapped[Optional[float]] = mapped_column(Float)
    relative_humidity: Mapped[Optional[float]] = mapped_column(Float)
    mixing_ratio: Mapped[Optional[float]] = mapped_column(Float)
    wind_direction: Mapped[Optional[int]] = mapped_column(Integer)
    wind_speed_kts: Mapped[Optional[float]] = mapped_column(Float)
    theta_e: Mapped[Optional[float]] = mapped_column(Float)
    theta_v: Mapped[Optional[float]] = mapped_column(Float)


class ResortCondition(Base):
    __tablename__ = "resort_conditions"
    __table_args__ = (
        UniqueConstraint("collected_at", "resort_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    collected_at: Mapped[str] = mapped_column(Text, nullable=False)
    resort_id: Mapped[str] = mapped_column(Text, nullable=False)
    base_depth_in: Mapped[Optional[float]] = mapped_column(Float)
    new_snow_24h_in: Mapped[Optional[float]] = mapped_column(Float)
    lifts_open: Mapped[Optional[int]] = mapped_column(Integer)
    lifts_total: Mapped[Optional[int]] = mapped_column(Integer)
    runs_open: Mapped[Optional[int]] = mapped_column(Integer)
    runs_total: Mapped[Optional[int]] = mapped_column(Integer)


class BackfillProgress(Base):
    __tablename__ = "backfill_progress"

    station_id: Mapped[str] = mapped_column(Text, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'pending'"), default="pending"
    )
    records_inserted: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text("0"), default=0
    )
    completed_at: Mapped[Optional[str]] = mapped_column(Text)
    error_message: Mapped[Optional[str]] = mapped_column(Text)


class NoaaCollectionProgress(Base):
    __tablename__ = "noaa_collection_progress"

    noaa_station_id: Mapped[str] = mapped_column(Text, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'pending'"), default="pending"
    )
    records_inserted: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text("0"), default=0
    )
    completed_at: Mapped[Optional[str]] = mapped_column(Text)


# Convenience: every model, for bulk operations / migration tooling.
ALL_MODELS = [
    Station,
    SnowfallDaily,
    AtmosphericDaily,
    HourlyObservation,
    RadiosondeStation,
    RadiosondeSounding,
    ResortCondition,
    BackfillProgress,
    NoaaCollectionProgress,
]
