"""
NWP-based snowfall probability forecast.

Replacement candidate for EnhancedRegionalForecastSystem. That engine predicts
from snow already observed at other stations; verified against measured COOP
snowfall under production data availability, its probabilities score worse
than day-of-year climatology at days 1-3. This engine instead uses numerical
weather prediction (Open-Meteo's forecast API) and turns the forecast snowfall
into a probability with a per-lead calibration fit against measured snowfall
(see scripts/backtest/fit_nwp_calibration.py).

Forecast period: the 24 hours ending 7:00 AM America/Chicago on each date --
the same window a COOP observer's snow-board reading covers. Matching that
window verified far better than calendar days (day-1 AUC 0.92 vs 0.86).

Probability for lead k:
    p_model = logistic(intercept_k + slope_k * log1p(forecast_mm)
                       + clim_coef_k * logit(climatology(day_of_year)))
    p       = w_k * p_model + (1 - w_k) * climatology(day_of_year)
The climatology term lets a zero-snow forecast mean "near zero" in a month
that rarely snows, instead of carrying a mid-winter base rate year-round.
Both the term and w_k are chosen by leave-one-winter-out cross-validation,
so leads with little NWP skill fall back toward climatology instead of
publishing false confidence.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

CALIBRATION_PATH = Path(__file__).with_name("nwp_calibration.json")
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
LOCAL_TZ = ZoneInfo("America/Chicago")
PERIOD_END_HOUR = 7  # local; COOP observation time
MEASURABLE_MM = 5.0

TARGET_STATIONS = {
    "phelps_wi": (46.0638, -89.0787),
    "land_o_lakes_wi": (46.1535, -89.3207),
    "eagle_river_wi": (45.9169, -89.2443),
}


def period_bounds_utc(day: date) -> tuple[datetime, datetime]:
    """UTC [start, end) of the 24h ending 7 AM local on `day` (DST-aware)."""
    end = datetime.combine(day, time(PERIOD_END_HOUR), LOCAL_TZ)
    start = datetime.combine(day - timedelta(days=1), time(PERIOD_END_HOUR), LOCAL_TZ)
    return start.astimezone(ZoneInfo("UTC")), end.astimezone(ZoneInfo("UTC"))


def logit(p: float, eps: float = 1e-3) -> float:
    p = min(max(p, eps), 1 - eps)
    return math.log(p / (1 - p))


def sum_period(hourly_times: list[datetime], hourly_mm: list[float | None], day: date,
               min_hours: int = 20) -> float | None:
    """Sum hourly mm over the period for `day`; None if too many hours are missing."""
    start, end = period_bounds_utc(day)
    vals = [v for t, v in zip(hourly_times, hourly_mm) if start <= t < end and v is not None]
    return sum(vals) if len(vals) >= min_hours else None


class NwpSnowfallForecast:
    """7-day snowfall probability from calibrated NWP forecasts."""

    def __init__(self, calibration_path: Path | str = CALIBRATION_PATH, session=None):
        with open(calibration_path) as f:
            self.calibration = json.load(f)
        self.session = session or requests

    def fetch_hourly_snowfall(self) -> tuple[list[datetime], list[float | None]]:
        """Hourly snowfall (mm), averaged across the target stations, UTC timestamps."""
        per_station = []
        for lat, lon in TARGET_STATIONS.values():
            r = self.session.get(FORECAST_URL, params={
                "latitude": lat, "longitude": lon, "hourly": "snowfall",
                "past_days": 1, "forecast_days": 9, "timezone": "UTC",
            }, timeout=30)
            r.raise_for_status()
            h = r.json()["hourly"]
            times = [datetime.fromisoformat(t).replace(tzinfo=ZoneInfo("UTC")) for t in h["time"]]
            per_station.append(dict(zip(times, h["snowfall"])))

        times = sorted(set().union(*per_station))
        mean_mm = []
        for t in times:
            vals = [s[t] for s in per_station if s.get(t) is not None]
            # Open-Meteo reports snowfall in cm; require every station for a clean mean
            mean_mm.append(sum(vals) / len(vals) * 10.0 if len(vals) == len(per_station) else None)
        return times, mean_mm

    def probability(self, lead: int, forecast_mm: float | None, day: date) -> tuple[float, str]:
        """Blend calibrated NWP probability with climatology. Returns (p, basis)."""
        clim = self.calibration["climatology"][day.timetuple().tm_yday - 1]
        cal = self.calibration["leads"].get(str(lead))
        # No calibration at this lead means no verified skill: publish the
        # climatological rate rather than borrowing a shorter lead's confidence.
        if cal is None or forecast_mm is None or cal["blend_weight"] == 0:
            return clim, "climatology"
        # Coefficients were fit only on these months. Outside them the winter
        # base rate baked into the intercept turns a zero-snow September
        # forecast into ~10%, so publish climatology instead. The raw forecast
        # snowfall is still reported alongside, so off-season storms stay visible.
        if day.month not in self.calibration.get("calibrated_months", range(1, 13)):
            return clim, "climatology (outside calibrated season)"
        z = (cal["intercept"] + cal["slope"] * math.log1p(max(forecast_mm, 0.0))
             + cal.get("clim_coef", 0.0) * logit(clim))
        p_model = 1.0 / (1.0 + math.exp(-z))
        w = cal["blend_weight"]
        return w * p_model + (1 - w) * clim, "nwp" if w >= 0.5 else "nwp+climatology"

    def generate(self, today: date | None = None, days_ahead: int = 7) -> dict:
        today = today or datetime.now(LOCAL_TZ).date()
        times, mean_mm = self.fetch_hourly_snowfall()
        days = []
        for k in range(1, days_ahead + 1):
            day = today + timedelta(days=k)
            mm = sum_period(times, mean_mm, day)
            p, basis = self.probability(k, mm, day)
            start, end = period_bounds_utc(day)
            cal = self.calibration["leads"].get(str(k), {})
            days.append({
                "date": day.isoformat(),
                "day_number": k,
                "period_start_utc": start.isoformat(),
                "period_end_utc": end.isoformat(),
                "period_label": f"24 hours ending 7 AM {day.strftime('%a %b')} {day.day}",
                "probability": round(100 * p),
                "forecast_snowfall_mm": None if mm is None else round(mm, 1),
                "forecast_snowfall_in": None if mm is None else round(mm / 25.4, 1),
                "basis": basis,
                "verified_skill_vs_climatology": cal.get("cv_brier_skill"),
            })
        return {
            "engine": "nwp_snowfall_forecast",
            "generated_at": datetime.now(LOCAL_TZ).isoformat(),
            "measurable_threshold_mm": MEASURABLE_MM,
            "target_stations": list(TARGET_STATIONS),
            "calibration_generated_at": self.calibration.get("generated_at"),
            "forecasts": days,
        }
