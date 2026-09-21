"""
Offline tests for the NWP snowfall engine: forecast-period windows (including
DST), hourly aggregation, probability calibration/blending, the shipped
calibration file, and end-to-end output shape with a fake HTTP session.
"""

import json
import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from snowforecast.engines.nwp_snowfall_forecast import (
    CALIBRATION_PATH,
    TARGET_STATIONS,
    NwpSnowfallForecast,
    period_bounds_utc,
    sum_period,
)

UTC = ZoneInfo("UTC")


def test_period_is_24h_ending_7am_local_in_standard_time():
    start, end = period_bounds_utc(date(2026, 1, 15))
    assert end == datetime(2026, 1, 15, 13, tzinfo=UTC)  # 7 AM CST
    assert end - start == timedelta(hours=24)


def test_period_tracks_daylight_saving_time():
    _, end = period_bounds_utc(date(2026, 3, 20))
    assert end == datetime(2026, 3, 20, 12, tzinfo=UTC)  # 7 AM CDT


def test_sum_period_uses_only_hours_inside_the_window():
    day = date(2026, 1, 15)
    start, _ = period_bounds_utc(day)
    times = [start + timedelta(hours=h) for h in range(-3, 27)]
    mm = [100.0 if (h < 0 or h >= 24) else 1.0 for h in range(-3, 27)]  # outside hours are poison
    assert sum_period(times, mm, day) == pytest.approx(24.0)


def test_sum_period_returns_none_when_too_many_hours_missing():
    day = date(2026, 1, 15)
    start, _ = period_bounds_utc(day)
    times = [start + timedelta(hours=h) for h in range(24)]
    mm = [1.0 if h < 10 else None for h in range(24)]
    assert sum_period(times, mm, day) is None


@pytest.fixture
def calibration(tmp_path):
    cal = {
        "leads": {"1": {"intercept": -2.0, "slope": 1.5, "blend_weight": 0.8, "cv_brier_skill": 0.5}},
        "climatology": [0.3] * 366,
        "calibrated_months": [11, 12, 1, 2, 3],
    }
    path = tmp_path / "cal.json"
    path.write_text(json.dumps(cal))
    return path


def test_uncalibrated_lead_publishes_climatology(calibration):
    engine = NwpSnowfallForecast(calibration)
    p, basis = engine.probability(7, 50.0, date(2026, 1, 15))
    assert (p, basis) == (0.3, "climatology")


def test_missing_forecast_publishes_climatology(calibration):
    p, basis = NwpSnowfallForecast(calibration).probability(1, None, date(2026, 1, 15))
    assert (p, basis) == (0.3, "climatology")


def test_outside_calibrated_season_publishes_climatology(calibration):
    # regression: winter-fit intercept turned a zero-snow September forecast into ~10%
    p, basis = NwpSnowfallForecast(calibration).probability(1, 0.0, date(2026, 9, 17))
    assert (p, basis) == (0.3, "climatology (outside calibrated season)")


def test_probability_blends_model_and_climatology(calibration):
    engine = NwpSnowfallForecast(calibration)
    p, basis = engine.probability(1, 0.0, date(2026, 1, 15))
    p_model = 1 / (1 + math.exp(2.0))
    assert p == pytest.approx(0.8 * p_model + 0.2 * 0.3)
    assert basis == "nwp"


def test_more_forecast_snow_never_lowers_probability(calibration):
    engine = NwpSnowfallForecast(calibration)
    probs = [engine.probability(1, mm, date(2026, 1, 15))[0] for mm in (0, 1, 5, 20, 80)]
    assert probs == sorted(probs)


def test_enso_factor_scales_climatology_fallback_in_calibrated_months(calibration):
    cal = json.loads(calibration.read_text())
    cal["enso_climatology_factors"] = {"strong_el_nino": 0.8}
    calibration.write_text(json.dumps(cal))
    engine = NwpSnowfallForecast(calibration, enso_phase="strong_el_nino")
    assert engine.probability(7, 50.0, date(2026, 1, 15)) == (pytest.approx(0.24), "climatology")
    # the factor was measured over the calibrated months only
    assert engine.probability(7, 0.0, date(2026, 9, 17))[0] == pytest.approx(0.3)
    # it scales the climatology share of a blended lead too, never the NWP share
    p_model = 1 / (1 + math.exp(2.0))
    assert engine.probability(1, 0.0, date(2026, 1, 15))[0] == pytest.approx(0.8 * p_model + 0.2 * 0.24)


def test_phase_without_a_measured_factor_uses_plain_climatology(calibration):
    cal = json.loads(calibration.read_text())
    cal["enso_climatology_factors"] = {"strong_el_nino": 0.8}
    calibration.write_text(json.dumps(cal))
    for phase in ("neutral", "la_nina"):
        engine = NwpSnowfallForecast(calibration, enso_phase=phase)
        assert engine.enso_factor == 1.0
        assert engine.probability(7, 50.0, date(2026, 1, 15)) == (0.3, "climatology")


def test_engine_defaults_to_the_shared_season_phase(calibration):
    from snowforecast.enso import CURRENT_ENSO_PHASE, PHASES
    assert CURRENT_ENSO_PHASE in PHASES
    assert NwpSnowfallForecast(calibration).enso_phase == CURRENT_ENSO_PHASE


def test_oni_bins_and_basic_phase():
    from snowforecast.enso import basic_phase, phase_from_oni
    assert [phase_from_oni(x) for x in (-1.5, -0.7, 0.0, 0.7, 1.8)] == [
        "strong_la_nina", "la_nina", "neutral", "el_nino", "strong_el_nino"]
    assert basic_phase("strong_el_nino") == "el_nino"
    assert basic_phase("neutral") == "neutral"


def test_shipped_calibration_file_is_well_formed():
    cal = json.loads(CALIBRATION_PATH.read_text())
    assert len(cal["climatology"]) == 366
    assert all(0.0 <= x <= 1.0 for x in cal["climatology"])
    assert cal["leads"], "at least one calibrated lead"
    for lead in cal["leads"].values():
        assert {"intercept", "slope", "blend_weight", "cv_brier_skill"} <= set(lead)
        assert 0.0 <= lead["blend_weight"] <= 1.0
        assert lead["slope"] > 0, "more forecast snow must mean higher probability"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Multi-model response: best_match 0.1 cm/h, jma 0.3 cm/h, icon 0.2 cm/h
    (mean 0.2 cm/h). ICON stops after 7 days, as the real one does."""

    def __init__(self, start):
        self.start = start
        self.calls = 0
        self.params = []

    def get(self, url, params, timeout):
        self.calls += 1
        self.params.append(params)
        n = 24 * 10
        hours = [self.start + timedelta(hours=h) for h in range(n)]
        return FakeResponse({"hourly": {
            "time": [t.strftime("%Y-%m-%dT%H:%M") for t in hours],
            "snowfall_best_match": [0.1] * n,
            "snowfall_jma_seamless": [0.3] * n,
            "snowfall_icon_seamless": [0.2] * (24 * 7) + [None] * (n - 24 * 7),
        }})


def test_fetch_averages_models_and_tolerates_a_model_dropping_out(calibration):
    session = FakeSession(datetime(2026, 1, 9, 0, tzinfo=UTC))
    times, mm = NwpSnowfallForecast(calibration, session=session).fetch_hourly_snowfall()
    assert all("jma_seamless" in p["models"] for p in session.params)
    assert mm[0] == pytest.approx(2.0)          # mean(0.1, 0.3, 0.2) cm -> mm, all three present
    assert mm[-1] == pytest.approx(2.0)         # ICON gone: mean(0.1, 0.3) cm -> mm, still a value
    assert None not in mm


def test_generate_end_to_end_with_fake_session(calibration):
    today = date(2026, 1, 10)
    session = FakeSession(datetime(2026, 1, 9, 0, tzinfo=UTC))
    out = NwpSnowfallForecast(calibration, session=session).generate(today=today)

    assert session.calls == len(TARGET_STATIONS)
    days = out["forecasts"]
    assert [d["day_number"] for d in days] == list(range(1, 8))
    assert days[0]["date"] == "2026-01-11"
    assert days[0]["forecast_snowfall_mm"] == pytest.approx(48.0)  # model-mean 0.2 cm/h * 24h -> mm
    assert out["models"] == ["best_match", "jma_seamless", "icon_seamless"]
    assert days[0]["basis"] == "nwp"
    assert all(d["basis"] == "climatology" for d in days[1:])  # only lead 1 calibrated in fixture
    assert all(0 <= d["probability"] <= 100 for d in days)
