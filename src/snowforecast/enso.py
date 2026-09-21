"""
Current ENSO phase, shared by everything on the site that carries a seasonal
signal (the NWP engine's climatology fallback and the trip planner's badge).

Update once per season from NOAA CPC's ENSO diagnostic discussion
(https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/enso_advisory/)
and the Oceanic Niño Index (https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt).

Phases are keyed on the DJF Oceanic Niño Index, using the bins that the
calibration fit uses to stratify COOP-measured winters (see
scripts/backtest/fit_nwp_calibration.py --oni):

    strong_la_nina  ONI <= -1.0
    la_nina         -1.0 < ONI <= -0.5
    neutral         -0.5 < ONI <  0.5
    el_nino          0.5 <= ONI < 1.0
    strong_el_nino  ONI >= 1.0   (CPC "moderate" or stronger)
"""

PHASES = ("strong_la_nina", "la_nina", "neutral", "el_nino", "strong_el_nino")

# Winter 2026-27. NOAA CPC, 2026-09-10: August Niño-3.4 anomaly +1.8 C, >90%
# chance of a very strong El Niño through winter, 75% chance it exceeds every
# event since 1950. The 2026-09-17 DJF outlook tilts the Great Lakes warm and
# below-normal precipitation.
CURRENT_ENSO_PHASE = "strong_el_nino"
CURRENT_ENSO_SOURCE = "NOAA CPC ENSO diagnostic discussion, 2026-09-10"


def phase_from_oni(oni: float) -> str:
    """Map a DJF Oceanic Niño Index value to one of PHASES."""
    if oni <= -1.0:
        return "strong_la_nina"
    if oni <= -0.5:
        return "la_nina"
    if oni < 0.5:
        return "neutral"
    if oni < 1.0:
        return "el_nino"
    return "strong_el_nino"


def basic_phase(phase: str) -> str:
    """Collapse strength: 'strong_el_nino' -> 'el_nino'. For consumers that only
    know the three classic phases (the planner badge and its ENSO modulation)."""
    return phase.removeprefix("strong_")
