# NWP engine: full-history verification and fine-tune review

**Date:** 2026-09-20 (overnight iteration through 2026-09-21; see "Iteration log")
**Branch:** `feature/enso-conditioning` (PR #85, on top of PR #83)
**Question:** Over every winter we can grade, does `NwpSnowfallForecast` predict the way it should, and can it be tuned closer?

## Setup

- **Truth:** NWS COOP / CoCoRaHS via RCC-ACIS (`coop_truth_targets.csv`), 3-station mean over the 24 h ending 7 AM local; event = measurable snow (>= 5 mm).
- **Sources:**
  - GEFSv12 reforecast, control member, 2000-2019 (21 winter seasons, every 3rd Nov-Mar day at the start of this review; densified overnight). Snowfall derived from 6-h precipitation and 2 m temperature. Leads 1-6.
  - Open-Meteo previous-runs archive, 2023-2026 (3 winters), 3-model mean (best_match + JMA + ICON), the exact input the engine uses. Leads 1-6.
- **Scoring:** leave-one-winter-out. Every coefficient and the climatology blend weight are fit on the other winters only. Skill = 1 - Brier / Brier(day-of-year climatology).
- **Caveat:** GEFS-derived snowfall is a different quantity from the Open-Meteo model mean, so the GEFS run tests the *method* (per-lead logistic calibration + blend), not the shipped coefficients.

## Is it predicting the way it should?

Yes. Reliability is good at every lead in both sources, overall bias is zero, and the top decile of forecasts verifies at about its stated rate through day 3.

| Lead | GEFS skill | GEFS AUC | GEFS top-10% fcst / obs | Open-Meteo skill | OM AUC | OM top-10% fcst / obs |
|---|---|---|---|---|---|---|
| 1 | 38% | 0.86 | 0.92 / 0.93 | 57% | 0.93 | 0.94 / 0.91 |
| 2 | 33% | 0.84 | 0.88 / 0.88 | 52% | 0.91 | 0.90 / 0.91 |
| 3 | 27% | 0.82 | 0.82 / 0.79 | 43% | 0.88 | 0.84 / 0.84 |
| 4 | 12% | 0.73 | 0.71 / 0.67 | 29% | 0.83 | 0.75 / 0.68 |
| 5 | 14% | 0.73 | 0.73 / 0.72 | 22% | 0.79 | 0.66 / 0.58 |
| 6 | 5% | 0.67 | 0.59 / 0.51 | 10% | 0.72 | 0.62 / 0.35 |

Reliability-bin gaps (observed minus forecast) are within +/- 0.05 for nearly every bin with more than ~50 days at leads 1-3. Two systematic patterns:

1. **Days 4-6 overforecast at the top end.** The highest-probability decile verifies 4-8 points below its stated rate in GEFS and 6-27 points below in Open-Meteo (the Open-Meteo day-6 figure rests on ~43 days). Direction is consistent across both sources, so it is real but small.
2. **March overforecasts** (GEFS lead 1 bias +0.07, Open-Meteo +0.06): forecast snow in March verifies less often on the snow board than in mid-winter.

### By ENSO phase (GEFS, 20 winters)

| Phase | Winters | Lead-1 obs rate | Lead-1 bias (p - y) | Lead-1 skill | Lead-1 AUC |
|---|---|---|---|---|---|
| strong La Niña | 3 | 0.34 | +0.05 | 37% | 0.88 |
| La Niña | 5 | 0.41 | +0.01 | 30% | 0.83 |
| neutral | 5 | 0.41 | 0.00 | 38% | 0.85 |
| El Niño | 6 | 0.43 | -0.05 | 43% | 0.88 |
| **strong El Niño** | 2 | **0.27** | **+0.06** | 43% | 0.91 |

Strong El Niño winters have the fewest snow days but the *best* discrimination (AUC 0.91): the model still ranks the days correctly, it just runs a few points warm. The same +0.06 to +0.08 bias appears at day 4 and shrinks to +0.02 to +0.04 at days 5-6.

## Fine-tune candidates (all leave-one-winter-out)

| Candidate | GEFS | Open-Meteo | Verdict |
|---|---|---|---|
| A. Seasonal feature: add logit(climatology) to the logistic | +1 pt at every lead; March bias 0.07 -> 0.03 | **-1 to -3 pts at every lead** | Reject. Helps the 20-winter source, hurts the input we ship. |
| B. Shrink the slope at days 4-6 (x0.9 .. x0.7) | monotonically worse | monotonically worse | Reject. Top-decile gap closes only by giving up skill. |
| C. ENSO-shifted intercept for strong El Niño | removes the bias (+0.06 -> +0.01) but skill in those winters falls 43% -> 40% (d1), 15% -> 11% (d4) | n/a (one such winter) | Reject. With 2 training winters the shift overcorrects on the held-out one. |
| D. ENSO factor on climatology (shipped in PR #85) | 3-fold over 2009-10, 2015-16, 2023-24 with the factor from the other two: mean climatology Brier 0.198 -> 0.192; the fixed shipped 0.79 gives 0.188. 2 of 3 winters improve, 2015-16 (obs rate 0.34, near normal) worsens. | | **Keep.** Modest, consistent with NOAA's outlook, and it only touches the fallback. |

**Conclusion:** the shipped calibration is at the out-of-sample optimum of what these data support. The day-4-6 top-end and March overforecasts are documented but not corrected, because every correction tested cost more Brier skill than it recovered. Revisit with more Open-Meteo winters (each new winter adds ~33% to that sample).

## Not in scope, worth knowing

- **Day 7 is now calibrated (shipped 23:15).** The local JMA archive carries `snowfall_previous_day7` (ICON is empty there; the cached best_match pull predates day 7). The calibration file now records the model subset each lead was fit on, and the engine averages only those models at that lead, so the live day-7 input (JMA only) matches its coefficients: AUC 0.68, +11.8% Brier skill, previously published as pure climatology. The live API also serves best_match and GFS at day 7, so a refit after re-pulling the best_match archive with day 7 should lift this further. **Nothing archived exists past day 7** in Open-Meteo's previous-runs API for any model, so days 8+ cannot be calibrated from it.
- **Beyond day 7 needs the ensemble.** GEFSv12 reforecasts on S3 hold 5 members (c00, p01-p04) to day 16 for 2000-2019, and Open-Meteo's live ensemble API serves 31 GEFS members to 16 days. Calibrating ensemble probability of measurable snow on the reforecast and applying it to the live ensemble is the physically consistent route to days 8-10; it is a multi-hour extraction and a new engine input, not an overnight change.
- The GEFS extraction now covers only the 3 target stations per init (the earlier every-3rd-day pass covered 19).

## Iteration log

- **22:36 CDT** GEFS densification to every winter day started in the background (resumable; `fetch_every1.log`). At 4 workers it ran ~1.3 inits/min (about 25 h for the remaining ~2000); restarted at 22:47 with 8 workers.
- **22:38** Shipped calibration reproduced byte-for-byte from the Sept 18 cache (`nwp_cache_calib`) and `coop_truth_targets.csv`; ENSO keys added.
- **22:40** First full-history run (GEFS every 3rd day, 968 days at lead 1). Results above.
- **22:43** Candidates A-D scored. None adopted beyond D.
- **23:00** Probed the archive for longer leads: every model stops at previous_day7; ICON and ECMWF have nothing at day 7. Live GEFS ensemble (31 members, 16 days) confirmed available.
- **23:04** Lead 7 calibrated on the local JMA archive and shipped with per-lead model subsets (engine + fit script + tests). Leads 1-6 coefficients unchanged.
- **23:55** Rerun with 1362 GEFS days at lead 1 (was 968; winters 2000-2004 now every day). Skill d1-6: 37 / 30 / 25 / 14 / 11 / 5% (was 38 / 33 / 27 / 12 / 14 / 5%). Top-decile reliability is now on the line at every lead (d4 0.74 forecast vs 0.72 observed, d5 0.70 vs 0.73, d6 0.60 vs 0.55), so the "days 4-6 overforecast at the top end" pattern was mostly small-sample noise in GEFS; it remains in the 3-winter Open-Meteo set. Strong-El Niño overforecast persists (d1 +0.07, d4 +0.09) but the ENSO-shifted intercept still loses held-out skill at d1, d3, d4 (gains at d2, d5, d6): rejected again. Seasonal feature: still +1 pt on GEFS, still -1 to -3 on Open-Meteo: rejected. ENSO factor 3-fold result unchanged. No calibration change.
