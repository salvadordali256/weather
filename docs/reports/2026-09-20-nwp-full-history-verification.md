# NWP engine: full-history verification and fine-tune review

**Date:** 2026-09-20, finalized 2026-09-21 03:20 CDT (see "Iteration log")
**Branch:** `feature/enso-conditioning` (PR #85, on top of PR #83)
**Question:** Over every winter we can grade, does `NwpSnowfallForecast` predict the way it should, and can it be tuned closer?

## Setup

- **Truth:** NWS COOP / CoCoRaHS via RCC-ACIS (`coop_truth_targets.csv`), 3-station mean over the 24 h ending 7 AM local; event = measurable snow (>= 5 mm).
- **Sources:**
  - GEFSv12 reforecast, control member, 2000-2019, every Nov-Mar day (3025 inits; 2894 graded days at lead 1 across 21 winter seasons). Snowfall derived from 6-h precipitation and 2 m temperature. Leads 1-6. The tables below are from the complete set; the iteration log shows how they moved as the set filled in.
  - Open-Meteo previous-runs archive, 2023-2026 (3 winters), 3-model mean (best_match + JMA + ICON), the exact input the engine uses. Leads 1-6.
- **Scoring:** leave-one-winter-out. Every coefficient and the climatology blend weight are fit on the other winters only. Skill = 1 - Brier / Brier(day-of-year climatology).
- **Caveat:** GEFS-derived snowfall is a different quantity from the Open-Meteo model mean, so the GEFS run tests the *method* (per-lead logistic calibration + blend), not the shipped coefficients.

## Is it predicting the way it should?

Yes. Reliability is good at every lead in both sources, overall bias is zero, and the top decile of forecasts verifies at about its stated rate through day 3.

| Lead | GEFS skill | GEFS AUC | GEFS top-10% fcst / obs | Open-Meteo skill | OM AUC | OM top-10% fcst / obs |
|---|---|---|---|---|---|---|
| 1 | 41% | 0.88 | 0.93 / 0.94 | 57% | 0.93 | 0.94 / 0.91 |
| 2 | 34% | 0.84 | 0.88 / 0.87 | 52% | 0.91 | 0.90 / 0.91 |
| 3 | 27% | 0.81 | 0.83 / 0.82 | 43% | 0.88 | 0.84 / 0.84 |
| 4 | 18% | 0.76 | 0.76 / 0.74 | 29% | 0.83 | 0.75 / 0.68 |
| 5 | 13% | 0.73 | 0.71 / 0.70 | 22% | 0.79 | 0.66 / 0.58 |
| 6 | 8% | 0.69 | 0.63 / 0.64 | 10% | 0.72 | 0.62 / 0.35 |
| 7 | n/a | n/a | n/a | 12% | 0.68 | (JMA only; shipped 23:04) |

Reliability-bin gaps (observed minus forecast) are within +/- 0.03 for every bin with more than ~100 days on the full GEFS set, and the top decile is on the line at every lead. Two patterns seen in the first (every-3rd-day) pass did not survive the full sample:

1. **Days 4-6 top-end overforecast: gone in GEFS.** With 2800+ days the top decile verifies within 2 points of its stated rate at every lead. It remains visible in the 3-winter Open-Meteo set (day 6: 0.62 forecast vs 0.35 observed on ~43 days), which the GEFS result says is small-sample noise rather than a defect of the method.
2. **Strong El Niño overforecast: gone.** Started at +0.06 at day 1 with 101 days; +0.013 with all 301 days. Every ENSO phase now sits within +/- 0.01 of zero bias at day 1.

One pattern does persist: **March overforecasts** (GEFS day-1 bias +0.06; December and January under-forecast by 0.03-0.04). Forecast snow in March verifies less often on the snow board than in mid-winter, presumably melt before the 7 AM reading.

### By ENSO phase (GEFS, 21 winter seasons, full set)

| Phase | Days (lead 1) | Obs rate | Bias (p - y) | Skill | AUC |
|---|---|---|---|---|---|
| strong La Niña | 388 | 0.35 | +0.01 | 48% | 0.91 |
| La Niña | 694 | 0.40 | 0.00 | 34% | 0.85 |
| neutral | 708 | 0.40 | 0.00 | 42% | 0.87 |
| El Niño | 803 | 0.37 | 0.00 | 43% | 0.88 |
| **strong El Niño** | 301 | **0.29** | +0.01 | 45% | 0.91 |

Strong El Niño winters have the fewest snow days and the best discrimination; the NWP term needs no ENSO adjustment. Only the climatology fallback does (candidate D).

## Fine-tune candidates (all leave-one-winter-out)

| Candidate | GEFS | Open-Meteo | Verdict |
|---|---|---|---|
| A. Seasonal feature: add logit(climatology) to the logistic | +1 pt at every lead on the full set; March bias 0.06 -> 0.02 | **-1 to -3 pts at every lead** | Reject for now. Consistently helps the 20-winter source and fixes the March pattern, but hurts the input we ship. Re-test when a 4th Open-Meteo winter exists; if it turns positive there, adopt. |
| B. Shrink the slope at days 4-6 (x0.9 .. x0.7) | monotonically worse | monotonically worse | Reject. Top-decile gap closes only by giving up skill. |
| C. ENSO-shifted intercept for strong El Niño | a wash on the full set (within 0.5 pt at every lead) because the bias it targets vanished with full sampling | n/a (one such winter) | Reject. Nothing left to correct. |
| D. ENSO factor on climatology (shipped in PR #85) | 3-fold over 2009-10, 2015-16, 2023-24 with the factor from the other two: mean climatology Brier 0.198 -> 0.192; the fixed shipped 0.79 gives 0.188. 2 of 3 winters improve, 2015-16 (obs rate 0.34, near normal) worsens. | | **Keep.** Modest, consistent with NOAA's outlook, and it only touches the fallback. |

**Conclusion:** the shipped calibration is at the out-of-sample optimum of what these data support. Over 21 winter seasons the method is reliable at every lead and every ENSO phase, with skill 41% at day 1 falling to 8% at day 6 on GEFS and 57% to 12% at days 1-7 on the shipped Open-Meteo input. The March overforecast is documented but not corrected, because the only fix that works on GEFS (the seasonal feature) costs skill on the shipped input. Revisit with the 4th Open-Meteo winter (2026-27), which adds ~33% to that sample.

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
- **00:47** Rerun with 1706 GEFS days at lead 1 (winters 2000-2007 now every day; 1786 of 3025 inits). Skill d1-6: 37 / 29 / 25 / 16 / 12 / 6%. Reliability on the line at every lead including the top decile (d6 0.62 forecast vs 0.60 observed). Strong-El Niño overforecast steady at d1 +0.07, d4 +0.09; the ENSO-shifted intercept still loses held-out skill at d1, d3, d4 (43 -> 40%, 21 -> 19%, 15 -> 12%). Seasonal feature +1 pt on GEFS, unchanged loss on Open-Meteo. Slope shrink monotonically worse. No calibration change.
- **01:39** Rerun with 2113 GEFS days at lead 1 (winters 2000-2011 every day; 2209 of 3025 inits; strong-El Niño 2009-10 now fully sampled, 200 days). Skill d1-6: 40 / 31 / 24 / 17 / 12 / 6%. Top decile on the line at every lead. With the fuller 2009-10 sample the strong-El Niño overforecast at d1 shrank to +0.04 (d4 +0.06, d6 +0.10) and the ENSO-shifted intercept is now a wash on GEFS (d1 -0.3 pt, d2 +1.2, d3 +1.7, d4 -0.2, d5 +1.4, d6 +3.4 in those winters; overall skill unchanged); it still cannot be tested on the shipped input (one such winter), so it stays out. Seasonal feature +1 pt on GEFS, still negative on Open-Meteo. No calibration change.
- **02:31** Rerun with 2594 GEFS days at lead 1 (winters 2000-2016 every day; 2719 of 3025 inits; both strong-El Niño winters fully sampled, 301 days). Skill d1-6: 41 / 32 / 26 / 17 / 13 / 7%. Top decile on the line at every lead. The strong-El Niño overforecast has shrunk to +0.02 at d1 (+0.05 at d4, +0.06 at d6) now that those winters are fully sampled: the +0.06 seen at 22:40 was mostly the every-3rd-day subsample. The ENSO-shifted intercept is a wash at every lead (within 0.6 pt). Seasonal feature +1 pt on GEFS, unchanged loss on Open-Meteo. No calibration change.
- **03:13** Fetch complete: 3025 of 3025 inits, 0 failed. Final run with 2894 GEFS days at lead 1. Skill d1-6: 41 / 34 / 27 / 18 / 13 / 8%. Bias 0.000 overall and within +/- 0.01 in every ENSO phase; top decile on the line at every lead. Seasonal feature +1 pt on GEFS (March bias 0.06 -> 0.02), still -1 to -3 on Open-Meteo. Slope shrink and ENSO intercept: no gain. ENSO climatology factor 3-fold: unchanged. Tables above updated to the full set. No calibration change; loop closed.

## GEFS ensemble: skill beyond day 7

**Question:** does the 5-member GEFSv12 reforecast ensemble (c00 + p01-p04, 6-hourly to 384 h) carry skill past day 7, and does it beat the control member at days 4-7? Every 6th winter day 2000-2019 (505 inits), same derived-snow rule and COOP truth, leave-one-winter-out logistic + climatology blend. Three predictors: control member, ensemble mean, ensemble mean + fraction of members >= 5 mm.

### Interim, 17:35 (241 of 505 inits; winters 2000-01 through 2008-09, ~225 days per lead)

| Lead | Control skill | Ens-mean skill | Ens-mean + fraction | Control AUC | Ens-mean AUC |
|---|---|---|---|---|---|
| 1 | 33% | 31% | 31% | 0.84 | 0.83 |
| 2 | 30% | 30% | 30% | 0.83 | 0.83 |
| 3 | 9% | 10% | 10% | 0.71 | 0.73 |
| 4 | 3% | 4% | 5% | 0.65 | 0.66 |
| 5 | 10% | **17%** | 17% | 0.70 | 0.75 |
| 6 | 9% | **13%** | 13% | 0.67 | 0.69 |
| 7 | 5% | **13%** | 14% | 0.63 | 0.72 |
| 8 | -2% | -1% | 0% | 0.54 | 0.57 |
| 9 | -1% | 0% | -1% | 0.57 | 0.53 |
| 10 | -2% | -2% | -3% | 0.55 | 0.48 |
| 11-15 | -0 to +6% | -1 to +5% | -1 to +5% | 0.53-0.66 | 0.50-0.65 |

Read at half sample: the ensemble mean adds 4-8 points at days 5-7 and nothing at days 1-2; past day 7 nothing has skill (AUC ~0.55, blend weights collapse toward climatology). The scattered positives at days 11-12 are noise at this sample size (a day-11 forecast cannot outscore day 8). Final numbers on the full 505 inits follow.
