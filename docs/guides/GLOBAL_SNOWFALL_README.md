# Global Snowfall Prediction System for Phelps & Land O'Lakes, Wisconsin

## 🌍 Overview

This is a **worldwide snowfall analysis and prediction system** that uses global weather patterns to forecast snowfall in Phelps and Land O'Lakes, Wisconsin. The system analyzes correlations between snowfall patterns across the globe and your target area, identifying which regions serve as the best predictors.

### The Core Idea

**When it snows heavily in Russia, Japan, or California, does it predict Wisconsin snowfall days or weeks later?**

The system answers this by:
1. Collecting 85 years (1940-2025) of snowfall data from **100+ locations worldwide**
2. Analyzing cross-regional correlations and lag patterns
3. Identifying the strongest global predictors
4. Building a weighted prediction model

## 🗺️ Global Coverage

### Regions Analyzed

| Region | Locations | Significance for Wisconsin |
|--------|-----------|----------------------------|
| **Northern Wisconsin** | Phelps, Land O'Lakes, Eagle River | Primary target area |
| **Russia/Siberia** | 11 stations across Siberia, Arctic | Cold air source region for North America |
| **Japan** | 7 stations (Hokkaido, Sea of Japan coast, Alps) | East Asian jet stream strength indicator |
| **China** | 7 stations (Manchuria, Tibet, NW China) | Tibetan High influence, continental patterns |
| **California Ski Resorts** | Mammoth, Tahoe, Shasta, Big Bear | Pacific atmospheric river/blocking patterns |
| **Colorado Rockies** | Aspen, Vail, Steamboat, Telluride, Breckenridge | Continental upslope, jet stream position |
| **Pacific Northwest** | Mt. Baker, Stevens Pass, Mt. Hood | Pacific moisture source |
| **Canada** | BC, Rockies, Great Lakes, Quebec | Regional proximity, Arctic influence |
| **Europe Alps** | France, Switzerland, Austria, Italy | Northern Hemisphere completeness |
| **Scandinavia** | Norway, Sweden, Finland | Arctic patterns, polar low systems |

**Total: 100+ locations across 10+ countries**

## 🔬 Scientific Basis

### Teleconnections

Global atmospheric patterns are connected across continents through:

1. **Wave Energy Propagation**
   - Asian jet stream → Stratosphere → North America (5-10 day lag)
   - Example: Siberian High → Polar vortex disruption → Wisconsin cold outbreak

2. **Pacific Patterns**
   - California/Colorado heavy snow → Atmospheric river strength → Downstream effects on Wisconsin
   - Blocking patterns: Western US ridging → Downstream troughing over Great Lakes

3. **Arctic Connections**
   - Polar vortex strength (measured by Arctic Russia snowfall)
   - Sudden Stratospheric Warming events → Cold air outbreaks

4. **ENSO/PDO Patterns**
   - El Niño/La Niña affects global circulation
   - Pacific Decadal Oscillation modulates patterns

## 🛠️ System Components

### 1. Data Collection (`global_snowfall_fetcher.py`)

Collects historical weather data from **Open-Meteo Historical Weather API**:
- **Coverage**: 1940-present (85+ years)
- **Parameters**: Daily snowfall, snow depth, temperature, precipitation, pressure, wind
- **Source**: Global grid-based data (any coordinates worldwide)
- **API**: Free, unlimited access with rate limiting

```bash
# Collect all global data (takes several hours first time)
python global_snowfall_fetcher.py --start 1940-01-01 --rate-limit 1.0

# Register locations only (no data fetch)
python global_snowfall_fetcher.py --register-only

# Show summary
python global_snowfall_fetcher.py --summary
```

**Database Schema:**
```sql
stations:           Global location metadata (region, coordinates, significance)
snowfall_daily:     Daily measurements (snowfall, temperature, pressure, wind)
regional_summaries: Pre-calculated statistics by region
correlations:       Cached cross-regional correlation results
```

### 2. Correlation Analysis (`global_correlation_analysis.py`)

Analyzes relationships between global regions and Northern Wisconsin:

**Key Analyses:**
- **Lag correlations**: Find optimal time lag between regions (e.g., "Russia leads WI by 7 days")
- **Strength measurement**: Pearson correlation with statistical significance testing
- **Pattern identification**: Which regions predict Wisconsin snowfall best?
- **Extreme events**: Do extreme snow events co-occur across regions?

```bash
# Full correlation analysis
python global_correlation_analysis.py \
    --target northern_wisconsin \
    --max-lag 30 \
    --export \
    --model \
    --model-file phelps_prediction_model.json

# Results show:
# - Best correlation coefficient
# - Optimal lag (days)
# - Statistical significance
# - Top predictor regions
```

**Output:**
- `phelps_prediction_model.json`: Weighted ensemble prediction model
- Database cache of correlations for fast future queries

### 3. Visualization (`visualize_global_snowfall.py`)

Creates comprehensive visual analysis:

```bash
python visualize_global_snowfall.py \
    --db global_snowfall.db \
    --model phelps_prediction_model.json
```

**Generated Visualizations:**
1. **Correlation Heatmap**: Visual matrix of regional correlations
2. **Global Map**: Geographic visualization showing prediction network
3. **Lag Distribution**: Histogram and scatter of lag patterns
4. **Model Summary**: Multi-panel prediction model breakdown
5. **Time Series Comparison**: Side-by-side regional snowfall plots

**Output Directory:** `snowfall_graphs/`

### 4. Master Orchestrator (`run_global_analysis.py`)

Complete end-to-end pipeline automation:

```bash
# Full pipeline (data + analysis + visualizations + report)
python run_global_analysis.py --full

# Just analysis (if data already collected)
python run_global_analysis.py --analyze-only

# Quick summary
python run_global_analysis.py --summary

# Custom date range
python run_global_analysis.py --full --start 2000-01-01

# Force re-collection
python run_global_analysis.py --full --force-collect
```

**Pipeline Steps:**
1. Data collection (if needed)
2. Correlation analysis (all regions vs Wisconsin)
3. Prediction model generation
4. Visualization creation
5. Comprehensive report generation

**Output Files:**
- `global_snowfall.db` - Complete global database (~2GB with full history)
- `phelps_prediction_model.json` - Prediction model configuration
- `global_analysis_report.txt` - Human-readable summary report
- `snowfall_graphs/*.png` - All visualizations

## 📊 Expected Results

### Sample Output

Based on teleconnection science, you'll likely see patterns like:

**Top Predictors (Hypothetical):**
1. **Siberia Central** - Strong positive correlation, 7-day lead
   - *Interpretation*: Heavy Siberian snowfall → Cold air mass formation → Wisconsin outbreak 1 week later

2. **Japan North (Hokkaido)** - Moderate positive correlation, 5-day lead
   - *Interpretation*: Extreme Japan snowfall → Amplified East Asian jet → North American pattern 5 days later

3. **California Ski** - Moderate correlation, 3-day lead
   - *Interpretation*: California atmospheric rivers → Blocking pattern → Moisture transport to Wisconsin

4. **Canada East (Thunder Bay)** - Strong positive correlation, 0-day lag
   - *Interpretation*: Simultaneous Lake Superior lake-effect events

5. **Colorado Rockies** - Weak-moderate correlation, 2-day lead
   - *Interpretation*: Upslope snow → Jet stream position indicator

### Interpretation Guide

**Correlation Strength:**
- `|r| > 0.3`: Strong (excellent predictor)
- `|r| = 0.15-0.3`: Moderate (useful predictor)
- `|r| < 0.15`: Weak (limited predictive value)

**Lag Patterns:**
- **Positive lag**: Predictor region leads Wisconsin (early warning signal)
- **Zero lag**: Simultaneous events (same weather system)
- **Negative lag**: Wisconsin leads predictor (not useful for prediction)

**Statistical Significance:**
- `p < 0.05` (marked with `***`): Statistically significant
- `p >= 0.05`: May be coincidental, use with caution

## 🚀 Quick Start

### Installation

```bash
# Ensure dependencies are installed
pip install -r weather_requirements.txt

# Additional requirements for global system
pip install scipy seaborn matplotlib pandas
```

### Option 1: Complete Analysis (Recommended for first run)

```bash
# This will take 3-5 hours for initial data collection
python run_global_analysis.py --full
```

This collects 85 years of data for 100+ locations worldwide (~3-5 hours at 1 second/location rate limit).

**Progress Tracking:**
- Shows real-time collection status for each location
- Organizes by region (Russia, Japan, China, etc.)
- Displays success/failure for each API call
- Final summary with database statistics

### Option 2: Quick Test (Recommended for testing)

```bash
# Collect just recent data (faster)
python global_snowfall_fetcher.py --start 2020-01-01

# Run analysis
python run_global_analysis.py --analyze-only
```

### Option 3: Incremental Approach

```bash
# Step 1: Register locations
python global_snowfall_fetcher.py --register-only --summary

# Step 2: Collect data (can interrupt and resume)
python global_snowfall_fetcher.py --start 1940-01-01

# Step 3: Analyze
python global_correlation_analysis.py --export --model

# Step 4: Visualize
python visualize_global_snowfall.py

# Step 5: Report
python run_global_analysis.py --analyze-only
```

## 📈 Using the Prediction Model

Once generated, `phelps_prediction_model.json` contains:

```json
{
  "target_region": "northern_wisconsin",
  "num_predictors": 20,
  "predictors": [
    {
      "region": "siberia_central",
      "correlation": 0.347,
      "lag_days": 7,
      "p_value": 0.0001,
      "significant": true,
      "weight": 0.347,
      "normalized_weight": 0.085
    },
    ...
  ]
}
```

**How to Use:**
1. Monitor current snowfall in top predictor regions
2. Apply lag pattern (e.g., Russia + 7 days = Wisconsin forecast)
3. Weight predictions by correlation strength
4. Combine multiple predictors for ensemble forecast

**Example Forecast Logic:**
```
IF Siberia gets 50mm snow today (Day 0)
   AND California gets 30mm snow today (Day 0)
   AND Japan got 40mm snow 2 days ago (Day -2)
THEN Wisconsin likely gets heavy snow in 5-7 days (ensemble average lag)
```

## 🔄 Operational Use

### Daily Update Workflow

```bash
# 1. Update global database with latest data
python global_snowfall_fetcher.py \
    --start $(date -d "7 days ago" +%Y-%m-%d) \
    --rate-limit 0.5

# 2. Check correlations with recent patterns
python global_correlation_analysis.py \
    --start $(date -d "30 days ago" +%Y-%m-%d)

# 3. Visual check of current patterns
python visualize_global_snowfall.py
```

### Automated Monitoring

Create `monitor_global_patterns.sh`:
```bash
#!/bin/bash
# Daily global pattern monitoring for Wisconsin prediction

echo "Fetching latest global snowfall data..."
python global_snowfall_fetcher.py --start $(date -d "7 days ago" +%Y-%m-%d)

echo "Analyzing current patterns..."
python global_correlation_analysis.py --start $(date -d "90 days ago" +%Y-%m-%d)

echo "✓ Global pattern monitoring complete"
```

```bash
# Add to cron for daily 6 AM runs
0 6 * * * cd /Users/kyle.jurgens/weather && ./monitor_global_patterns.sh
```

## 📊 Storage Requirements

| Component | Size (85 years) | Size (5 years) |
|-----------|-----------------|----------------|
| `global_snowfall.db` | ~2.0 GB | ~120 MB |
| Visualizations | ~50 MB | ~50 MB |
| Model files | ~1 MB | ~1 MB |
| **Total** | **~2.1 GB** | **~170 MB** |

## 🧪 Validation & Testing

### Cross-Validation

The system includes built-in validation:
- Statistical significance testing (p-values)
- Sample size requirements (minimum 30 overlapping days)
- Correlation stability across different time periods

### Historical Backtesting

```python
# Test model on historical data
from global_correlation_analysis import GlobalCorrelationAnalyzer

analyzer = GlobalCorrelationAnalyzer()

# Test 2020-2021 winter
results_2020 = analyzer.analyze_all_regions_vs_target(
    start_date="2020-10-01",
    end_date="2021-04-30"
)

# Compare to 2021-2022 winter
results_2021 = analyzer.analyze_all_regions_vs_target(
    start_date="2021-10-01",
    end_date="2022-04-30"
)

# Check if top predictors remain consistent
```

## 🔍 Advanced Queries

### SQL Queries on Global Database

```sql
-- Find days when Russia AND Japan both had extreme snow
SELECT
    r.date,
    r.total_snowfall_mm AS russia_snow,
    j.total_snowfall_mm AS japan_snow
FROM
    (SELECT date, SUM(snowfall_mm) AS total_snowfall_mm
     FROM snowfall_daily d
     JOIN stations s ON d.station_id = s.station_id
     WHERE s.region = 'siberia_central'
     GROUP BY date) r
JOIN
    (SELECT date, SUM(snowfall_mm) AS total_snowfall_mm
     FROM snowfall_daily d
     JOIN stations s ON d.station_id = s.station_id
     WHERE s.region = 'japan_north'
     GROUP BY date) j
ON r.date = j.date
WHERE r.total_snowfall_mm > 50 AND j.total_snowfall_mm > 50
ORDER BY r.date DESC;

-- Find Wisconsin snowfall 5-10 days after Siberian extreme events
SELECT
    w.date AS wisconsin_date,
    w.snowfall_mm AS wisconsin_snow,
    s.date AS siberia_date,
    s.total_snowfall_mm AS siberia_snow,
    julianday(w.date) - julianday(s.date) AS lag_days
FROM
    (SELECT date, SUM(snowfall_mm) AS total_snowfall_mm
     FROM snowfall_daily d
     JOIN stations s ON d.station_id = s.station_id
     WHERE s.region = 'siberia_central' AND snowfall_mm > 30
     GROUP BY date) s
JOIN
    snowfall_daily w ON w.station_id = 'Phelps_WI_USA'
WHERE
    julianday(w.date) - julianday(s.date) BETWEEN 5 AND 10
    AND w.snowfall_mm > 20
ORDER BY w.date DESC;
```

## 🌟 Future Enhancements

### Potential Additions

1. **Real-time forecasting**
   - Integrate with 7-day forecast APIs
   - Automatic pattern matching against historical analogues
   - Push notifications for high-probability events

2. **Machine Learning**
   - Train neural network on global patterns
   - Non-linear relationship detection
   - Multi-variable ensemble models

3. **Additional Indices**
   - Pacific Decadal Oscillation (PDO)
   - Arctic Oscillation (AO) index
   - North Atlantic Oscillation (NAO) index
   - Madden-Julian Oscillation (MJO)

4. **Extreme Event Prediction**
   - Focus on 95th percentile+ snowfall events
   - Pattern recognition for major storms
   - Analog year identification

5. **Mobile/Web Interface**
   - Dashboard showing current global patterns
   - Visual predictor status (traffic light system)
   - Automated forecast generation

## 📚 Additional Resources

### Related Files in This Project

- `january_2026_GLOBAL_teleconnections.py` - Existing global pattern analysis
- `visualize_enso_snowfall.py` - ENSO-snowfall correlation template
- `analyze_85year_trends.py` - Long-term trend analysis methods
- `duckdb_queries.py` - Advanced query templates

### Scientific Background

- **Teleconnections**: Wallace & Gutzler (1981) - Teleconnections in the Geopotential Height Field
- **Polar Vortex**: Baldwin & Dunkerton (2001) - Stratospheric Harbingers of Anomalous Weather
- **ENSO**: Trenberth (1997) - The Definition of El Niño
- **Pacific Patterns**: Mantua et al. (1997) - Pacific Decadal Oscillation

## 💡 Tips & Best Practices

1. **Data Collection**
   - Start with recent years (2000+) for testing (~1 hour)
   - Then extend to full history (1940+) when validated (~4 hours)
   - Use `--rate-limit 1.0` to be respectful to Open-Meteo API

2. **Analysis**
   - Focus on winter months (October-April) for snowfall analysis
   - Test multiple lag windows (7, 14, 30 days) to find optimal patterns
   - Validate correlations across different decades to ensure stability

3. **Interpretation**
   - Strong correlations (>0.3) are rare but extremely valuable
   - Consistent moderate correlations (0.15-0.3) across multiple regions = robust signal
   - Always check statistical significance (p-value < 0.05)

4. **Operational Use**
   - Update database weekly during winter season
   - Monitor top 5 predictor regions daily
   - Combine multiple predictor signals for confidence

## 🐛 Troubleshooting

### Common Issues

**"Database locked" error**
```bash
# Close all connections, then:
sqlite3 global_snowfall.db "PRAGMA integrity_check;"
```

**API rate limiting**
```bash
# Increase delay between calls
python global_snowfall_fetcher.py --rate-limit 2.0
```

**Missing correlations**
```bash
# Ensure sufficient overlapping data
python global_correlation_analysis.py --start 1940-01-01
```

**Visualization errors**
```bash
# Install visualization dependencies
pip install matplotlib seaborn scipy pandas
```

## 📞 Contact & Support

This system was built for comprehensive global snowfall prediction focused on Phelps and Land O'Lakes, Wisconsin. For questions or issues:

1. Check this README
2. Review individual script help: `python script_name.py --help`
3. Examine output logs for error details
4. Validate database integrity with `--summary` commands

---

**Built with:**
- Open-Meteo Historical Weather API
- Python 3.x
- SQLite3
- Pandas, NumPy, SciPy
- Matplotlib, Seaborn

**Version:** 1.0
**Last Updated:** 2026-01-05
