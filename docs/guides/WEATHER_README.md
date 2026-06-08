# Northern Wisconsin Weather & Snowfall Analysis System

Complete weather data collection and analysis system using Open-Meteo and NOAA APIs with DuckDB analytics.

## 🚀 Quick Start

### Run the Simple Weather App (No setup needed!)

```bash
./venv/bin/python weather_app.py
```

This gives you an interactive menu to:
- Check current weather for any location
- Get 7-day forecasts
- View historical data (30, 90, or 365 days)
- No API key required!

### Quick Analysis of Existing Data

```bash
# 85-year climate trends (1940-2025):
./venv/bin/python analyze_85year_trends.py

# 1-year Northern Wisconsin analysis:
./venv/bin/python analyze_northwoods.py

# Recent 7-day snapshot:
./venv/bin/python analyze_7day_data.py
```

## 📊 What You Have

### Databases

| Database | Size | Records | Locations | Period |
|----------|------|---------|-----------|--------|
| `northwoods_full_history.db` | 18.2 MB | 94,101 | 3 | 1940-2025 (85 years) |
| `northwoods_snowfall.db` | 704 KB | 3,294 | 9 | 2024-2025 (1 year) |
| `snowfall_7day.db` | 60 KB | 152 | 19 | Nov 9-16, 2025 |

### Key Locations (Northern Wisconsin)

✅ **Full 85-year data:**
- Eagle River, WI
- Land O'Lakes, WI
- Phelps, WI

⏳ **Pending (rate limited, can collect later):**
- Iron River, WI
- Palmer, WI
- St. Germain, WI
- Boulder Junction, WI
- Mercer, WI
- Hurley, WI

## 📱 Applications & Tools

### 1. Weather App (`weather_app.py`)

Simple interactive application for any location:
```bash
./venv/bin/python weather_app.py
```

Features:
- Current weather conditions
- 7-day forecast
- Historical data (30, 90, or 365 days)
- Works worldwide, no API key needed!

### 2. Data Collection Tools

#### Collect 7-Day Recent Data
```bash
./venv/bin/python collect_7days_openmeteo.py
```
Quick snapshot of recent conditions for major ski areas.

#### Collect Northern Wisconsin Data (1-10 years)
```bash
./venv/bin/python collect_northwoods_wi.py
```
Interactive collection - you choose how many years.

#### Collect Full Historical Data (85 years: 1940-2025)
```bash
./venv/bin/python collect_northwoods_full_history.py
```
Maximum available data from Open-Meteo archive.

### 3. Analysis Scripts

#### 85-Year Climate Trends
```bash
./venv/bin/python analyze_85year_trends.py
```

Shows:
- Location summaries (85 years)
- Decade-by-decade trends (1940s-2020s)
- Climate change comparison (1940-1979 vs 1990-2025)
- Top 20 biggest snowstorms in history
- Record years (snowiest and least snowy)

#### Northern Wisconsin Analysis
```bash
./venv/bin/python analyze_northwoods.py
```

Shows:
- Total snowfall by location
- Monthly snowfall patterns
- Snowiest winters (top 10)
- Recent 7-day snowfall
- Record single-day snowfalls
- Snowfall rankings
- Snow season characteristics

## 🔑 API Access

### Open-Meteo (Recommended)
- ✅ **No API key required**
- ✅ Free unlimited access (reasonable use)
- ✅ Historical data: **1940-present** (85 years!)
- ✅ Current weather + forecasts
- 🌐 Website: https://open-meteo.com

### NOAA (Optional)
- 📧 Email: kyle@salvadordali256.net
- 🔑 Token: `dTIloUkePZGxmwosKdjYELmXTjXqKUHk`
- ⚠️ Limited historical data (1998-2025 for this region)
- ⚠️ Open-Meteo has MORE history for Northern Wisconsin!

To check NOAA coverage:
```bash
./venv/bin/python check_noaa_coverage.py
```

## 📈 Key Findings (85-Year Analysis)

### Average Annual Snowfall
- **Land O'Lakes**: 156.5 cm/year (61.6 inches)
- **Eagle River**: 137.3 cm/year (54.1 inches)  
- **Phelps**: 137.1 cm/year (54.0 inches)

### Climate Change Trend
Comparing Historical (1940-1979) vs Recent (1990-2025):
- **Change**: -5.5 cm (-3.8% decrease)
- Historical average: 144.8 cm/year
- Recent average: 139.3 cm/year

### Record Years
- **Snowiest year**: 1950 (226.0 cm average)
- **Least snowy**: 2010 (83.1 cm average)

### Biggest Storm
- **26.5 cm** at Land O'Lakes on **March 4, 1979**

## 🛠️ Technical Details

### Requirements
```bash
pip install -r weather_requirements.txt
```

Includes:
- `duckdb >= 1.4.2` - Fast analytical queries
- `pandas` - Data manipulation
- `requests` - API calls
- `python-dotenv` - Configuration

### Database Schema

All databases use the same structure:

```sql
-- Stations/locations
CREATE TABLE stations (
    station_id TEXT PRIMARY KEY,
    name TEXT,
    latitude REAL,
    longitude REAL,
    elevation REAL,
    state TEXT,
    country TEXT,
    min_date TEXT,
    max_date TEXT,
    data_source TEXT
);

-- Daily measurements
CREATE TABLE snowfall_daily (
    id INTEGER PRIMARY KEY,
    station_id TEXT,
    date TEXT,
    snowfall_mm REAL,
    snow_depth_mm REAL,
    temp_max_celsius REAL,
    temp_min_celsius REAL,
    precipitation_mm REAL,
    data_quality TEXT
);

-- Summary statistics
CREATE TABLE station_summaries (
    station_id TEXT PRIMARY KEY,
    total_years INTEGER,
    total_snowfall_mm REAL,
    avg_annual_snowfall_mm REAL,
    max_daily_snowfall_mm REAL,
    max_daily_snowfall_date TEXT,
    max_snow_depth_mm REAL,
    max_snow_depth_date TEXT,
    first_snow_date TEXT,
    last_snow_date TEXT,
    days_with_snow INTEGER
);
```

### DuckDB Integration

Use the `SnowfallDuckDB` class for high-performance analytics:

```python
from snowfall_duckdb import SnowfallDuckDB

# Open database
engine = SnowfallDuckDB("./northwoods_full_history.db")

# Run SQL queries
df = engine.query("""
    SELECT
        YEAR(CAST(date AS DATE)) as year,
        ROUND(SUM(snowfall_mm) / 10.0, 1) as total_cm
    FROM snowfall.snowfall_daily
    GROUP BY year
    ORDER BY total_cm DESC
    LIMIT 10
""")

print(df)
engine.close()
```

### Built-in Analysis Methods

```python
# Top snowiest stations
engine.top_snowiest_stations(limit=10)

# Snowfall by year
engine.snowfall_by_year(year=2024)

# Climate change analysis
engine.climate_change_analysis(
    period1_start=1940, period1_end=1979,
    period2_start=1990, period2_end=2025
)

# Multi-day storm tracking
engine.multi_day_storms(min_total_mm=500, max_days=7)
```

## 📁 File Structure

```
weather/
├── weather_app.py                        # ⭐ Simple weather application
├── collect_7days_openmeteo.py            # 7-day data collector
├── collect_northwoods_wi.py              # 1-10 year collector
├── collect_northwoods_full_history.py    # Full 85-year collector
├── analyze_7day_data.py                  # 7-day analysis
├── analyze_northwoods.py                 # 1-year analysis
├── analyze_85year_trends.py              # 85-year climate analysis
├── check_noaa_coverage.py                # Check NOAA data availability
├── snowfall_duckdb.py                    # DuckDB analysis engine
├── openmeteo_weather_fetcher.py          # Open-Meteo API client
├── noaa_weather_fetcher.py               # NOAA API client
├── weather_requirements.txt              # Python dependencies
├── northwoods_full_history.db            # 85 years of data (18.2 MB)
├── northwoods_snowfall.db                # 1 year of data (704 KB)
└── snowfall_7day.db                      # 7 days of data (60 KB)
```

## 🎯 Common Tasks

### Get Current Weather for Any Location

```bash
./venv/bin/python weather_app.py
# Enter coordinates when prompted
# Example: Eagle River, WI = 45.9169, -89.2443
```

### Analyze Specific Winter Season

```python
from snowfall_duckdb import SnowfallDuckDB

engine = SnowfallDuckDB("./northwoods_full_history.db")

# Get 1978-1979 winter (the big one!)
df = engine.query("""
    SELECT
        date,
        station_id,
        ROUND(snowfall_mm / 10.0, 1) as snow_cm
    FROM snowfall.snowfall_daily
    WHERE date BETWEEN '1978-11-01' AND '1979-04-30'
        AND snowfall_mm > 0
    ORDER BY snowfall_mm DESC
""")

print(df)
engine.close()
```

### Export Data to CSV

```python
from snowfall_duckdb import SnowfallDuckDB

engine = SnowfallDuckDB("./northwoods_full_history.db")

# Export all daily data
df = engine.query("SELECT * FROM snowfall.snowfall_daily")
df.to_csv("snowfall_data.csv", index=False)

engine.close()
```

### Find Biggest Storms by Decade

```python
from snowfall_duckdb import SnowfallDuckDB

engine = SnowfallDuckDB("./northwoods_full_history.db")

df = engine.query("""
    SELECT
        (YEAR(CAST(date AS DATE)) / 10) * 10 as decade,
        MAX(snowfall_mm) / 10.0 as max_snow_cm,
        MIN(date) FILTER (WHERE snowfall_mm = MAX(snowfall_mm)) as storm_date
    FROM snowfall.snowfall_daily
    GROUP BY decade
    ORDER BY decade
""")

print(df)
engine.close()
```

## 🚨 Troubleshooting

### Rate Limit Errors (429)
If you hit Open-Meteo rate limits:
1. Wait 1-2 hours
2. Re-run the collection script
3. It will skip already-collected locations

### Missing Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r weather_requirements.txt
```

### DuckDB Date Function Errors
Make sure you're using DuckDB 1.4.2+:
```bash
pip install --upgrade duckdb
```

## 📚 Documentation

- `DUCKDB_GUIDE.md` - Complete DuckDB integration guide
- `PROJECT_COMPLETE.md` - Project summary and setup
- `weather_requirements.txt` - Python dependencies

## 🌐 Resources

- **Open-Meteo**: https://open-meteo.com
- **NOAA Climate Data**: https://www.ncdc.noaa.gov/cdo-web/
- **DuckDB**: https://duckdb.org
- **Project GitHub**: (your repo here)

## 💡 Next Steps

1. **Collect remaining locations** (wait for rate limit reset)
2. **Export data** to Excel/CSV for visualization
3. **Create custom queries** for specific research questions
4. **Compare winters** (e.g., 1978-79 vs 2023-24)
5. **Analyze temperature trends** alongside snowfall

---

**Questions?** Refer to the script files - they all have detailed comments and docstrings!

**Enjoy exploring 85 years of Northern Wisconsin snowfall data! ❄️**
