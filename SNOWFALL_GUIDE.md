#❄️ Snowfall Data Collection Guide
## 100 Years of US & Canada Snowfall Data

---

## 🎯 Project Overview

This guide helps you collect and analyze **100+ years** of snowfall data across the **United States and Canada**.

### What You'll Get:
- **Daily snowfall measurements** (mm)
- **Snow depth readings** (mm)
- **Temperature data** alongside snow events
- **Station metadata** (location, elevation, etc.)
- **100+ years** of historical data (1920-present)
- **Thousands of stations** across US and Canada

---

## 📊 Data Sources

### NOAA NCEI (United States)
- **Coverage:** 50 US states + territories
- **Stations:** ~10,000+ with snowfall data
- **Time Range:** 1920-present (some stations go back to 1800s)
- **Parameters:** SNOW (daily snowfall), SNWD (snow depth)
- **Quality:** High - official government records

### Open-Meteo (Canada)
- **Coverage:** 15+ major Canadian cities
- **Time Range:** 1940-present
- **Parameters:** Snowfall, snow depth, temperature
- **Quality:** Good - reanalysis data

---

## 💾 Storage Requirements

### Database Size Estimates

| Coverage | Stations | Years | Database Size | Storage Recommendation |
|----------|----------|-------|---------------|------------------------|
| **Test** (3 states) | ~150 | 100 | **500 MB - 1 GB** | Any modern drive |
| **Regional** (10 states) | ~500 | 100 | **2-4 GB** | Any modern drive |
| **Full US** (all states) | ~5,000 | 100 | **15-25 GB** | External HDD/SSD |
| **US + Canada** (complete) | ~5,500 | 100 | **18-30 GB** | External HDD/SSD |

### File Breakdown:
- **SQLite Database:** Main storage (~90% of total)
- **Progress Files:** Tracking (~10 MB)
- **CSV Exports:** Optional (~20% of DB size)
- **Temp Files:** Auto-deleted (~500 MB peak)

**Recommended:** 100 GB drive for comfort + future expansion

---

## 🚀 Quick Start

### Step 1: Install Dependencies

```bash
cd /Users/kyle.jurgens/weather
pip install -r weather_requirements.txt
pip install sqlite3  # Usually included with Python
```

### Step 2: Get NOAA API Token

1. Visit: https://www.ncdc.noaa.gov/cdo-web/token
2. Enter your email
3. Check email for token (arrives in minutes)
4. Save token - you'll need it!

### Step 3: Configure Storage Path

Edit `snowfall_collector.py` and set your network storage path:

```python
# Line 758 - Update this path
DB_PATH = "/Volumes/YourNetworkDrive/snowfall_data.db"
```

Or use local storage:
```python
DB_PATH = "./snowfall_data.db"  # Current directory
```

### Step 4: Run Collection

#### Option A: Test Run (3 states, fast)
```python
# Edit snowfall_collector.py, line 763:
collector.collect_us_snowfall(
    states=["08", "30", "56"],  # CO, MT, WY - snowy states
    start_date="2010-01-01",     # Just last 15 years for testing
    max_stations_per_state=10    # Limit stations
)
```

Run:
```bash
python snowfall_collector.py
```

**Expected:** ~200 MB, ~30 minutes

#### Option B: Full US Collection
```python
# Edit snowfall_collector.py, line 763:
collector.collect_us_snowfall(
    states=None,  # All states
    start_date="1920-01-01",
    max_stations_per_state=100
)
```

**Expected:** ~20-25 GB, several days

#### Option C: Full US + Canada
```python
# Collect US (as above), then add:
collector.collect_canada_snowfall(start_date="1940-01-01")
```

**Expected:** ~25-30 GB total

---

## 📈 Data Analysis

### Using the Analysis Tools

```python
from snowfall_analysis import SnowfallAnalyzer

# Initialize
analyzer = SnowfallAnalyzer("./snowfall_data.db")

# Generate comprehensive report
analyzer.generate_comprehensive_report()

# Top snowiest stations
top_20 = analyzer.get_top_snowiest_stations(20)
print(top_20)

# Biggest snowstorms in history
storms = analyzer.get_biggest_snowstorms(100)
print(storms)

# Annual trends (climate change analysis)
trends = analyzer.get_annual_snowfall_trend(1920, 2024)
print(trends)

# Compare US vs Canada
comparison = analyzer.compare_us_canada()
print(comparison)
```

### SQL Queries

The data is in SQLite - you can query it directly:

```sql
-- Top 10 snowiest days ever
SELECT date, name, state, snowfall_mm / 10.0 as snowfall_cm
FROM snowfall_daily sd
JOIN stations s ON sd.station_id = s.station_id
WHERE snowfall_mm > 0
ORDER BY snowfall_mm DESC
LIMIT 10;

-- Average snowfall by state
SELECT state, AVG(avg_annual_snowfall_mm) / 1000.0 as avg_meters
FROM station_summaries ss
JOIN stations s ON ss.station_id = s.station_id
WHERE country = 'USA'
GROUP BY state
ORDER BY avg_meters DESC;

-- Snowfall trends over time
SELECT
    CAST(strftime('%Y', date) AS INTEGER) as year,
    SUM(snowfall_mm) / 1000.0 as total_meters
FROM snowfall_daily
GROUP BY year
ORDER BY year;
```

---

## 🗄️ Database Schema

### Tables

#### `stations`
- **station_id**: Unique identifier (e.g., "GHCND:USC00050848")
- **name**: Station name
- **latitude, longitude**: Geographic coordinates
- **elevation**: Meters above sea level
- **state, country**: Location
- **min_date, max_date**: Data availability
- **data_source**: NOAA or Open-Meteo

#### `snowfall_daily`
- **station_id**: Links to stations table
- **date**: YYYY-MM-DD
- **snowfall_mm**: Daily snowfall (millimeters)
- **snow_depth_mm**: Snow on ground (millimeters)
- **temp_max_celsius, temp_min_celsius**: Daily temperatures
- **precipitation_mm**: Total precipitation

#### `station_summaries`
- **station_id**: Links to stations table
- **total_snowfall_mm**: Lifetime total
- **avg_annual_snowfall_mm**: Average per year
- **max_daily_snowfall_mm**: Biggest single day
- **days_with_snow**: Count of snowy days
- **total_years**: Years of data

---

## 🔍 Example Queries & Analyses

### Find Your Local Station

```python
analyzer = SnowfallAnalyzer("./snowfall_data.db")

# Search by name
denver_data = analyzer.query_station("%Denver%")
print(denver_data)
```

### Compare Decades

```python
# See if snowfall is decreasing (climate change)
decades = analyzer.get_decade_comparison()
print(decades)
```

### Find Record Events

```python
# Biggest blizzards
storms = analyzer.get_biggest_snowstorms(50)

# Deepest snow ever
deepest = analyzer.get_deepest_snow(50)

# Longest snow seasons
long_seasons = analyzer.get_longest_snow_seasons()
```

### Regional Analysis

```python
# Colorado snowfall 2000-2024
co_data = analyzer.query_region("Colorado", 2000, 2024)

# State-by-state comparison
states = analyzer.get_snowfall_by_state()
```

---

## ⏱️ Collection Time Estimates

| Scope | Stations | Time (100 Mbps) | Time (Slow Connection) |
|-------|----------|-----------------|------------------------|
| Test (3 states, 15 years) | ~150 | 30-60 min | 1-2 hours |
| Regional (10 states) | ~500 | 2-4 hours | 6-8 hours |
| Full US (100 years) | ~5,000 | 2-4 days | 5-7 days |
| US + Canada | ~5,500 | 3-5 days | 6-8 days |

**Note:** NOAA rate limits (5 req/sec) are the main bottleneck, not your connection speed.

###💡 Tips to Speed Up:
1. **Run overnight** - Long collections take days
2. **Use progress tracking** - Can resume if interrupted
3. **Start small** - Test with a few states first
4. **Parallel friendly** - Can run multiple state collections separately

---

## 🎯 Pre-Configured Scenarios

### Scenario 1: Mountain States (Ski Resorts)
```python
states = ["08", "30", "49", "56", "06", "41", "53"]
# Colorado, Montana, Utah, Wyoming, California, Oregon, Washington
```
**Storage:** ~3-5 GB
**Time:** 8-12 hours
**Use Case:** Ski industry analysis, mountain climate

### Scenario 2: Great Lakes (Lake Effect Snow)
```python
states = ["26", "36", "39", "42", "55"]
# Michigan, New York, Ohio, Pennsylvania, Wisconsin
```
**Storage:** ~2-4 GB
**Time:** 6-10 hours
**Use Case:** Lake effect snow research

### Scenario 3: Northeast Corridor
```python
states = ["09", "23", "25", "33", "34", "36", "42", "44", "50"]
# CT, ME, MA, NH, NJ, NY, PA, RI, VT
```
**Storage:** ~3-5 GB
**Time:** 8-12 hours
**Use Case:** Population center snow analysis

### Scenario 4: Complete North America
```python
states = None  # All US states
# Plus Canadian cities
```
**Storage:** ~25-30 GB
**Time:** 3-5 days
**Use Case:** Continental climate research

---

## 📦 Export Options

### CSV Export
```python
collector.db.export_to_csv("./snowfall_exports")
```

Creates:
- `stations.csv` - All station info
- `station_summaries.csv` - Summary statistics

### SQLite to Other Formats

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect("snowfall_data.db")

# Export specific query to CSV
df = pd.read_sql("SELECT * FROM snowfall_daily WHERE state = 'Colorado'", conn)
df.to_csv("colorado_snowfall.csv", index=False)

# Export to Excel
df.to_excel("colorado_snowfall.xlsx", index=False)

# Export to Parquet (for big data tools)
df.to_parquet("colorado_snowfall.parquet")
```

---

## 🐛 Troubleshooting

### "NOAA_API_TOKEN not set"
- Make sure you edited line 756 in `snowfall_collector.py`
- Replace `YOUR_NOAA_TOKEN_HERE` with your actual token

### "No snowfall stations found"
- Some southern states have very few snowfall stations
- Try a northern/mountain state for testing

### Database locked error
- Only one process can write at a time
- Wait for current collection to finish
- Or use different database paths for parallel collections

### Slow downloads
- Normal! NOAA limits to 5 requests/second
- Expect ~200-400 stations per hour
- Use progress tracking - you can stop and resume

### Out of disk space
- Check available space on your network drive
- Delete temp files: `rm -rf temp_snowfall/`
- Use smaller date range for testing

---

## 💻 Network Storage Setup

### macOS
```bash
# Mount network drive
# In Finder: Go > Connect to Server > smb://your.network.drive

# Set path in script:
DB_PATH = "/Volumes/NetworkDrive/snowfall_data.db"
```

### Windows
```bash
# Map network drive (Z:)
# In script:
DB_PATH = "Z:/snowfall_data.db"
```

### Linux
```bash
# Mount NFS/SMB share
sudo mount -t cifs //server/share /mnt/network

# In script:
DB_PATH = "/mnt/network/snowfall_data.db"
```

---

## 📚 Additional Resources

### NOAA Documentation
- **API Docs:** https://www.ncdc.noaa.gov/cdo-web/webservices/v2
- **Data Info:** https://www.ncdc.noaa.gov/cdo-web/datasets
- **Station Search:** https://www.ncdc.noaa.gov/cdo-web/search

### Data Dictionary
- **SNOW:** Daily snowfall (tenths of mm)
- **SNWD:** Snow depth on ground (tenths of mm)
- **TMAX/TMIN:** Temperature max/min (tenths of °C)
- **PRCP:** Precipitation (tenths of mm)

### Conversions
- **1 cm = 10 mm**
- **1 meter = 1000 mm**
- **1 inch = 25.4 mm**
- **Celsius to Fahrenheit:** (C × 9/5) + 32

---

## 🎓 Research Ideas

1. **Climate Change Analysis**
   - Compare 1920-1960 vs 1980-2020
   - Identify warming trends
   - Map snowfall decrease areas

2. **Extreme Events**
   - Catalog biggest blizzards
   - Frequency of record snowfalls
   - Regional patterns

3. **Economic Impact**
   - Correlate with ski resort revenues
   - Road maintenance costs
   - Agriculture (snow cover for crops)

4. **Geographic Patterns**
   - Elevation effects
   - Lake effect regions
   - Mountain vs plains

5. **Seasonal Shifts**
   - Earlier/later first snow
   - Shortened snow season
   - Peak snowfall month changes

---

## ✅ Next Steps

1. ✅ Get NOAA API token
2. ✅ Set up network storage path
3. ✅ Run test collection (3 states)
4. ✅ Verify data looks good
5. ✅ Run full collection
6. ✅ Generate analysis reports
7. ✅ Export data as needed

---

## 🆘 Support

Created: 2025-01-16
For issues or questions, refer to:
- CODE_REVIEW_SUMMARY.md
- STORAGE_QUICK_REFERENCE.md
- weather_requirements.txt

**Happy snow data hunting!** ❄️
