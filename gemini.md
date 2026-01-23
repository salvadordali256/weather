# Gemini Weather Scraper

This document outlines the plan for using the existing weather data collection system to scrape weather data from NOAA and Open-Meteo.

## Project Overview

The project is a comprehensive system for collecting, storing, and managing historical weather data. It consists of the following key components:

*   `weather_orchestrator.py`: The main script for orchestrating data collection from multiple sources.
*   `noaa_weather_fetcher.py`: A fetcher for the NOAA NCEI API.
*   `openmeteo_weather_fetcher.py`: A fetcher for the Open-Meteo API.
*   `weather_requirements.txt`: A list of project dependencies.
*   `WEATHER_README.md`: A detailed guide to the project.

## API Capabilities

### NOAA NCEI

*   **Coverage:** US-focused, station-based
*   **Time Range:** 1763 - present
*   **Resolution:** Daily, monthly, yearly
*   **Rate Limit:** 5 req/sec, 10K req/day
*   **Cost:** Free with API token

### Open-Meteo

*   **Coverage:** Global (any coordinate)
*   **Time Range:** 1940 - present
*   **Resolution:** Hourly, daily
*   **Rate Limit:** Generous, no strict limits
*   **Cost:** Free, no API key needed

## Plan

1.  **Installation:** Install the required dependencies from `weather_requirements.txt`.
2.  **Configuration:**
    *   Obtain a NOAA API token (optional but recommended).
    *   Define a mandatory NOAA User-Agent string (e.g., `"(salvadordali256.net, kyle@salvadordali256.net)"`).
    *   Create a `.env` file with the token, user agent, and storage path.
3.  **Execution:**
    *   Use the `weather_orchestrator.py` to fetch data.
    *   For US data, use the `fetch_us_comprehensive` method.
    *   For global data, use the `fetch_global_grid` or `fetch_major_cities` methods.
4.  **Data Storage:**
    *   Data will be stored in Parquet format with Snappy compression.
    *   Data will be partitioned by year for efficient querying.

## Example Usage

### Download California Weather
```python
from weather_orchestrator import WeatherDataOrchestrator

orchestrator = WeatherDataOrchestrator(
    noaa_api_token="YOUR_TOKEN",
    storage_path="/storage/path",
    noaa_user_agent="(salvadordali256.net, kyle@salvadordali256.net)"
)

orchestrator.fetch_us_comprehensive(
    states=["06"],  # California
    start_date="2000-01-01",
    max_stations_per_state=50
)
```

### Query Data with DuckDB
```python
import duckdb

result = duckdb.query("""
    SELECT date, AVG(TMAX) as avg_temp
    FROM '/storage/path/noaa/**/data.parquet'
    WHERE year >= 2020
    GROUP BY date
""").to_df()
```
