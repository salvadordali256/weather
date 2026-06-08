# Graph Report - .  (2026-06-06)

## Corpus Check
- Corpus is ~24,270 words - fits in a single context window. You may not need a graph.

## Summary
- 410 nodes · 654 edges · 23 communities (22 shown, 1 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 12 edges (avg confidence: 0.65)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Snowfall DuckDB Storage|Snowfall DuckDB Storage]]
- [[_COMMUNITY_Seasonal Snowfall Analysis|Seasonal Snowfall Analysis]]
- [[_COMMUNITY_DuckDB Query Layer|DuckDB Query Layer]]
- [[_COMMUNITY_Open-Meteo Weather Fetcher|Open-Meteo Weather Fetcher]]
- [[_COMMUNITY_Comprehensive Forecast Engine|Comprehensive Forecast Engine]]
- [[_COMMUNITY_Global Correlation Analysis|Global Correlation Analysis]]
- [[_COMMUNITY_NOAA Weather Fetcher|NOAA Weather Fetcher]]
- [[_COMMUNITY_GFS Atmospheric Patterns|GFS Atmospheric Patterns]]
- [[_COMMUNITY_Regional Ensemble Forecasting|Regional Ensemble Forecasting]]
- [[_COMMUNITY_Global Snowfall Collector|Global Snowfall Collector]]
- [[_COMMUNITY_Jet Stream Analyzer|Jet Stream Analyzer]]
- [[_COMMUNITY_Weather Data Orchestrator|Weather Data Orchestrator]]
- [[_COMMUNITY_Integrated Forecast System|Integrated Forecast System]]
- [[_COMMUNITY_Pattern Matching Forecast|Pattern Matching Forecast]]
- [[_COMMUNITY_Local Event Detection|Local Event Detection]]
- [[_COMMUNITY_Major Event Predictor|Major Event Predictor]]
- [[_COMMUNITY_Configuration Management|Configuration Management]]
- [[_COMMUNITY_Package Entry Point|Package Entry Point]]

## God Nodes (most connected - your core abstractions)
1. `SnowfallAnalyzer` - 20 edges
2. `datetime` - 20 edges
3. `SnowfallDuckDB` - 20 edges
4. `OpenMeteoWeatherFetcher` - 17 edges
5. `WeatherQueryEngine` - 17 edges
6. `DataFrame` - 14 edges
7. `GFSAtmosphericFetcher` - 14 edges
8. `NOAAWeatherFetcher` - 14 edges
9. `GlobalCorrelationAnalyzer` - 13 edges
10. `ComprehensiveForecastSystem` - 13 edges

## Surprising Connections (you probably didn't know these)
- `ComprehensiveForecastSystem` --uses--> `LocalEventDetector`  [INFERRED]
  src/snowforecast/engines/comprehensive_forecast_system.py → src/snowforecast/analysis/local_event_analyzer.py
- `ComprehensiveForecastSystem` --uses--> `IntegratedForecastSystem`  [INFERRED]
  src/snowforecast/engines/comprehensive_forecast_system.py → src/snowforecast/engines/integrated_forecast_system.py
- `EnhancedForecastSystem` --uses--> `GFSAtmosphericFetcher`  [INFERRED]
  src/snowforecast/engines/enhanced_forecast_system.py → src/snowforecast/fetchers/gfs_atmospheric_fetcher.py
- `WeatherDataOrchestrator` --uses--> `NOAAWeatherFetcher`  [INFERRED]
  src/snowforecast/engines/weather_orchestrator.py → src/snowforecast/fetchers/noaa_weather_fetcher.py
- `WeatherDataOrchestrator` --uses--> `OpenMeteoWeatherFetcher`  [INFERRED]
  src/snowforecast/engines/weather_orchestrator.py → src/snowforecast/fetchers/openmeteo_weather_fetcher.py

## Import Cycles
- 1-file cycle: `src/snowforecast/engines/enhanced_regional_forecast_system.py -> src/snowforecast/engines/enhanced_regional_forecast_system.py`

## Communities (23 total, 1 thin omitted)

### Community 0 - "Snowfall DuckDB Storage"
Cohesion: 0.09
Nodes (23): DataFrame, main(), DuckDB Snowfall Analysis Engine ================================  High-performan, Total snowfall by year - fast time series aggregation          DuckDB uses paral, Aggregate snowfall by state/province          Uses DuckDB's hash aggregation for, Calculate rolling N-year average snowfall          Uses DuckDB window functions, Calculate percentiles for each station          Uses DuckDB's advanced statistic, Calculate year-over-year snowfall changes          Uses LAG window function for (+15 more)

### Community 1 - "Seasonal Snowfall Analysis"
Cohesion: 0.10
Nodes (22): main(), Snowfall Data Analysis Tools =============================  Analyze 100 years of, Get annual snowfall totals over time          Useful for identifying climate cha, Get average snowfall by month across all years          Shows seasonal patterns, Compare snowfall across decades          Useful for climate change analysis, Analyze snowfall data from SQLite database, Find the biggest single-day snowfall events          Args:             limit: Nu, Find the deepest snow depth measurements          Args:             limit: Numbe (+14 more)

### Community 2 - "DuckDB Query Layer"
Cohesion: 0.12
Nodes (20): DataFrame, main(), DuckDB Query Examples for Weather Data =======================================, Get total precipitation by month for a specific year          Example output:, Get temperature extremes across all data          Example output:             me, Calculate climate normals (30-year averages)          Climate normals are 30-yea, Calculate temperature anomalies compared to baseline period          Anomalies s, Query engine for weather data using DuckDB (+12 more)

### Community 3 - "Open-Meteo Weather Fetcher"
Cohesion: 0.11
Nodes (17): main(), OpenMeteoWeatherFetcher, Open-Meteo Weather Data Fetcher Fetches historical weather data from Open-Meteo, Fetch large date ranges by chunking into smaller requests                  Args:, Fetch data for multiple locations in parallel                  Args:, Fetches weather data from Open-Meteo Historical Weather API     Historical data, Create a grid of coordinate points for a bounding box                  Args:, Save data to local storage with efficient formats                  Args: (+9 more)

### Community 4 - "Comprehensive Forecast Engine"
Cohesion: 0.10
Nodes (16): ComprehensiveForecastSystem, main(), Determine what type of event this is, Combine all models into final forecast, Main forecast generation - runs everything, Ultimate forecasting system - combines all detection methods, Run global-scale prediction models, Run local/regional detection models (+8 more)

### Community 5 - "Global Correlation Analysis"
Cohesion: 0.12
Nodes (14): GlobalCorrelationAnalyzer, main(), Calculate correlation between two timeseries at various lag intervals          A, Analyze correlation between two regions at various lag intervals          Args:, Analyze all regions against target region (Phelps/Land O'Lakes area)          Ar, Analyze global snowfall correlations and teleconnections, Analyze if extreme snow events in region_a predict events in region_b          A, Get aggregated daily snowfall timeseries for a region          Args: (+6 more)

### Community 6 - "NOAA Weather Fetcher"
Cohesion: 0.14
Nodes (14): main(), NOAAWeatherFetcher, NOAA NCEI Weather Data Fetcher Fetches historical weather data from NOAA and sto, Fetch daily weather data for a station                  Args:             statio, Fetch data using newer NCEI Data Service API (more efficient for bulk downloads), Fetches weather data from NOAA NCEI API     Rate limits: 5 requests/second, 10,0, Fetch large date ranges by chunking into smaller requests                  Args:, Save data to local storage with efficient formats                  Args: (+6 more)

### Community 7 - "GFS Atmospheric Patterns"
Cohesion: 0.13
Nodes (12): GFSAtmosphericFetcher, main(), Fetch atmospheric forecast data for key locations          Args:             hou, Store atmospheric data in database, Detect Alberta Clipper formation indicators          Signature:         - Low pr, Fetch atmospheric data from Open-Meteo's GFS forecast API, Detect lake effect snow setup          Signature:         - Strong NW winds cros, Analyze atmospheric data to detect patterns          Returns:             List o (+4 more)

### Community 8 - "Regional Ensemble Forecasting"
Cohesion: 0.16
Nodes (12): datetime, EnhancedRegionalForecastSystem, main(), Categorize snow amount and return activity level         Returns: (category, act, Check global predictor signals (long-range forecast), Check regional predictor signals (short-range forecast), Generate combined forecast from global + regional predictors, Enhanced forecast system combining global teleconnections with regional predicto (+4 more)

### Community 9 - "Global Snowfall Collector"
Cohesion: 0.14
Nodes (11): GlobalSnowfallFetcher, main(), Fetch and store global snowfall data using Open-Meteo Historical Weather API, Initialize SQLite database with schema for global snowfall data, Register all global locations in the database, Fetch historical weather data from Open-Meteo for a single location          Arg, Store fetched data in database, Collect historical data for all registered locations          Args: (+3 more)

### Community 10 - "Jet Stream Analyzer"
Cohesion: 0.15
Nodes (10): JetStreamAnalyzer, main(), Parse meteorologist discussion for jet stream info, Analyze jet stream patterns for Wisconsin snowfall potential, Manual pattern analysis when AFD not available, Identify jet stream patterns that preceded major Wisconsin snow events         T, Combine jet stream analysis with our existing snow predictor signals          Lo, Fetch GFS model wind data at specified pressure level          Note: This is a s (+2 more)

### Community 11 - "Weather Data Orchestrator"
Cohesion: 0.16
Nodes (11): main(), Save progress to file, Fetch comprehensive US weather data using NOAA                  Args:, Fetch global weather data on a grid using Open-Meteo                  Args:, Fetch weather data for major world cities using Open-Meteo                  Args, Generate a comprehensive report of all collected data, Unified orchestrator for weather data collection from multiple sources, Example orchestration workflows (+3 more)

### Community 12 - "Integrated Forecast System"
Cohesion: 0.18
Nodes (9): IntegratedForecastSystem, main(), Run correlation-based global predictor model, Simplified jet stream analysis, Combine all models with weights and cross-checks, Multi-model ensemble forecast with false positive filtering, Main forecast generation, Get recent snowfall for a station (+1 more)

### Community 13 - "Pattern Matching Forecast"
Cohesion: 0.18
Nodes (9): main(), PatternMatchingForecast, Generate forecasts by matching current conditions to historical patterns, For each analog date, get what happened in Wisconsin         in the following 7, Generate forecast based on historical outcomes, Main forecast generation, Get current snowfall conditions at all predictor stations, Categorize snowfall amount (+1 more)

### Community 14 - "Local Event Detection"
Cohesion: 0.21
Nodes (8): LocalEventDetector, main(), Check for Alberta Clipper indicators, Check for lake effect snow indicators, Check for regional low pressure systems, Detect local/regional snow events without global precursors, Run all local event detection methods, Get snowfall for a station

### Community 15 - "Major Event Predictor"
Cohesion: 0.19
Nodes (8): main(), MajorEventPredictor, Analyze all major events to find common precursor patterns, Check if current conditions match historical precursor patterns, Predict major snow events by pattern matching precursor conditions, Main analysis pipeline, Find all major Wisconsin snow events, For a given event, analyze what global conditions existed 1-7 days before

### Community 16 - "Configuration Management"
Cohesion: 0.18
Nodes (10): load_env_file(), main(), Configuration Management ========================  Handles loading configuration, Validate configuration, String representation, Load environment variables from .env file, Test configuration loading, Weather data fetcher configuration (+2 more)

## Knowledge Gaps
- **1 isolated node(s):** `Series`
  These have ≤1 connection - possible missing edges or undocumented components.
- **1 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `datetime` connect `Regional Ensemble Forecasting` to `Open-Meteo Weather Fetcher`, `Comprehensive Forecast Engine`, `Global Correlation Analysis`, `NOAA Weather Fetcher`, `GFS Atmospheric Patterns`, `Global Snowfall Collector`, `Jet Stream Analyzer`, `Integrated Forecast System`, `Pattern Matching Forecast`, `Local Event Detection`, `Major Event Predictor`, `Configuration Management`?**
  _High betweenness centrality (0.401) - this node is a cross-community bridge._
- **Why does `OpenMeteoWeatherFetcher` connect `Open-Meteo Weather Fetcher` to `Weather Data Orchestrator`?**
  _High betweenness centrality (0.069) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `OpenMeteoWeatherFetcher` (e.g. with `WeatherDataOrchestrator` and `.__init__()`) actually correct?**
  _`OpenMeteoWeatherFetcher` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `snowforecast — Wisconsin snowfall forecast system.`, `Series`, `Analyze global snowfall correlations and teleconnections` to the rest of the system?**
  _181 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Snowfall DuckDB Storage` be split into smaller, more focused modules?**
  _Cohesion score 0.08536585365853659 - nodes in this community are weakly interconnected._
- **Should `Seasonal Snowfall Analysis` be split into smaller, more focused modules?**
  _Cohesion score 0.09743589743589744 - nodes in this community are weakly interconnected._
- **Should `DuckDB Query Layer` be split into smaller, more focused modules?**
  _Cohesion score 0.11932773109243698 - nodes in this community are weakly interconnected._