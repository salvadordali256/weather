"""
DuckDB Snowfall Analysis Engine
================================

High-performance analytical queries on snowfall data using DuckDB.

DuckDB is much faster than SQLite for analytical queries:
- 10-100x faster for aggregations
- Can query Parquet files directly (no loading)
- Advanced SQL features (window functions, CTEs)
- Easy integration with Pandas

Usage:
    # Query SQLite database directly
    engine = SnowfallDuckDB("./snowfall_data.db")

    # Or query Parquet files
    engine = SnowfallDuckDB("./snowfall_parquet/*.parquet")

    # Run queries
    df = engine.top_snowiest_stations(20)
"""

import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, Union, List
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowfallDuckDB:
    """High-performance snowfall data analysis with DuckDB"""

    def __init__(self, data_source: str, use_parquet: bool = False):
        """
        Initialize DuckDB connection

        Args:
            data_source: Path to SQLite database or Parquet directory
            use_parquet: If True, queries Parquet files instead of SQLite
        """
        self.data_source = data_source
        self.use_parquet = use_parquet
        self.conn = duckdb.connect()  # In-memory connection

        # Configure DuckDB for performance
        self.conn.execute("SET memory_limit='4GB'")
        self.conn.execute("SET threads=4")

        if not use_parquet:
            # Attach SQLite database
            self._attach_sqlite()

        logger.info(f"DuckDB initialized with {'Parquet' if use_parquet else 'SQLite'} source")

    def _attach_sqlite(self):
        """Attach SQLite database to DuckDB"""
        try:
            self.conn.execute(f"ATTACH '{self.data_source}' AS snowfall (TYPE SQLITE)")
            logger.info(f"Attached SQLite database: {self.data_source}")
        except Exception as e:
            logger.error(f"Failed to attach SQLite database: {e}")
            raise

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        try:
            return self.conn.execute(sql).df()
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

    # ==========================================================================
    # Quick Aggregations (10-100x faster than SQLite)
    # ==========================================================================

    def top_snowiest_stations(self, limit: int = 20) -> pd.DataFrame:
        """
        Get top snowiest stations by total snowfall

        DuckDB is ~50x faster than SQLite for this aggregation
        """
        if self.use_parquet:
            sql = f"""
            SELECT
                station_id,
                name,
                state,
                country,
                ROUND(SUM(snowfall_mm) / 1000.0, 2) as total_snowfall_meters,
                ROUND(AVG(snowfall_mm), 2) as avg_daily_mm,
                COUNT(*) as record_count
            FROM read_parquet('{self.data_source}/snowfall_daily_*.parquet')
            WHERE snowfall_mm > 0
            GROUP BY station_id, name, state, country
            ORDER BY total_snowfall_meters DESC
            LIMIT {limit}
            """
        else:
            sql = f"""
            SELECT
                s.name,
                s.state,
                s.country,
                s.latitude,
                s.longitude,
                s.elevation,
                ROUND(ss.total_snowfall_mm / 1000.0, 2) as total_snowfall_meters,
                ROUND(ss.avg_annual_snowfall_mm / 1000.0, 2) as avg_annual_meters,
                ROUND(ss.max_daily_snowfall_mm / 10.0, 2) as max_daily_cm,
                ss.total_years
            FROM snowfall.station_summaries ss
            JOIN snowfall.stations s ON ss.station_id = s.station_id
            ORDER BY ss.total_snowfall_mm DESC
            LIMIT {limit}
            """
        return self.query(sql)

    def snowfall_by_year(self, start_year: int = 1920,
                         end_year: int = 2024) -> pd.DataFrame:
        """
        Total snowfall by year - fast time series aggregation

        DuckDB uses parallel processing for year-level aggregations
        """
        if self.use_parquet:
            sql = f"""
            SELECT
                YEAR(date) as year,
                COUNT(DISTINCT station_id) as station_count,
                ROUND(SUM(snowfall_mm) / 1000.0, 2) as total_snowfall_meters,
                ROUND(AVG(snowfall_mm), 2) as avg_daily_mm,
                SUM(CASE WHEN snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
                ROUND(MAX(snowfall_mm) / 10.0, 2) as max_daily_cm
            FROM read_parquet('{self.data_source}/snowfall_daily_*.parquet')
            WHERE YEAR(date) BETWEEN {start_year} AND {end_year}
            GROUP BY year
            ORDER BY year
            """
        else:
            sql = f"""
            SELECT
                CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
                COUNT(DISTINCT station_id) as station_count,
                ROUND(SUM(snowfall_mm) / 1000.0, 2) as total_snowfall_meters,
                ROUND(AVG(snowfall_mm), 2) as avg_daily_mm,
                SUM(CASE WHEN snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
                ROUND(MAX(snowfall_mm) / 10.0, 2) as max_daily_cm
            FROM snowfall.snowfall_daily
            WHERE CAST(YEAR(CAST(date AS DATE)) AS INTEGER) BETWEEN {start_year} AND {end_year}
            GROUP BY year
            ORDER BY year
            """
        return self.query(sql)

    def snowfall_by_state(self) -> pd.DataFrame:
        """
        Aggregate snowfall by state/province

        Uses DuckDB's hash aggregation for speed
        """
        sql = """
        SELECT
            s.state,
            s.country,
            COUNT(DISTINCT s.station_id) as station_count,
            ROUND(SUM(ss.total_snowfall_mm) / 1000.0, 2) as total_snowfall_meters,
            ROUND(AVG(ss.avg_annual_snowfall_mm) / 1000.0, 2) as avg_annual_meters,
            ROUND(MAX(ss.max_daily_snowfall_mm) / 10.0, 2) as record_daily_cm
        FROM snowfall.stations s
        JOIN snowfall.station_summaries ss ON s.station_id = ss.station_id
        GROUP BY s.state, s.country
        ORDER BY total_snowfall_meters DESC
        """
        return self.query(sql)

    # ==========================================================================
    # Advanced Window Functions (DuckDB specialty)
    # ==========================================================================

    def rolling_annual_snowfall(self, window_years: int = 10) -> pd.DataFrame:
        """
        Calculate rolling N-year average snowfall

        Uses DuckDB window functions - not easily done in SQLite
        """
        sql = f"""
        WITH yearly_totals AS (
            SELECT
                CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
                ROUND(SUM(snowfall_mm) / 1000.0, 2) as total_snowfall_meters
            FROM snowfall.snowfall_daily
            GROUP BY year
        )
        SELECT
            year,
            total_snowfall_meters,
            ROUND(AVG(total_snowfall_meters) OVER (
                ORDER BY year
                ROWS BETWEEN {window_years - 1} PRECEDING AND CURRENT ROW
            ), 2) as rolling_avg_{window_years}yr
        FROM yearly_totals
        ORDER BY year
        """
        return self.query(sql)

    def snowfall_percentiles_by_station(self) -> pd.DataFrame:
        """
        Calculate percentiles for each station

        Uses DuckDB's advanced statistical functions
        """
        sql = """
        SELECT
            s.name,
            s.state,
            COUNT(*) as total_days,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY sd.snowfall_mm), 2) as median_mm,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY sd.snowfall_mm), 2) as p75_mm,
            ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY sd.snowfall_mm), 2) as p90_mm,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sd.snowfall_mm), 2) as p95_mm,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY sd.snowfall_mm), 2) as p99_mm
        FROM snowfall.snowfall_daily sd
        JOIN snowfall.stations s ON sd.station_id = s.station_id
        WHERE sd.snowfall_mm > 0
        GROUP BY s.name, s.state, sd.station_id
        HAVING total_days > 100
        ORDER BY p99_mm DESC
        LIMIT 50
        """
        return self.query(sql)

    def year_over_year_changes(self) -> pd.DataFrame:
        """
        Calculate year-over-year snowfall changes

        Uses LAG window function for comparisons
        """
        sql = """
        WITH yearly_totals AS (
            SELECT
                CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
                ROUND(SUM(snowfall_mm) / 1000.0, 2) as total_meters
            FROM snowfall.snowfall_daily
            GROUP BY year
        )
        SELECT
            year,
            total_meters,
            LAG(total_meters, 1) OVER (ORDER BY year) as prev_year_meters,
            ROUND(total_meters - LAG(total_meters, 1) OVER (ORDER BY year), 2) as change_meters,
            ROUND(100.0 * (total_meters - LAG(total_meters, 1) OVER (ORDER BY year)) /
                  LAG(total_meters, 1) OVER (ORDER BY year), 2) as pct_change
        FROM yearly_totals
        WHERE year >= 1920
        ORDER BY year
        """
        return self.query(sql)

    # ==========================================================================
    # Complex Analytical Queries
    # ==========================================================================

    def climate_change_analysis(self,
                                baseline_start: int = 1950,
                                baseline_end: int = 1980,
                                recent_start: int = 1990,
                                recent_end: int = 2024) -> pd.DataFrame:
        """
        Compare recent snowfall to historical baseline

        Shows warming trends and snowfall decline
        """
        sql = f"""
        WITH baseline AS (
            SELECT
                'Baseline ({baseline_start}-{baseline_end})' as period,
                ROUND(AVG(yearly_total), 2) as avg_annual_meters,
                ROUND(STDDEV(yearly_total), 2) as stddev_meters
            FROM (
                SELECT
                    CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
                    SUM(snowfall_mm) / 1000.0 as yearly_total
                FROM snowfall.snowfall_daily
                WHERE CAST(YEAR(CAST(date AS DATE)) AS INTEGER) BETWEEN {baseline_start} AND {baseline_end}
                GROUP BY year
            )
        ),
        recent AS (
            SELECT
                'Recent ({recent_start}-{recent_end})' as period,
                ROUND(AVG(yearly_total), 2) as avg_annual_meters,
                ROUND(STDDEV(yearly_total), 2) as stddev_meters
            FROM (
                SELECT
                    CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year,
                    SUM(snowfall_mm) / 1000.0 as yearly_total
                FROM snowfall.snowfall_daily
                WHERE CAST(YEAR(CAST(date AS DATE)) AS INTEGER) BETWEEN {recent_start} AND {recent_end}
                GROUP BY year
            )
        )
        SELECT
            period,
            avg_annual_meters,
            stddev_meters
        FROM baseline
        UNION ALL
        SELECT * FROM recent
        """
        return self.query(sql)

    def biggest_blizzards(self, limit: int = 100) -> pd.DataFrame:
        """
        Find the biggest snowstorm events

        DuckDB handles large sorts very efficiently
        """
        sql = f"""
        SELECT
            sd.date,
            s.name,
            s.state,
            s.country,
            ROUND(sd.snowfall_mm / 10.0, 2) as snowfall_cm,
            ROUND(sd.snow_depth_mm / 10.0, 2) as snow_depth_cm,
            ROUND(sd.temp_max_celsius, 1) as temp_max_c,
            ROUND(sd.temp_min_celsius, 1) as temp_min_c,
            s.elevation
        FROM snowfall.snowfall_daily sd
        JOIN snowfall.stations s ON sd.station_id = s.station_id
        WHERE sd.snowfall_mm > 0
        ORDER BY sd.snowfall_mm DESC
        LIMIT {limit}
        """
        return self.query(sql)

    def multi_day_storms(self, min_days: int = 3,
                        min_total_cm: float = 50.0) -> pd.DataFrame:
        """
        Find multi-day snowstorm events

        Uses window functions to detect consecutive snowy days
        """
        sql = f"""
        WITH daily_snow AS (
            SELECT
                sd.station_id,
                s.name,
                s.state,
                CAST(sd.date AS DATE) as date,
                ROUND(sd.snowfall_mm / 10.0, 2) as snowfall_cm,
                CASE
                    WHEN LAG(CAST(sd.date AS DATE), 1) OVER (PARTITION BY sd.station_id ORDER BY sd.date)
                         = CAST(sd.date AS DATE) - INTERVAL '1 day'
                    THEN 0 ELSE 1
                END as is_new_storm
            FROM snowfall.snowfall_daily sd
            JOIN snowfall.stations s ON sd.station_id = s.station_id
            WHERE sd.snowfall_mm > 0
        ),
        storm_groups AS (
            SELECT
                *,
                SUM(is_new_storm) OVER (PARTITION BY station_id ORDER BY date) as storm_id
            FROM daily_snow
        )
        SELECT
            name,
            state,
            MIN(date) as storm_start,
            MAX(date) as storm_end,
            COUNT(*) as days,
            ROUND(SUM(snowfall_cm), 2) as total_snowfall_cm,
            ROUND(AVG(snowfall_cm), 2) as avg_daily_cm
        FROM storm_groups
        GROUP BY station_id, name, state, storm_id
        HAVING days >= {min_days} AND total_snowfall_cm >= {min_total_cm}
        ORDER BY total_snowfall_cm DESC
        LIMIT 100
        """
        return self.query(sql)

    # ==========================================================================
    # Geographic Analysis
    # ==========================================================================

    def snowfall_by_elevation_band(self, band_size: int = 500) -> pd.DataFrame:
        """
        Analyze snowfall patterns by elevation

        Fast group-by with DuckDB
        """
        sql = f"""
        SELECT
            CAST(s.elevation / {band_size} AS INTEGER) * {band_size} as elevation_m,
            COUNT(DISTINCT s.station_id) as station_count,
            ROUND(AVG(ss.avg_annual_snowfall_mm) / 1000.0, 2) as avg_annual_meters,
            ROUND(AVG(ss.max_daily_snowfall_mm) / 10.0, 2) as avg_max_daily_cm,
            ROUND(AVG(ss.days_with_snow), 0) as avg_days_with_snow
        FROM snowfall.stations s
        JOIN snowfall.station_summaries ss ON s.station_id = ss.station_id
        WHERE s.elevation IS NOT NULL
        GROUP BY elevation_m
        HAVING station_count >= 5
        ORDER BY elevation_m
        """
        return self.query(sql)

    def us_vs_canada_detailed(self) -> pd.DataFrame:
        """
        Detailed comparison between US and Canada
        """
        sql = """
        SELECT
            s.country,
            COUNT(DISTINCT s.station_id) as stations,
            COUNT(*) as total_records,
            ROUND(AVG(sd.snowfall_mm), 2) as avg_daily_mm,
            ROUND(AVG(sd.snow_depth_mm), 2) as avg_depth_mm,
            ROUND(AVG(sd.temp_min_celsius), 1) as avg_temp_c,
            SUM(CASE WHEN sd.snowfall_mm > 254 THEN 1 ELSE 0 END) as days_over_10in,
            SUM(CASE WHEN sd.snowfall_mm > 508 THEN 1 ELSE 0 END) as days_over_20in,
            MIN(sd.date) as earliest_record,
            MAX(sd.date) as latest_record
        FROM snowfall.snowfall_daily sd
        JOIN snowfall.stations s ON sd.station_id = s.station_id
        WHERE sd.snowfall_mm > 0
        GROUP BY s.country
        ORDER BY country
        """
        return self.query(sql)

    # ==========================================================================
    # Export & Conversion
    # ==========================================================================

    def export_to_parquet(self, output_dir: str = "./snowfall_parquet"):
        """
        Export SQLite data to Parquet format for faster future queries

        Parquet is DuckDB's native format - queries will be much faster
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True, parents=True)

        logger.info("Exporting to Parquet format...")

        # Export stations table
        logger.info("Exporting stations...")
        self.conn.execute(f"""
            COPY (SELECT * FROM snowfall.stations)
            TO '{output_path}/stations.parquet' (FORMAT PARQUET)
        """)

        # Export station_summaries
        logger.info("Exporting station summaries...")
        self.conn.execute(f"""
            COPY (SELECT * FROM snowfall.station_summaries)
            TO '{output_path}/station_summaries.parquet' (FORMAT PARQUET)
        """)

        # Export snowfall_daily by year (partitioned for efficiency)
        logger.info("Exporting daily data (partitioned by year)...")
        self.conn.execute(f"""
            COPY (
                SELECT *, CAST(YEAR(CAST(date AS DATE)) AS INTEGER) as year
                FROM snowfall.snowfall_daily
            ) TO '{output_path}/snowfall_daily'
            (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE)
        """)

        logger.info(f"✓ Export complete! Files in {output_path}")
        logger.info(f"  To use Parquet files: SnowfallDuckDB('{output_path}', use_parquet=True)")

    def benchmark_query(self, query_name: str, sql: str, iterations: int = 3):
        """
        Benchmark a query for performance testing
        """
        import time

        times = []
        for i in range(iterations):
            start = time.time()
            self.conn.execute(sql).df()
            elapsed = time.time() - start
            times.append(elapsed)

        avg_time = sum(times) / len(times)
        logger.info(f"{query_name}: {avg_time:.3f}s average ({iterations} runs)")
        return avg_time

    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()


def main():
    """Example usage and benchmarks"""

    print("❄️  DuckDB Snowfall Analysis")
    print("=" * 80)
    print()

    # Initialize
    engine = SnowfallDuckDB("./snowfall_data.db")

    # Example 1: Top snowiest stations
    print("📊 Top 10 Snowiest Stations")
    print("-" * 80)
    df = engine.top_snowiest_stations(10)
    print(df)
    print()

    # Example 2: Annual trends
    print("📈 Snowfall by Year (Last 20 Years)")
    print("-" * 80)
    df = engine.snowfall_by_year(2004, 2024)
    print(df)
    print()

    # Example 3: Climate change analysis
    print("🌡️  Climate Change Analysis")
    print("-" * 80)
    df = engine.climate_change_analysis()
    print(df)
    print()

    # Example 4: Biggest blizzards
    print("❄️  Top 10 Biggest Blizzards")
    print("-" * 80)
    df = engine.biggest_blizzards(10)
    print(df)
    print()

    # Example 5: Multi-day storms
    print("🌨️  Multi-Day Storm Events")
    print("-" * 80)
    df = engine.multi_day_storms(min_days=3, min_total_cm=50)
    print(df.head(10))
    print()

    # Export to Parquet (optional - for even faster future queries)
    print("💾 Export to Parquet Format? (y/n)")
    # Uncomment to actually export:
    # engine.export_to_parquet("./snowfall_parquet")

    engine.close()


if __name__ == "__main__":
    main()
