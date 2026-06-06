"""
Snowfall Data Analysis Tools
=============================

Analyze 100 years of snowfall data for US and Canada.
Provides queries, statistics, and insights.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Tuple
import matplotlib.pyplot as plt
import seaborn as sns


class SnowfallAnalyzer:
    """Analyze snowfall data from SQLite database"""

    def __init__(self, db_path: str = "./snowfall_data.db"):
        """
        Initialize analyzer

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        return pd.read_sql(sql, self.conn)

    # ==========================================================================
    # Summary Statistics
    # ==========================================================================

    def get_overview(self) -> pd.DataFrame:
        """Get database overview"""
        sql = """
        SELECT
            COUNT(DISTINCT station_id) as total_stations,
            COUNT(*) as total_records,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            SUM(CASE WHEN snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
            ROUND(SUM(snowfall_mm) / 1000.0, 2) as total_snowfall_meters
        FROM snowfall_daily
        """
        return self.query(sql)

    def get_top_snowiest_stations(self, limit: int = 20) -> pd.DataFrame:
        """
        Get stations with highest total snowfall

        Returns:
            DataFrame with station name, location, and total snowfall
        """
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
            ss.max_daily_snowfall_date,
            ss.total_years
        FROM station_summaries ss
        JOIN stations s ON ss.station_id = s.station_id
        ORDER BY ss.total_snowfall_mm DESC
        LIMIT {limit}
        """
        return self.query(sql)

    def get_snowfall_by_state(self) -> pd.DataFrame:
        """Get total snowfall by state/province"""
        sql = """
        SELECT
            s.state,
            s.country,
            COUNT(DISTINCT s.station_id) as station_count,
            ROUND(SUM(ss.total_snowfall_mm) / 1000.0, 2) as total_snowfall_meters,
            ROUND(AVG(ss.avg_annual_snowfall_mm) / 1000.0, 2) as avg_annual_meters
        FROM stations s
        JOIN station_summaries ss ON s.station_id = ss.station_id
        GROUP BY s.state, s.country
        ORDER BY total_snowfall_meters DESC
        """
        return self.query(sql)

    # ==========================================================================
    # Temporal Analysis
    # ==========================================================================

    def get_annual_snowfall_trend(self,
                                  start_year: Optional[int] = None,
                                  end_year: Optional[int] = None) -> pd.DataFrame:
        """
        Get annual snowfall totals over time

        Useful for identifying climate change trends
        """
        where_clause = ""
        if start_year and end_year:
            where_clause = f"WHERE CAST(strftime('%Y', date) AS INTEGER) BETWEEN {start_year} AND {end_year}"

        sql = f"""
        SELECT
            CAST(strftime('%Y', date) AS INTEGER) as year,
            COUNT(DISTINCT station_id) as station_count,
            COUNT(*) as total_days,
            SUM(CASE WHEN snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
            ROUND(SUM(snowfall_mm) / 1000.0, 2) as total_snowfall_meters,
            ROUND(AVG(snowfall_mm), 2) as avg_daily_mm,
            ROUND(MAX(snowfall_mm) / 10.0, 2) as max_daily_cm
        FROM snowfall_daily
        {where_clause}
        GROUP BY year
        ORDER BY year
        """
        return self.query(sql)

    def get_monthly_snowfall_climatology(self) -> pd.DataFrame:
        """
        Get average snowfall by month across all years

        Shows seasonal patterns
        """
        sql = """
        SELECT
            CAST(strftime('%m', date) AS INTEGER) as month,
            CASE CAST(strftime('%m', date) AS INTEGER)
                WHEN 1 THEN 'January'
                WHEN 2 THEN 'February'
                WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'
                WHEN 5 THEN 'May'
                WHEN 6 THEN 'June'
                WHEN 7 THEN 'July'
                WHEN 8 THEN 'August'
                WHEN 9 THEN 'September'
                WHEN 10 THEN 'October'
                WHEN 11 THEN 'November'
                WHEN 12 THEN 'December'
            END as month_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
            ROUND(AVG(snowfall_mm), 2) as avg_snowfall_mm,
            ROUND(AVG(snow_depth_mm), 2) as avg_snow_depth_mm
        FROM snowfall_daily
        GROUP BY month
        ORDER BY month
        """
        return self.query(sql)

    def get_decade_comparison(self) -> pd.DataFrame:
        """
        Compare snowfall across decades

        Useful for climate change analysis
        """
        sql = """
        SELECT
            (CAST(strftime('%Y', date) AS INTEGER) / 10) * 10 as decade,
            COUNT(DISTINCT station_id) as station_count,
            SUM(CASE WHEN snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow,
            ROUND(AVG(snowfall_mm), 2) as avg_snowfall_mm,
            ROUND(SUM(snowfall_mm) / 1000.0 / COUNT(DISTINCT CAST(strftime('%Y', date) AS INTEGER)), 2) as avg_annual_meters
        FROM snowfall_daily
        GROUP BY decade
        ORDER BY decade
        """
        return self.query(sql)

    # ==========================================================================
    # Record Events
    # ==========================================================================

    def get_biggest_snowstorms(self, limit: int = 100) -> pd.DataFrame:
        """
        Find the biggest single-day snowfall events

        Args:
            limit: Number of events to return
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
            s.elevation
        FROM snowfall_daily sd
        JOIN stations s ON sd.station_id = s.station_id
        WHERE sd.snowfall_mm > 0
        ORDER BY sd.snowfall_mm DESC
        LIMIT {limit}
        """
        return self.query(sql)

    def get_deepest_snow(self, limit: int = 100) -> pd.DataFrame:
        """
        Find the deepest snow depth measurements

        Args:
            limit: Number of records to return
        """
        sql = f"""
        SELECT
            sd.date,
            s.name,
            s.state,
            s.country,
            ROUND(sd.snow_depth_mm / 10.0, 2) as snow_depth_cm,
            ROUND(sd.snowfall_mm / 10.0, 2) as snowfall_cm,
            s.elevation
        FROM snowfall_daily sd
        JOIN stations s ON sd.station_id = s.station_id
        WHERE sd.snow_depth_mm > 0
        ORDER BY sd.snow_depth_mm DESC
        LIMIT {limit}
        """
        return self.query(sql)

    def get_longest_snow_seasons(self) -> pd.DataFrame:
        """
        Find locations with longest periods between first and last snow

        Identifies places with extended winter seasons
        """
        sql = """
        SELECT
            s.name,
            s.state,
            s.country,
            ss.first_snow_date,
            ss.last_snow_date,
            CAST((julianday(ss.last_snow_date) - julianday(ss.first_snow_date)) AS INTEGER) as season_length_days,
            ROUND(ss.avg_annual_snowfall_mm / 1000.0, 2) as avg_annual_meters
        FROM station_summaries ss
        JOIN stations s ON ss.station_id = s.station_id
        WHERE ss.first_snow_date IS NOT NULL
        ORDER BY season_length_days DESC
        LIMIT 50
        """
        return self.query(sql)

    # ==========================================================================
    # Geographic Analysis
    # ==========================================================================

    def get_snowfall_by_elevation(self, bin_size: int = 500) -> pd.DataFrame:
        """
        Analyze snowfall patterns by elevation

        Args:
            bin_size: Elevation bin size in meters
        """
        sql = f"""
        SELECT
            (CAST(s.elevation / {bin_size} AS INTEGER) * {bin_size}) as elevation_bin,
            COUNT(DISTINCT s.station_id) as station_count,
            ROUND(AVG(ss.avg_annual_snowfall_mm) / 1000.0, 2) as avg_annual_meters,
            ROUND(AVG(ss.max_daily_snowfall_mm) / 10.0, 2) as avg_max_daily_cm
        FROM stations s
        JOIN station_summaries ss ON s.station_id = ss.station_id
        WHERE s.elevation IS NOT NULL
        GROUP BY elevation_bin
        HAVING station_count >= 3
        ORDER BY elevation_bin
        """
        return self.query(sql)

    def compare_us_canada(self) -> pd.DataFrame:
        """Compare snowfall patterns between US and Canada"""
        sql = """
        SELECT
            s.country,
            COUNT(DISTINCT s.station_id) as station_count,
            ROUND(AVG(ss.avg_annual_snowfall_mm) / 1000.0, 2) as avg_annual_meters,
            ROUND(AVG(ss.max_daily_snowfall_mm) / 10.0, 2) as avg_max_daily_cm,
            ROUND(AVG(ss.days_with_snow), 1) as avg_days_with_snow,
            MIN(sd.date) as earliest_record,
            MAX(sd.date) as latest_record
        FROM stations s
        JOIN station_summaries ss ON s.station_id = ss.station_id
        JOIN snowfall_daily sd ON s.station_id = sd.station_id
        GROUP BY s.country
        """
        return self.query(sql)

    # ==========================================================================
    # Custom Queries
    # ==========================================================================

    def query_station(self, station_name_pattern: str) -> pd.DataFrame:
        """
        Get all data for stations matching name pattern

        Args:
            station_name_pattern: SQL LIKE pattern (e.g., "%Denver%")
        """
        sql = f"""
        SELECT
            sd.date,
            s.name,
            s.state,
            ROUND(sd.snowfall_mm / 10.0, 2) as snowfall_cm,
            ROUND(sd.snow_depth_mm / 10.0, 2) as snow_depth_cm,
            ROUND(sd.temp_max_celsius, 1) as temp_max_c,
            ROUND(sd.temp_min_celsius, 1) as temp_min_c
        FROM snowfall_daily sd
        JOIN stations s ON sd.station_id = s.station_id
        WHERE s.name LIKE '{station_name_pattern}'
        ORDER BY sd.date
        """
        return self.query(sql)

    def query_region(self, state: str, start_year: int, end_year: int) -> pd.DataFrame:
        """
        Get snowfall data for a specific state/province and time range

        Args:
            state: State or province name
            start_year: Start year
            end_year: End year
        """
        sql = f"""
        SELECT
            CAST(strftime('%Y', sd.date) AS INTEGER) as year,
            COUNT(DISTINCT sd.station_id) as station_count,
            ROUND(SUM(sd.snowfall_mm) / 1000.0, 2) as total_snowfall_meters,
            ROUND(AVG(sd.snowfall_mm), 2) as avg_daily_mm,
            SUM(CASE WHEN sd.snowfall_mm > 0 THEN 1 ELSE 0 END) as days_with_snow
        FROM snowfall_daily sd
        JOIN stations s ON sd.station_id = s.station_id
        WHERE s.state = '{state}'
        AND CAST(strftime('%Y', sd.date) AS INTEGER) BETWEEN {start_year} AND {end_year}
        GROUP BY year
        ORDER BY year
        """
        return self.query(sql)

    # ==========================================================================
    # Reports
    # ==========================================================================

    def generate_comprehensive_report(self, output_file: str = "snowfall_report.txt"):
        """Generate comprehensive analysis report"""
        lines = []

        lines.append("=" * 80)
        lines.append("SNOWFALL DATA ANALYSIS REPORT - US & CANADA")
        lines.append("=" * 80)
        lines.append("")

        # Overview
        lines.append("DATABASE OVERVIEW")
        lines.append("-" * 80)
        overview = self.get_overview()
        for col in overview.columns:
            lines.append(f"{col}: {overview[col].iloc[0]}")
        lines.append("")

        # Top snowiest stations
        lines.append("TOP 10 SNOWIEST STATIONS (Total Snowfall)")
        lines.append("-" * 80)
        top_stations = self.get_top_snowiest_stations(10)
        for _, row in top_stations.iterrows():
            lines.append(f"{row['name']}, {row['state']}, {row['country']}: "
                        f"{row['total_snowfall_meters']:,.1f}m total, "
                        f"{row['avg_annual_meters']:.1f}m/year, "
                        f"{row['max_daily_cm']:.1f}cm max daily")
        lines.append("")

        # US vs Canada
        lines.append("US vs CANADA COMPARISON")
        lines.append("-" * 80)
        comparison = self.compare_us_canada()
        for _, row in comparison.iterrows():
            lines.append(f"\n{row['country']}:")
            lines.append(f"  Stations: {row['station_count']}")
            lines.append(f"  Avg Annual Snowfall: {row['avg_annual_meters']}m")
            lines.append(f"  Avg Max Daily: {row['avg_max_daily_cm']}cm")
            lines.append(f"  Avg Days with Snow: {row['avg_days_with_snow']}")
            lines.append(f"  Date Range: {row['earliest_record']} to {row['latest_record']}")
        lines.append("")

        # Top snowstorms
        lines.append("TOP 10 BIGGEST SNOWSTORMS (Single Day)")
        lines.append("-" * 80)
        storms = self.get_biggest_snowstorms(10)
        for _, row in storms.iterrows():
            lines.append(f"{row['date']}: {row['snowfall_cm']:.1f}cm - "
                        f"{row['name']}, {row['state']}, {row['country']}")
        lines.append("")

        # Decade comparison
        lines.append("SNOWFALL BY DECADE")
        lines.append("-" * 80)
        decades = self.get_decade_comparison()
        for _, row in decades.iterrows():
            lines.append(f"{row['decade']}s: {row['avg_annual_meters']:.2f}m/year avg, "
                        f"{row['days_with_snow']:,} days with snow")
        lines.append("")

        report_text = "\n".join(lines)
        print(report_text)

        # Save to file
        with open(output_file, 'w') as f:
            f.write(report_text)

        print(f"\n📄 Report saved to {output_file}")

        return report_text

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    """Example usage"""

    # Initialize analyzer
    analyzer = SnowfallAnalyzer("./snowfall_data.db")

    print("❄️  SNOWFALL DATA ANALYSIS")
    print("=" * 80)
    print()

    # Generate comprehensive report
    analyzer.generate_comprehensive_report()

    # Example queries
    print("\n📊 Top 5 Snowiest States/Provinces:")
    print(analyzer.get_snowfall_by_state().head())

    print("\n📈 Annual Snowfall Trend (Last 10 Years):")
    print(analyzer.get_annual_snowfall_trend(2014, 2024))

    print("\n🗓️  Monthly Snowfall Climatology:")
    print(analyzer.get_monthly_snowfall_climatology())

    # Close connection
    analyzer.close()


if __name__ == "__main__":
    main()
