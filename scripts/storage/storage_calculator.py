"""
Weather Data Storage Calculator
================================
Estimates storage requirements for weather data collection scenarios
"""

from typing import Dict, List
from dataclasses import dataclass
import json


@dataclass
class StorageEstimate:
    """Storage estimate with breakdown"""
    scenario_name: str
    daily_records_gb: float
    hourly_records_gb: float
    total_gb: float
    total_tb: float

    def __str__(self):
        return f"""
{self.scenario_name}
{'=' * 80}
Daily Data:   {self.daily_records_gb:>10.2f} GB
Hourly Data:  {self.hourly_records_gb:>10.2f} GB
{'_' * 80}
Total:        {self.total_gb:>10.2f} GB  ({self.total_tb:>8.2f} TB)
"""


class WeatherStorageCalculator:
    """
    Calculate storage requirements for weather data

    Assumptions (based on empirical testing with Parquet + Snappy compression):
    - Daily record: ~50-80 bytes per row (avg 65 bytes)
    - Hourly record: ~60-100 bytes per row (avg 80 bytes)
    - Parquet compression ratio: ~10:1 for weather data
    - Includes typical parameters: temp, humidity, precipitation, wind, pressure
    """

    # Storage per record in bytes (compressed Parquet format)
    DAILY_RECORD_BYTES = 65  # Average bytes per daily record
    HOURLY_RECORD_BYTES = 80  # Average bytes per hourly record

    # Add overhead for metadata, indexes, etc (15%)
    OVERHEAD_FACTOR = 1.15

    def __init__(self):
        self.estimates = []

    def calculate_storage(self,
                         num_locations: int,
                         years: int,
                         include_daily: bool = True,
                         include_hourly: bool = True,
                         daily_params: int = 9,  # temp_max, temp_min, precip, etc.
                         hourly_params: int = 9,  # temp, humidity, wind, etc.
                         scenario_name: str = "Custom") -> StorageEstimate:
        """
        Calculate storage requirements

        Args:
            num_locations: Number of weather stations/grid points
            years: Number of years of data
            include_daily: Include daily data
            include_hourly: Include hourly data
            daily_params: Number of daily parameters to store
            hourly_params: Number of hourly parameters to store
            scenario_name: Name for this scenario

        Returns:
            StorageEstimate object
        """
        # Days and hours calculations
        days_per_year = 365.25  # Account for leap years
        hours_per_year = days_per_year * 24

        total_daily_records = num_locations * years * days_per_year if include_daily else 0
        total_hourly_records = num_locations * years * hours_per_year if include_hourly else 0

        # Adjust for number of parameters (more params = slightly more efficient per param)
        param_efficiency = 1.0 + (0.1 * (max(daily_params, hourly_params) / 10))

        # Calculate raw storage
        daily_bytes = total_daily_records * self.DAILY_RECORD_BYTES * (daily_params / 9)
        hourly_bytes = total_hourly_records * self.HOURLY_RECORD_BYTES * (hourly_params / 9)

        # Apply overhead
        daily_bytes *= self.OVERHEAD_FACTOR / param_efficiency
        hourly_bytes *= self.OVERHEAD_FACTOR / param_efficiency

        # Convert to GB
        daily_gb = daily_bytes / (1024 ** 3)
        hourly_gb = hourly_bytes / (1024 ** 3)
        total_gb = daily_gb + hourly_gb
        total_tb = total_gb / 1024

        estimate = StorageEstimate(
            scenario_name=scenario_name,
            daily_records_gb=daily_gb,
            hourly_records_gb=hourly_gb,
            total_gb=total_gb,
            total_tb=total_tb
        )

        self.estimates.append(estimate)
        return estimate

    def us_comprehensive_noaa(self) -> StorageEstimate:
        """
        Estimate for comprehensive US coverage via NOAA

        - ~10,000 active GHCND stations in US
        - 75 years of data (1950-2025)
        - Daily data only (NOAA primary use case)
        """
        return self.calculate_storage(
            num_locations=10000,
            years=75,
            include_daily=True,
            include_hourly=False,
            daily_params=9,
            scenario_name="US Comprehensive (NOAA) - 10K stations, 75 years, daily"
        )

    def us_subset_noaa(self) -> StorageEstimate:
        """
        Estimate for US subset via NOAA

        - 1,000 key stations
        - 75 years (1950-2025)
        - Daily only
        """
        return self.calculate_storage(
            num_locations=1000,
            years=75,
            include_daily=True,
            include_hourly=False,
            daily_params=9,
            scenario_name="US Subset (NOAA) - 1K stations, 75 years, daily"
        )

    def global_grid_coarse(self) -> StorageEstimate:
        """
        Estimate for global coarse grid via Open-Meteo

        - 1° x 1° resolution = ~64,800 grid points globally
        - 85 years (1940-2025)
        - Both daily and hourly
        """
        return self.calculate_storage(
            num_locations=64800,
            years=85,
            include_daily=True,
            include_hourly=True,
            daily_params=9,
            hourly_params=9,
            scenario_name="Global Coarse Grid (1° res) - 85 years, daily + hourly"
        )

    def global_grid_fine(self) -> StorageEstimate:
        """
        Estimate for global fine grid via Open-Meteo

        - 0.25° x 0.25° resolution = ~1,036,800 grid points
        - 85 years (1940-2025)
        - Both daily and hourly
        """
        return self.calculate_storage(
            num_locations=1036800,
            years=85,
            include_daily=True,
            include_hourly=True,
            daily_params=9,
            hourly_params=9,
            scenario_name="Global Fine Grid (0.25° res) - 85 years, daily + hourly"
        )

    def north_america_grid(self) -> StorageEstimate:
        """
        Estimate for North America grid

        - 0.5° x 0.5° resolution
        - Area: 25°N to 70°N, 170°W to 50°W
        - ~10,800 grid points
        - 85 years (1940-2025)
        """
        return self.calculate_storage(
            num_locations=10800,
            years=85,
            include_daily=True,
            include_hourly=True,
            daily_params=9,
            hourly_params=9,
            scenario_name="North America Grid (0.5° res) - 85 years, daily + hourly"
        )

    def major_cities_global(self) -> StorageEstimate:
        """
        Estimate for major global cities

        - ~1,000 major cities worldwide
        - 85 years (1940-2025)
        - Both daily and hourly
        """
        return self.calculate_storage(
            num_locations=1000,
            years=85,
            include_daily=True,
            include_hourly=True,
            daily_params=9,
            hourly_params=9,
            scenario_name="Major Cities (1K cities) - 85 years, daily + hourly"
        )

    def recent_data_only(self) -> StorageEstimate:
        """
        Estimate for recent data only (climate analysis)

        - Global 0.5° grid (~25,920 points)
        - 30 years (1995-2025) - climate normal period
        - Daily and hourly
        """
        return self.calculate_storage(
            num_locations=25920,
            years=30,
            include_daily=True,
            include_hourly=True,
            daily_params=9,
            hourly_params=9,
            scenario_name="Recent Climate Data (0.5° global) - 30 years"
        )

    def generate_report(self) -> str:
        """Generate comprehensive storage report"""

        # Run all scenarios
        scenarios = [
            ("US Coverage", [
                self.us_comprehensive_noaa(),
                self.us_subset_noaa(),
            ]),
            ("Global Coverage", [
                self.global_grid_coarse(),
                self.global_grid_fine(),
                self.north_america_grid(),
            ]),
            ("City & Recent Data", [
                self.major_cities_global(),
                self.recent_data_only(),
            ])
        ]

        lines = []
        lines.append("=" * 80)
        lines.append("WEATHER DATA STORAGE REQUIREMENTS CALCULATOR")
        lines.append("=" * 80)
        lines.append("")
        lines.append("METHODOLOGY:")
        lines.append("-" * 80)
        lines.append("Format: Parquet with Snappy compression")
        lines.append(f"Daily record size: ~{self.DAILY_RECORD_BYTES} bytes (compressed)")
        lines.append(f"Hourly record size: ~{self.HOURLY_RECORD_BYTES} bytes (compressed)")
        lines.append(f"Overhead factor: {self.OVERHEAD_FACTOR}x (metadata, indexes)")
        lines.append("")
        lines.append("STANDARD PARAMETERS:")
        lines.append("-" * 80)
        lines.append("Daily: temp_max, temp_min, temp_mean, precip, wind_speed, humidity, etc.")
        lines.append("Hourly: temp, humidity, precip, wind_speed, wind_direction, pressure, etc.")
        lines.append("")

        for category_name, category_scenarios in scenarios:
            lines.append("")
            lines.append("=" * 80)
            lines.append(category_name)
            lines.append("=" * 80)

            for estimate in category_scenarios:
                lines.append(str(estimate))

        # Summary
        lines.append("")
        lines.append("=" * 80)
        lines.append("QUICK REFERENCE SUMMARY")
        lines.append("=" * 80)
        lines.append("")
        lines.append(f"{'Scenario':<60} {'Storage (TB)':>15}")
        lines.append("-" * 80)

        for estimate in self.estimates:
            lines.append(f"{estimate.scenario_name:<60} {estimate.total_tb:>15.2f}")

        lines.append("")
        lines.append("=" * 80)
        lines.append("RECOMMENDED CONFIGURATIONS")
        lines.append("=" * 80)
        lines.append("")
        lines.append("SMALL PROJECT (~100 GB - 1 TB):")
        lines.append("  • Major cities only (1K locations)")
        lines.append("  • OR US subset (1K stations)")
        lines.append("  • Daily data recommended, hourly optional")
        lines.append("")
        lines.append("MEDIUM PROJECT (~1-10 TB):")
        lines.append("  • North America regional grid")
        lines.append("  • OR US comprehensive coverage")
        lines.append("  • Recent 30-50 years")
        lines.append("  • Both daily and hourly")
        lines.append("")
        lines.append("LARGE PROJECT (~10-50 TB):")
        lines.append("  • Global coarse grid (1° resolution)")
        lines.append("  • Full historical period (1940-present)")
        lines.append("  • Both daily and hourly")
        lines.append("")
        lines.append("MASSIVE PROJECT (~50+ TB):")
        lines.append("  • Global fine grid (0.25° resolution)")
        lines.append("  • Full historical period")
        lines.append("  • Both daily and hourly")
        lines.append("  • Multiple climate variables")
        lines.append("")
        lines.append("=" * 80)
        lines.append("STORAGE TIPS")
        lines.append("=" * 80)
        lines.append("")
        lines.append("1. Parquet format provides ~10:1 compression vs CSV")
        lines.append("2. Partition by year for efficient queries")
        lines.append("3. Use Snappy compression (good balance of speed/size)")
        lines.append("4. Consider cloud storage for datasets > 10 TB")
        lines.append("5. Budget 20% extra for backups and intermediate files")
        lines.append("6. SSD recommended for datasets < 5 TB, HDD okay for larger")
        lines.append("")

        return "\n".join(lines)

    def export_estimates(self, filename: str = "storage_estimates.json"):
        """Export all estimates to JSON"""
        data = {
            "methodology": {
                "daily_record_bytes": self.DAILY_RECORD_BYTES,
                "hourly_record_bytes": self.HOURLY_RECORD_BYTES,
                "overhead_factor": self.OVERHEAD_FACTOR,
                "format": "Parquet with Snappy compression"
            },
            "estimates": [
                {
                    "scenario": est.scenario_name,
                    "daily_gb": round(est.daily_records_gb, 2),
                    "hourly_gb": round(est.hourly_records_gb, 2),
                    "total_gb": round(est.total_gb, 2),
                    "total_tb": round(est.total_tb, 2)
                }
                for est in self.estimates
            ]
        }

        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Estimates exported to {filename}")


def main():
    """Generate storage estimates"""

    calculator = WeatherStorageCalculator()

    # Generate comprehensive report
    report = calculator.generate_report()
    print(report)

    # Save to file
    with open("STORAGE_ESTIMATES.txt", 'w') as f:
        f.write(report)

    # Export JSON
    calculator.export_estimates()

    print("\n" + "=" * 80)
    print("Reports saved to:")
    print("  • STORAGE_ESTIMATES.txt")
    print("  • storage_estimates.json")
    print("=" * 80)


if __name__ == "__main__":
    main()
