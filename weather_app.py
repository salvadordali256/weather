#!/usr/bin/env python3
"""
Simple Open-Meteo Weather Application
======================================

Easy command-line tool to fetch weather data from Open-Meteo for any location.
No API token required - completely free!
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from openmeteo_weather_fetcher import OpenMeteoWeatherFetcher
from datetime import datetime, timedelta
import pandas as pd


def print_header(text):
    """Print a formatted header"""
    print()
    print("=" * 80)
    print(text)
    print("=" * 80)
    print()


def fetch_current_weather(lat, lon, location_name):
    """Fetch and display current weather conditions"""
    fetcher = OpenMeteoWeatherFetcher()

    print_header(f"CURRENT WEATHER - {location_name}")
    lat_dir = "N" if lat >= 0 else "S"
    lon_dir = "E" if lon >= 0 else "W"
    print(f"Coordinates: {abs(lat):.4f}°{lat_dir}, {abs(lon):.4f}°{lon_dir}")
    print()

    result = fetcher.fetch_current_weather(
        latitude=lat,
        longitude=lon,
        current_params=[
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "snowfall",
            "snow_depth",
            "wind_speed_10m",
            "wind_direction_10m"
        ]
    )

    if result and 'current' in result:
        current = result['current']

        print("🌡️  TEMPERATURE")
        print(f"   Current: {current.get('temperature_2m', 'N/A')}°C")
        print(f"   Feels Like: {current.get('apparent_temperature', 'N/A')}°C")
        print()

        print("💧 PRECIPITATION")
        print(f"   Rain: {current.get('rain', 0)} mm")
        print(f"   Snow: {current.get('snowfall', 0)} cm")
        print(f"   Snow Depth: {current.get('snow_depth', 0)} cm")
        print()

        print("💨 WIND")
        print(f"   Speed: {current.get('wind_speed_10m', 'N/A')} km/h")
        print(f"   Direction: {current.get('wind_direction_10m', 'N/A')}°")
        print()

        print("💧 HUMIDITY")
        print(f"   {current.get('relative_humidity_2m', 'N/A')}%")
        print()


def fetch_7day_forecast(lat, lon, location_name):
    """Fetch and display 7-day weather forecast"""
    fetcher = OpenMeteoWeatherFetcher()

    print_header(f"7-DAY FORECAST - {location_name}")
    lat_dir = "N" if lat >= 0 else "S"
    lon_dir = "E" if lon >= 0 else "W"
    print(f"Coordinates: {abs(lat):.4f}°{lat_dir}, {abs(lon):.4f}°{lon_dir}")
    print()

    result = fetcher.fetch_forecast(
        latitude=lat,
        longitude=lon,
        forecast_days=7,
        daily_params=[
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "wind_speed_10m_max"
        ]
    )

    if result and 'daily' in result:
        daily_df = result['daily']

        if not daily_df.empty:
            print(f"{'Date':<12} {'High°C':<8} {'Low°C':<8} {'Rain mm':<10} {'Snow cm':<10} {'Wind km/h':<12}")
            print("-" * 80)

            for idx, row in daily_df.iterrows():
                date = row.get('time', idx)
                temp_max = row.get('temperature_2m_max', 'N/A')
                temp_min = row.get('temperature_2m_min', 'N/A')
                rain = row.get('rain_sum', 0)
                snow = row.get('snowfall_sum', 0)
                wind = row.get('wind_speed_10m_max', 'N/A')

                # Format values
                temp_max_str = f"{temp_max:.1f}" if isinstance(temp_max, (int, float)) else "N/A"
                temp_min_str = f"{temp_min:.1f}" if isinstance(temp_min, (int, float)) else "N/A"
                rain_str = f"{rain:.1f}" if rain > 0 else "-"
                snow_str = f"{snow:.1f}" if snow > 0 else "-"
                wind_str = f"{wind:.1f}" if isinstance(wind, (int, float)) else "N/A"

                print(f"{str(date):<12} {temp_max_str:<8} {temp_min_str:<8} {rain_str:<10} {snow_str:<10} {wind_str:<12}")
            print()


def fetch_historical_data(lat, lon, location_name, days_back=30):
    """Fetch and display historical weather data"""
    fetcher = OpenMeteoWeatherFetcher()

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)

    print_header(f"HISTORICAL DATA - {location_name}")
    lat_dir = "N" if lat >= 0 else "S"
    lon_dir = "E" if lon >= 0 else "W"
    print(f"Coordinates: {abs(lat):.4f}°{lat_dir}, {abs(lon):.4f}°{lon_dir}")
    print(f"Period: {start_date} to {end_date} ({days_back} days)")
    print()

    result = fetcher.fetch_historical_data(
        latitude=lat,
        longitude=lon,
        start_date=str(start_date),
        end_date=str(end_date),
        daily_params=[
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "snowfall_sum",
            "snow_depth_mean"
        ]
    )

    if result and 'daily' in result:
        daily_df = result['daily']

        if not daily_df.empty:
            # Calculate statistics
            total_precip = daily_df['precipitation_sum'].sum() if 'precipitation_sum' in daily_df else 0
            total_snow = daily_df['snowfall_sum'].sum() if 'snowfall_sum' in daily_df else 0
            avg_temp_max = daily_df['temperature_2m_max'].mean() if 'temperature_2m_max' in daily_df else 0
            avg_temp_min = daily_df['temperature_2m_min'].mean() if 'temperature_2m_min' in daily_df else 0

            print("📊 SUMMARY STATISTICS")
            print(f"   Average High: {avg_temp_max:.1f}°C")
            print(f"   Average Low: {avg_temp_min:.1f}°C")
            print(f"   Total Precipitation: {total_precip:.1f} mm")
            print(f"   Total Snowfall: {total_snow:.1f} cm")
            print()

            # Show days with significant weather
            print("❄️  DAYS WITH SNOW (showing top 10)")
            print("-" * 60)
            snow_days = daily_df[daily_df['snowfall_sum'] > 0].copy() if 'snowfall_sum' in daily_df else pd.DataFrame()

            if not snow_days.empty:
                snow_days = snow_days.sort_values('snowfall_sum', ascending=False).head(10)

                for idx, row in snow_days.iterrows():
                    date = row.get('time', idx)
                    snow = row.get('snowfall_sum', 0)
                    temp_max = row.get('temperature_2m_max', 'N/A')
                    temp_min = row.get('temperature_2m_min', 'N/A')

                    print(f"   {date}: {snow:.1f} cm (High: {temp_max:.1f}°C, Low: {temp_min:.1f}°C)")
            else:
                print("   No snowfall in this period")
            print()


def main():
    """Main application menu"""

    print_header("OPEN-METEO WEATHER APPLICATION")
    print("Free weather data for any location worldwide!")
    print("Data source: Open-Meteo.com (no API key required)")
    print()

    # Get location from user
    print("Enter location coordinates:")
    print("(Examples: Eagle River, WI = 45.9169, -89.2443)")
    print("           (Phelps, WI = 46.0608, -89.0793)")
    print("           (Your location = look it up on Google Maps)")
    print()

    try:
        lat = float(input("Latitude: ").strip())
        lon = float(input("Longitude: ").strip())
        location_name = input("Location name (optional): ").strip() or f"{lat:.4f}, {lon:.4f}"
    except ValueError:
        print("❌ Invalid coordinates. Please enter numbers.")
        return

    # Show menu
    while True:
        print()
        print("=" * 80)
        print("WHAT WOULD YOU LIKE TO SEE?")
        print("=" * 80)
        print()
        print("1. Current Weather")
        print("2. 7-Day Forecast")
        print("3. Historical Data (past 30 days)")
        print("4. Historical Data (past 90 days)")
        print("5. Historical Data (past year)")
        print("6. Change Location")
        print("7. Exit")
        print()

        choice = input("Select option (1-7): ").strip()

        if choice == "1":
            fetch_current_weather(lat, lon, location_name)
        elif choice == "2":
            fetch_7day_forecast(lat, lon, location_name)
        elif choice == "3":
            fetch_historical_data(lat, lon, location_name, days_back=30)
        elif choice == "4":
            fetch_historical_data(lat, lon, location_name, days_back=90)
        elif choice == "5":
            fetch_historical_data(lat, lon, location_name, days_back=365)
        elif choice == "6":
            # Get new location
            try:
                lat = float(input("Latitude: ").strip())
                lon = float(input("Longitude: ").strip())
                location_name = input("Location name (optional): ").strip() or f"{lat:.4f}, {lon:.4f}"
            except ValueError:
                print("❌ Invalid coordinates. Using previous location.")
        elif choice == "7":
            print()
            print("Thanks for using Open-Meteo Weather App! 🌤️")
            print()
            break
        else:
            print("❌ Invalid option. Please select 1-7.")


if __name__ == "__main__":
    main()
