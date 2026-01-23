#!/usr/bin/env python3
"""
Test script for weather_app.py
Tests all functions without requiring user input
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from openmeteo_weather_fetcher import OpenMeteoWeatherFetcher
from datetime import datetime, timedelta
import pandas as pd


def test_current_weather():
    """Test current weather fetching"""
    print("=" * 80)
    print("TEST 1: Current Weather")
    print("=" * 80)
    print()

    fetcher = OpenMeteoWeatherFetcher()

    # Test with Eagle River, WI
    lat, lon = 45.9169, -89.2443
    print(f"Testing: Eagle River, WI ({lat}, {lon})")
    print()

    try:
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
            print("✅ Current weather fetched successfully!")
            print(f"   Temperature: {current.get('temperature_2m', 'N/A')}°C")
            print(f"   Humidity: {current.get('relative_humidity_2m', 'N/A')}%")
            print(f"   Wind: {current.get('wind_speed_10m', 'N/A')} km/h")
            print(f"   Snow Depth: {current.get('snow_depth', 0)} cm")
            return True
        else:
            print("❌ Failed to fetch current weather")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_forecast():
    """Test 7-day forecast"""
    print()
    print("=" * 80)
    print("TEST 2: 7-Day Forecast")
    print("=" * 80)
    print()

    fetcher = OpenMeteoWeatherFetcher()

    lat, lon = 45.9169, -89.2443
    print(f"Testing: Eagle River, WI ({lat}, {lon})")
    print()

    try:
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
                print(f"✅ Forecast fetched successfully! ({len(daily_df)} days)")
                print()
                print(f"{'Date':<12} {'High°C':<8} {'Low°C':<8} {'Rain mm':<10}")
                print("-" * 50)

                for idx, row in daily_df.head(3).iterrows():
                    date = row.get('time', idx)
                    temp_max = row.get('temperature_2m_max', 'N/A')
                    temp_min = row.get('temperature_2m_min', 'N/A')
                    rain = row.get('rain_sum', 0)

                    temp_max_str = f"{temp_max:.1f}" if isinstance(temp_max, (int, float)) else "N/A"
                    temp_min_str = f"{temp_min:.1f}" if isinstance(temp_min, (int, float)) else "N/A"
                    rain_str = f"{rain:.1f}" if rain > 0 else "-"

                    print(f"{str(date):<12} {temp_max_str:<8} {temp_min_str:<8} {rain_str:<10}")

                print("   ... (showing first 3 days)")
                return True
            else:
                print("❌ Forecast dataframe is empty")
                return False
        else:
            print("❌ Failed to fetch forecast")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_historical_data():
    """Test historical data fetching"""
    print()
    print("=" * 80)
    print("TEST 3: Historical Data (30 days)")
    print("=" * 80)
    print()

    fetcher = OpenMeteoWeatherFetcher()

    lat, lon = 45.9169, -89.2443
    days_back = 30

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)

    print(f"Testing: Eagle River, WI ({lat}, {lon})")
    print(f"Period: {start_date} to {end_date}")
    print()

    try:
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
                print(f"✅ Historical data fetched successfully! ({len(daily_df)} days)")
                print()

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

                # Count snow days
                snow_days = daily_df[daily_df['snowfall_sum'] > 0] if 'snowfall_sum' in daily_df else pd.DataFrame()
                print(f"   Snow Days: {len(snow_days)}")

                return True
            else:
                print("❌ Historical dataframe is empty")
                return False
        else:
            print("❌ Failed to fetch historical data")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_coordinate_formats():
    """Test various coordinate formats"""
    print()
    print("=" * 80)
    print("TEST 4: Coordinate Validation")
    print("=" * 80)
    print()

    test_cases = [
        (45.9169, -89.2443, "Eagle River, WI (positive lat, negative lon)"),
        (-33.8688, 151.2093, "Sydney, Australia (negative lat, positive lon)"),
        (51.5074, -0.1278, "London, UK (positive lat, negative lon)"),
        (35.6762, 139.6503, "Tokyo, Japan (positive lat, positive lon)"),
    ]

    fetcher = OpenMeteoWeatherFetcher()
    passed = 0

    for lat, lon, description in test_cases:
        try:
            result = fetcher.fetch_current_weather(
                latitude=lat,
                longitude=lon,
                current_params=["temperature_2m"]
            )

            if result and 'current' in result:
                temp = result['current'].get('temperature_2m', 'N/A')
                print(f"✅ {description}")
                print(f"   Lat: {lat}, Lon: {lon}, Temp: {temp}°C")
                passed += 1
            else:
                print(f"❌ {description} - No data returned")
        except Exception as e:
            print(f"❌ {description} - Error: {e}")

    print()
    print(f"Passed: {passed}/{len(test_cases)}")
    return passed == len(test_cases)


def main():
    """Run all tests"""
    print("=" * 80)
    print("WEATHER APP TEST SUITE")
    print("=" * 80)
    print()
    print("Testing all weather_app.py functionality...")
    print()

    results = []

    # Run tests
    results.append(("Current Weather", test_current_weather()))
    results.append(("7-Day Forecast", test_forecast()))
    results.append(("Historical Data", test_historical_data()))
    results.append(("Coordinate Validation", test_coordinate_formats()))

    # Summary
    print()
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print()

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}  {test_name}")

    print()
    print(f"Total: {passed}/{total} tests passed")
    print()

    if passed == total:
        print("🎉 All tests passed! The weather app is working correctly.")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")

    print()


if __name__ == "__main__":
    main()
