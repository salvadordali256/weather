"""
Check NOAA Data Coverage for Northern Wisconsin
================================================

This script checks what historical snowfall data NOAA has available
for the Northwoods Wisconsin area.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from noaa_weather_fetcher import NOAAWeatherFetcher
import json

def check_noaa_coverage():
    """Check NOAA station coverage for Northern Wisconsin"""

    print("=" * 80)
    print("NOAA DATA COVERAGE CHECK - NORTHERN WISCONSIN")
    print("=" * 80)
    print()
    print("This will check what historical snowfall data NOAA has available")
    print("for the Northwoods Wisconsin area.")
    print()

    # Get API token
    api_token = input("Enter your NOAA API token (or press Enter to see instructions): ").strip()

    if not api_token:
        print()
        print("=" * 80)
        print("HOW TO GET A NOAA API TOKEN (Free, takes 5 minutes)")
        print("=" * 80)
        print()
        print("1. Visit: https://www.ncdc.noaa.gov/cdo-web/token")
        print("2. Enter your email address")
        print("3. Check your email for the token (arrives within minutes)")
        print("4. Copy the token and run this script again")
        print()
        print("The token looks like: 'AbCdEfGhIjKlMnOpQrStUvWxYz123456'")
        print()
        return

    # Initialize NOAA fetcher
    # NOAA requires a user-agent string (your email is fine)
    user_agent = "(salvadordali256.net, kyle@salvadordali256.net)"
    noaa = NOAAWeatherFetcher(api_token, user_agent=user_agent)

    print()
    print("Searching for stations in Northern Wisconsin...")
    print("(This may take a minute)")
    print()

    # Search for stations with snowfall data in Wisconsin
    # FIPS code for Wisconsin is 55
    try:
        stations = noaa.find_stations(
            datatypeid=["SNOW", "SNWD"],
            locationid="FIPS:55",
            limit=1000  # Get as many as possible
        )

        if not stations:
            print("❌ No stations found. Check your API token.")
            return

        print(f"✓ Found {len(stations)} total Wisconsin stations with snowfall data")
        print()

        # Filter for Northern Wisconsin (roughly latitude > 45.5)
        northern_stations = []
        for station in stations:
            lat = station.get('latitude', 0)
            lon = station.get('longitude', 0)

            # Northern Wisconsin: latitude > 45.5, longitude between -92 and -88
            if lat > 45.5 and -92 < lon < -88:
                northern_stations.append(station)

        print(f"✓ Found {len(northern_stations)} stations in Northern Wisconsin")
        print()

        # Sort by date range (oldest first)
        northern_stations.sort(key=lambda x: x.get('mindate', '9999'))

        print("=" * 80)
        print("HISTORICAL STATION DATA")
        print("=" * 80)
        print()

        # Show stations with details
        for i, station in enumerate(northern_stations[:20], 1):  # Show top 20
            name = station.get('name', 'Unknown')
            station_id = station.get('id', '')
            lat = station.get('latitude', 0)
            lon = station.get('longitude', 0)
            mindate = station.get('mindate', 'Unknown')
            maxdate = station.get('maxdate', 'Unknown')

            # Calculate years of data
            if mindate != 'Unknown' and maxdate != 'Unknown':
                try:
                    min_year = int(mindate[:4])
                    max_year = int(maxdate[:4])
                    years = max_year - min_year
                except:
                    years = 0
            else:
                years = 0

            print(f"{i}. {name}")
            print(f"   ID: {station_id}")
            print(f"   Location: {lat:.4f}°N, {lon:.4f}°W")
            print(f"   Data Range: {mindate[:10]} to {maxdate[:10]} ({years} years)")
            print()

        if len(northern_stations) > 20:
            print(f"... and {len(northern_stations) - 20} more stations")
            print()

        # Find the oldest station
        if northern_stations:
            oldest = northern_stations[0]
            oldest_year = int(oldest.get('mindate', '9999')[:4])

            print("=" * 80)
            print("SUMMARY")
            print("=" * 80)
            print()
            print(f"Oldest Station: {oldest.get('name')}")
            print(f"Records Start: {oldest.get('mindate')[:10]} ({oldest_year})")
            print(f"Available History: ~{2025 - oldest_year} years")
            print()

            if oldest_year < 1875:
                print("🎉 You have access to 150+ years of data!")
            elif oldest_year < 1925:
                print("✓ You have access to 100+ years of data!")
            elif oldest_year < 1975:
                print("✓ You have access to 50+ years of data!")
            else:
                print("⚠️  Limited to recent decades of data")

            print()
            print("=" * 80)
            print("NEXT STEPS")
            print("=" * 80)
            print()
            print("To collect this historical data:")
            print("1. Save your NOAA API token")
            print("2. Use the snowfall_collector.py script")
            print("3. Select stations from the list above")
            print("4. Specify date range (e.g., 1870-2025)")
            print()
            print("Note: Older data may have gaps and quality issues.")
            print("Most reliable data is from 1950s onward.")
            print()

    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        print("Common issues:")
        print("  - Invalid API token")
        print("  - Rate limit exceeded (5 requests per second)")
        print("  - Network connection problem")
        print()


if __name__ == "__main__":
    check_noaa_coverage()
