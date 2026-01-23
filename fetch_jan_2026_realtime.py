"""
Fetch REAL-TIME weather data for January 2026
Collecting data from Open-Meteo and NOAA for Northwoods region
Date: January 4, 2026
"""

import requests
from datetime import datetime, timedelta
import json

print("=" * 80)
print("FETCHING REAL-TIME WEATHER DATA - January 4, 2026")
print("=" * 80)
print()

# Northwoods Wisconsin locations
locations = {
    'Phelps_WI': {'lat': 46.06, 'lon': -89.09, 'name': 'Phelps, WI'},
    'Land_O_Lakes_WI': {'lat': 46.15, 'lon': -89.31, 'name': "Land O'Lakes, WI"},
    'Eagle_River_WI': {'lat': 45.92, 'lon': -89.24, 'name': 'Eagle River, WI'},
    'St_Germain_WI': {'lat': 45.91, 'lon': -89.47, 'name': 'St. Germain, WI'},
    'Watersmeet_MI': {'lat': 46.27, 'lon': -89.18, 'name': 'Watersmeet, MI'},
    'Ironwood_MI': {'lat': 46.45, 'lon': -90.17, 'name': 'Ironwood, MI'},
    'Bergland_MI': {'lat': 46.59, 'lon': -89.55, 'name': 'Bergland, MI'}
}

# Date range: December 29, 2025 to January 4, 2026
start_date = '2025-12-29'
end_date = '2026-01-04'
today = datetime.now().strftime('%Y-%m-%d')

print(f"Fetching data from {start_date} to {end_date}")
print()

all_data = {}

for loc_key, loc_info in locations.items():
    print("=" * 80)
    print(f"LOCATION: {loc_info['name']}")
    print("=" * 80)

    # Open-Meteo API - Free weather data
    # Using archive API for past data and forecast API for current/future

    # Historical data (Dec 29 - today)
    url = f"https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': loc_info['lat'],
        'longitude': loc_info['lon'],
        'start_date': start_date,
        'end_date': today,
        'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,snow_depth_mean',
        'temperature_unit': 'fahrenheit',
        'precipitation_unit': 'inch',
        'timezone': 'America/Chicago'
    }

    try:
        print(f"\nFetching historical data from Open-Meteo...")
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()

            if 'daily' in data:
                daily = data['daily']

                print(f"\n--- Weather Data for {loc_info['name']} ---")
                print(f"{'Date':<12} {'Temp Max':<10} {'Temp Min':<10} {'Precip':<10} {'Snowfall':<10} {'Snow Depth':<12}")
                print("-" * 80)

                location_data = []

                for i in range(len(daily['time'])):
                    date = daily['time'][i]
                    temp_max = daily['temperature_2m_max'][i] if daily['temperature_2m_max'][i] is not None else 'N/A'
                    temp_min = daily['temperature_2m_min'][i] if daily['temperature_2m_min'][i] is not None else 'N/A'
                    precip = daily['precipitation_sum'][i] if daily['precipitation_sum'][i] is not None else 0.0
                    snowfall = daily['snowfall_sum'][i] if daily['snowfall_sum'][i] is not None else 0.0
                    snow_depth = daily['snow_depth_mean'][i] if daily['snow_depth_mean'][i] is not None else 'N/A'

                    print(f"{date:<12} {str(temp_max)+'°F':<10} {str(temp_min)+'°F':<10} {str(precip)+'\"':<10} {str(snowfall)+'\"':<10} {str(snow_depth)+'\"':<12}")

                    location_data.append({
                        'date': date,
                        'temp_max_f': temp_max,
                        'temp_min_f': temp_min,
                        'precipitation_in': precip,
                        'snowfall_in': snowfall,
                        'snow_depth_in': snow_depth
                    })

                # Calculate totals
                total_snow = sum([d['snowfall_in'] for d in location_data if isinstance(d['snowfall_in'], (int, float))])
                total_precip = sum([d['precipitation_in'] for d in location_data if isinstance(d['precipitation_in'], (int, float))])

                print("-" * 80)
                print(f"TOTALS (Dec 29 - {today}):")
                print(f"  Total Snowfall: {total_snow:.2f} inches")
                print(f"  Total Precipitation: {total_precip:.2f} inches")

                # Calculate January totals (starting from Jan 1)
                jan_snow = sum([d['snowfall_in'] for d in location_data
                               if isinstance(d['snowfall_in'], (int, float)) and d['date'] >= '2026-01-01'])
                jan_precip = sum([d['precipitation_in'] for d in location_data
                                 if isinstance(d['precipitation_in'], (int, float)) and d['date'] >= '2026-01-01'])

                print(f"\nJANUARY 2026 TOTALS (Jan 1-{today.split('-')[2]}):")
                print(f"  Snowfall: {jan_snow:.2f} inches")
                print(f"  Precipitation: {jan_precip:.2f} inches")

                all_data[loc_key] = {
                    'location': loc_info['name'],
                    'daily_data': location_data,
                    'totals': {
                        'period_snow': total_snow,
                        'period_precip': total_precip,
                        'jan_snow': jan_snow,
                        'jan_precip': jan_precip
                    }
                }

        else:
            print(f"Error: HTTP {response.status_code}")
            print(response.text[:200])

    except Exception as e:
        print(f"Error fetching data for {loc_info['name']}: {e}")

    print()

# Summary
print("=" * 80)
print("JANUARY 2026 SNOWFALL SUMMARY (Jan 1-4)")
print("=" * 80)
print()
print(f"{'Location':<25} {'Snowfall (inches)':<20} {'Precip (inches)':<20}")
print("-" * 80)

for loc_key, data in all_data.items():
    if 'totals' in data:
        print(f"{data['location']:<25} {data['totals']['jan_snow']:<20.2f} {data['totals']['jan_precip']:<20.2f}")

print()
print("=" * 80)
print("REGIONAL AVERAGE (Jan 1-4):")
print("=" * 80)

if all_data:
    avg_snow = sum([d['totals']['jan_snow'] for d in all_data.values() if 'totals' in d]) / len(all_data)
    avg_precip = sum([d['totals']['jan_precip'] for d in all_data.values() if 'totals' in d]) / len(all_data)

    print(f"Average Snowfall: {avg_snow:.2f} inches")
    print(f"Average Precipitation: {avg_precip:.2f} inches")
    print()
    print(f"📊 FORECAST CHECK:")
    print(f"   Predicted for Week 1 (Jan 1-7): 6-12 inches")
    print(f"   Actual so far (Jan 1-4): {avg_snow:.2f} inches")

    if avg_snow < 6:
        print(f"   Status: ⚠️  Below forecast pace ({avg_snow/6*100:.0f}% of low end)")
    elif avg_snow <= 12:
        print(f"   Status: ✅ On track ({avg_snow/12*100:.0f}% of high end)")
    else:
        print(f"   Status: 🚀 Above forecast!")

print()

# Save to file
output_file = 'realtime_jan2026_data.json'
with open(output_file, 'w') as f:
    json.dump(all_data, f, indent=2)

print(f"Data saved to: {output_file}")
print()
print("=" * 80)
print(f"Data fetch completed: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
print("=" * 80)
