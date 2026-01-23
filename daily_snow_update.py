"""
DAILY SNOW FORECAST UPDATE
==========================
Run this script daily to get latest snow forecast updates
for the Northwoods Wisconsin/Upper Michigan region
"""

import requests
from datetime import datetime, timedelta
import json
import os

def fetch_latest_data():
    """Fetch latest weather data from Open-Meteo"""

    print("=" * 80)
    print("☁️  DAILY SNOW FORECAST UPDATE")
    print("=" * 80)
    print(f"Update Time: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    print()

    # Locations
    locations = {
        'Phelps_WI': {'lat': 46.06, 'lon': -89.09, 'name': 'Phelps, WI'},
        'Land_O_Lakes_WI': {'lat': 46.15, 'lon': -89.31, 'name': "Land O'Lakes, WI"},
        'Eagle_River_WI': {'lat': 45.92, 'lon': -89.24, 'name': 'Eagle River, WI'},
    }

    # Get 16-day forecast
    print("📡 Fetching latest forecast data...")
    print()

    all_data = {}

    for loc_key, loc_info in locations.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            'latitude': loc_info['lat'],
            'longitude': loc_info['lon'],
            'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,precipitation_probability_max,weathercode',
            'temperature_unit': 'fahrenheit',
            'precipitation_unit': 'inch',
            'timezone': 'America/Chicago',
            'forecast_days': 16
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                all_data[loc_key] = {
                    'location': loc_info['name'],
                    'forecast': data['daily']
                }
        except Exception as e:
            print(f"⚠️  Error fetching {loc_info['name']}: {e}")

    if not all_data:
        print("❌ Could not fetch data. Check internet connection.")
        return None

    print("✅ Data fetched successfully")
    print()

    return all_data


def analyze_forecast(data):
    """Analyze forecast and generate summary"""

    if not data:
        return

    # Get sample location for analysis
    sample_loc = list(data.values())[0]
    forecast = sample_loc['forecast']

    # Calculate snow totals for different periods
    today = datetime.now().date()

    # Next 7 days
    next_7_snow = []
    next_7_dates = []

    # Days 8-14
    days_8_14_snow = []

    for i, date_str in enumerate(forecast['time']):
        date = datetime.strptime(date_str, '%Y-%m-%d').date()
        days_from_now = (date - today).days

        snow = forecast['snowfall_sum'][i] if forecast['snowfall_sum'][i] else 0.0

        if 0 <= days_from_now <= 7:
            next_7_snow.append(snow)
            next_7_dates.append(date_str)
        elif 8 <= days_from_now <= 14:
            days_8_14_snow.append(snow)

    # Calculate averages across all locations
    all_locs_7day = []
    all_locs_8_14 = []

    for loc_key, loc_data in data.items():
        fc = loc_data['forecast']
        snow_7 = sum([fc['snowfall_sum'][i] if fc['snowfall_sum'][i] else 0
                      for i in range(min(8, len(fc['time'])))])
        snow_8_14 = sum([fc['snowfall_sum'][i] if fc['snowfall_sum'][i] else 0
                         for i in range(8, min(15, len(fc['time'])))])

        all_locs_7day.append(snow_7)
        all_locs_8_14.append(snow_8_14)

    avg_7day = sum(all_locs_7day) / len(all_locs_7day) if all_locs_7day else 0
    avg_8_14 = sum(all_locs_8_14) / len(all_locs_8_14) if all_locs_8_14 else 0

    # Display results
    print("=" * 80)
    print("📊 SNOW FORECAST SUMMARY")
    print("=" * 80)
    print()

    print("NEXT 7 DAYS (Short-range forecast):")
    print("-" * 80)
    print(f"  Expected snowfall: {avg_7day:.1f} inches")
    if avg_7day >= 12:
        print(f"  Status: 🚨 MAJOR SNOW EXPECTED")
    elif avg_7day >= 6:
        print(f"  Status: ❄️  Significant snow expected")
    elif avg_7day >= 3:
        print(f"  Status: ☁️  Moderate snow expected")
    else:
        print(f"  Status: 🌤️  Minimal snow expected")
    print()

    # Find biggest snow days in next 7
    biggest_days = []
    for i, date_str in enumerate(next_7_dates):
        if i < len(next_7_snow) and next_7_snow[i] >= 1.0:
            biggest_days.append((date_str, next_7_snow[i]))

    if biggest_days:
        biggest_days.sort(key=lambda x: x[1], reverse=True)
        print("  Biggest snow days:")
        for date, snow in biggest_days[:3]:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            day_name = date_obj.strftime('%A, %B %d')
            print(f"    • {day_name}: {snow:.1f} inches")
    else:
        print("  No significant snow days (≥1\") in next 7 days")
    print()

    print("DAYS 8-14 (Medium-range outlook):")
    print("-" * 80)
    print(f"  Expected snowfall: {avg_8_14:.1f} inches")
    if avg_8_14 >= 10:
        print(f"  Status: ⚠️  Active pattern possible")
    elif avg_8_14 >= 5:
        print(f"  Status: ☁️  Some snow likely")
    else:
        print(f"  Status: 🌤️  Quiet pattern")
    print()

    # Temperature outlook
    temps_next_7 = [sample_loc['forecast']['temperature_2m_min'][i]
                    for i in range(min(7, len(sample_loc['forecast']['time'])))]
    avg_low = sum(temps_next_7) / len(temps_next_7) if temps_next_7 else 0
    min_low = min(temps_next_7) if temps_next_7 else 0

    print("TEMPERATURE OUTLOOK (Next 7 days):")
    print("-" * 80)
    print(f"  Average overnight low: {avg_low:.0f}°F")
    print(f"  Coldest expected: {min_low:.0f}°F")

    if min_low <= -20:
        print(f"  Status: 🥶 EXTREME COLD expected")
    elif min_low <= 0:
        print(f"  Status: ❄️  Very cold")
    elif min_low <= 15:
        print(f"  Status: 🌡️  Cold")
    else:
        print(f"  Status: 🌤️  Mild for winter")
    print()

    # Day-by-day detail for next 7 days
    print("=" * 80)
    print("7-DAY DETAILED FORECAST")
    print("=" * 80)
    print()

    for i in range(min(7, len(sample_loc['forecast']['time']))):
        date_str = sample_loc['forecast']['time'][i]
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        day_name = date_obj.strftime('%A, %B %d')

        temp_max = sample_loc['forecast']['temperature_2m_max'][i]
        temp_min = sample_loc['forecast']['temperature_2m_min'][i]
        snow = sample_loc['forecast']['snowfall_sum'][i] if sample_loc['forecast']['snowfall_sum'][i] else 0
        precip_prob = sample_loc['forecast']['precipitation_probability_max'][i]

        print(f"{day_name}")
        print(f"  High: {temp_max:.0f}°F  |  Low: {temp_min:.0f}°F  |  Snow: {snow:.1f}\"  |  Prob: {precip_prob}%")

        if snow >= 6:
            print(f"  ⚠️  SIGNIFICANT SNOW POSSIBLE")
        elif snow >= 3:
            print(f"  ❄️  Moderate snow expected")
        elif snow >= 1:
            print(f"  ☁️  Light snow expected")

        print()

    # Ski resort implications
    print("=" * 80)
    print("🎿 SKI CONDITIONS OUTLOOK")
    print("=" * 80)
    print()

    total_7day = avg_7day

    if total_7day >= 12:
        print("  EXCELLENT - Fresh powder expected!")
        print(f"  New base: ~{total_7day:.0f} inches over next week")
        print("  Best days: Check 'Biggest snow days' above")
    elif total_7day >= 6:
        print("  GOOD - Decent snowfall expected")
        print(f"  New base: ~{total_7day:.0f} inches over next week")
    elif total_7day >= 3:
        print("  FAIR - Some new snow")
        print(f"  New base: ~{total_7day:.0f} inches over next week")
    else:
        print("  LIMITED - Minimal new snow")
        print(f"  New base: ~{total_7day:.0f} inches over next week")
        print("  Rely on existing base")

    print()

    # Save summary to file
    summary = {
        'date': datetime.now().isoformat(),
        'next_7_days_snow': round(avg_7day, 2),
        'days_8_14_snow': round(avg_8_14, 2),
        'avg_low_temp': round(avg_low, 1),
        'min_temp': round(min_low, 1),
        'biggest_snow_days': biggest_days[:3] if biggest_days else []
    }

    with open('daily_update_history.json', 'a') as f:
        f.write(json.dumps(summary) + '\n')

    print("=" * 80)
    print("✅ Update complete. Summary saved to daily_update_history.json")
    print("=" * 80)
    print()
    print("💡 TIP: Run this script daily to track forecast changes!")
    print()


if __name__ == "__main__":
    data = fetch_latest_data()
    analyze_forecast(data)
