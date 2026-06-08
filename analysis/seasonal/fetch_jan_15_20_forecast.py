"""
Fetch 15-day forecast for January 6-20, 2026
Using Open-Meteo Forecast API
Date: January 5, 2026
"""

import requests
from datetime import datetime
import json

print("=" * 80)
print("FETCHING 15-DAY FORECAST - January 6-20, 2026")
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

all_forecast_data = {}

for loc_key, loc_info in locations.items():
    print("=" * 80)
    print(f"LOCATION: {loc_info['name']}")
    print("=" * 80)

    # Open-Meteo Forecast API - 16 days forecast
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
        print(f"\nFetching 16-day forecast from Open-Meteo...")
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()

            if 'daily' in data:
                daily = data['daily']

                print(f"\n--- 16-Day Forecast for {loc_info['name']} ---")
                print(f"{'Date':<15} {'High':<8} {'Low':<8} {'Precip':<8} {'Snow':<8} {'Prob%':<8} {'Wx':<20}")
                print("-" * 90)

                location_forecast = []

                # Weather code descriptions
                weather_codes = {
                    0: "Clear",
                    1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
                    45: "Fog", 48: "Rime Fog",
                    51: "Light Drizzle", 53: "Drizzle", 55: "Heavy Drizzle",
                    61: "Light Rain", 63: "Rain", 65: "Heavy Rain",
                    71: "Light Snow", 73: "Snow", 75: "Heavy Snow",
                    77: "Snow Grains",
                    80: "Light Showers", 81: "Showers", 82: "Heavy Showers",
                    85: "Light Snow Showers", 86: "Snow Showers",
                    95: "Thunderstorm",
                    96: "Thunderstorm + Hail", 99: "Heavy Thunderstorm + Hail"
                }

                for i in range(len(daily['time'])):
                    date = daily['time'][i]
                    temp_max = daily['temperature_2m_max'][i] if daily['temperature_2m_max'][i] is not None else 'N/A'
                    temp_min = daily['temperature_2m_min'][i] if daily['temperature_2m_min'][i] is not None else 'N/A'
                    precip = daily['precipitation_sum'][i] if daily['precipitation_sum'][i] is not None else 0.0
                    snowfall = daily['snowfall_sum'][i] if daily['snowfall_sum'][i] is not None else 0.0
                    precip_prob = daily['precipitation_probability_max'][i] if daily['precipitation_probability_max'][i] is not None else 0
                    wx_code = daily['weathercode'][i] if daily['weathercode'][i] is not None else 0
                    wx_desc = weather_codes.get(wx_code, "Unknown")

                    print(f"{date:<15} {str(temp_max)+'°F':<8} {str(temp_min)+'°F':<8} {str(precip)+'\"':<8} {str(snowfall)+'\"':<8} {str(precip_prob)+'%':<8} {wx_desc:<20}")

                    location_forecast.append({
                        'date': date,
                        'temp_max_f': temp_max,
                        'temp_min_f': temp_min,
                        'precipitation_in': precip,
                        'snowfall_in': snowfall,
                        'precip_probability': precip_prob,
                        'weather_code': wx_code,
                        'weather_desc': wx_desc
                    })

                # Calculate weekly totals
                jan6_12 = [d for d in location_forecast if '2026-01-06' <= d['date'] <= '2026-01-12']
                jan13_19 = [d for d in location_forecast if '2026-01-13' <= d['date'] <= '2026-01-19']
                jan15_20 = [d for d in location_forecast if '2026-01-15' <= d['date'] <= '2026-01-20']

                jan6_12_snow = sum([d['snowfall_in'] for d in jan6_12 if isinstance(d['snowfall_in'], (int, float))])
                jan13_19_snow = sum([d['snowfall_in'] for d in jan13_19 if isinstance(d['snowfall_in'], (int, float))])
                jan15_20_snow = sum([d['snowfall_in'] for d in jan15_20 if isinstance(d['snowfall_in'], (int, float))])

                print("-" * 90)
                print(f"FORECAST TOTALS:")
                print(f"  Jan 6-12:  {jan6_12_snow:.2f} inches")
                print(f"  Jan 13-19: {jan13_19_snow:.2f} inches")
                print(f"  Jan 15-20: {jan15_20_snow:.2f} inches ⭐ TARGET PERIOD")

                all_forecast_data[loc_key] = {
                    'location': loc_info['name'],
                    'forecast': location_forecast,
                    'totals': {
                        'jan6_12': jan6_12_snow,
                        'jan13_19': jan13_19_snow,
                        'jan15_20': jan15_20_snow
                    }
                }

        else:
            print(f"Error: HTTP {response.status_code}")
            print(response.text[:200])

    except Exception as e:
        print(f"Error fetching forecast for {loc_info['name']}: {e}")

    print()

# Summary
print("=" * 80)
print("FORECAST SUMMARY: JANUARY 15-20, 2026")
print("=" * 80)
print()
print(f"{'Location':<25} {'Jan 15-20 Snow (inches)':<30}")
print("-" * 80)

for loc_key, data in all_forecast_data.items():
    if 'totals' in data:
        print(f"{data['location']:<25} {data['totals']['jan15_20']:<30.2f}")

print()
print("=" * 80)
print("REGIONAL AVERAGE FORECAST:")
print("=" * 80)

if all_forecast_data:
    avg_jan15_20 = sum([d['totals']['jan15_20'] for d in all_forecast_data.values() if 'totals' in d]) / len(all_forecast_data)
    avg_jan6_12 = sum([d['totals']['jan6_12'] for d in all_forecast_data.values() if 'totals' in d]) / len(all_forecast_data)
    avg_jan13_19 = sum([d['totals']['jan13_19'] for d in all_forecast_data.values() if 'totals' in d]) / len(all_forecast_data)

    print(f"Jan 6-12 Average:  {avg_jan6_12:.2f} inches")
    print(f"Jan 13-19 Average: {avg_jan13_19:.2f} inches")
    print(f"Jan 15-20 Average: {avg_jan15_20:.2f} inches ⭐")
    print()

    print(f"📊 COMPARISON TO ORIGINAL FORECAST:")
    print(f"   Original Jan 15-20: 18-28 inches (updated forecast)")
    print(f"   Current Model:      {avg_jan15_20:.2f} inches")

    if avg_jan15_20 < 18:
        print(f"   Status: ⚠️  Below forecast ({avg_jan15_20/18*100:.0f}% of low end)")
    elif avg_jan15_20 <= 28:
        print(f"   Status: ✅ Within forecast range")
    else:
        print(f"   Status: 🚀 Above forecast!")

print()

# Save to file
output_file = 'forecast_jan15_20_2026.json'
with open(output_file, 'w') as f:
    json.dump(all_forecast_data, f, indent=2)

print(f"Forecast data saved to: {output_file}")
print()
print("=" * 80)
print(f"Forecast fetch completed: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
print("=" * 80)
