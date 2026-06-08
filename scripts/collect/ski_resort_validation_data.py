"""
Ski Resort Historical Snowfall Data
====================================

Historical snowfall records from Northern Wisconsin and Upper Peninsula ski resorts
for validation and comparison with NOAA weather station data.

Data sourced from OnTheSnow.com historical records (2012-2025)
"""

ski_resort_data = {
    # Upper Peninsula Michigan - Near Phelps/Land O'Lakes area
    "Big Powderhorn Mountain": {
        "location": "Bessemer, MI (Upper Peninsula)",
        "distance_from_phelps_wi": "~25 miles",
        "avg_annual_snowfall": 79,  # inches
        "seasons": {
            "2012-2013": {"total": 159, "snow_days": 35, "biggest_day": 12},
            "2013-2014": {"total": 122, "snow_days": 33, "biggest_day": 12},
            "2014-2015": {"total": 59, "snow_days": 19, "biggest_day": 6},
            "2015-2016": {"total": 107, "snow_days": 32, "biggest_day": 12},
            "2016-2017": {"total": 45, "snow_days": 17, "biggest_day": 6},
            "2017-2018": {"total": 78, "snow_days": 23, "biggest_day": 8},
            "2018-2019": {"total": 80, "snow_days": 17, "biggest_day": 16},
            "2019-2020": {"total": 64, "snow_days": 23, "biggest_day": 12},
            "2020-2021": {"total": 33, "snow_days": 11, "biggest_day": 12},
            "2021-2022": {"total": 77, "snow_days": 18, "biggest_day": 20},
            "2022-2023": {"total": 104, "snow_days": 17, "biggest_day": 36},
            "2023-2024": {"total": 32, "snow_days": 5, "biggest_day": 7},
        }
    },

    "Whitecap Mountain": {
        "location": "Montreal, WI (Near Hurley)",
        "distance_from_phelps_wi": "~30 miles",
        "avg_annual_snowfall": 72,
        "notes": "Filed bankruptcy Nov 2025 after historic low snow in 2023-24",
        "seasons": {
            "2012-2013": {"total": 75, "snow_days": 16, "biggest_day": 12},
            "2013-2014": {"total": 95, "snow_days": 30, "biggest_day": 30},
            "2014-2015": {"total": 105, "snow_days": 24, "biggest_day": 16},
            "2015-2016": {"total": 70, "snow_days": 21, "biggest_day": 12},
            "2016-2017": {"total": 50, "snow_days": 12, "biggest_day": 12},
            "2017-2018": {"total": 69, "snow_days": 11, "biggest_day": 12},
            "2018-2019": {"total": 66, "snow_days": 16, "biggest_day": 8},
            "2019-2020": {"total": 65, "snow_days": 20, "biggest_day": 15},
            "2020-2021": {"total": 11, "snow_days": 3, "biggest_day": 6},
            "2021-2022": {"total": 114, "snow_days": 21, "biggest_day": 30},
            "2022-2023": {"total": 163, "snow_days": 37, "biggest_day": 12, "notes": "260\" reported by resort"},
            "2023-2024": {"total": 10, "snow_days": 4, "biggest_day": 5, "notes": "HISTORIC LOW - Led to bankruptcy"},
            "2024-2025": {"total": 71, "snow_days": 27, "notes": "Current season"},
        }
    },

    "Mount Bohemia": {
        "location": "Lac La Belle, MI (Keweenaw Peninsula)",
        "distance_from_phelps_wi": "~150 miles",
        "avg_annual_snowfall": 99,  # OnTheSnow average
        "reported_avg": 273,  # Resort-reported average (includes unmeasured snow)
        "notes": "Lake Superior lake effect, powder snow",
        "seasons": {
            "2012-2013": {"total": 119, "snow_days": 22},
            "2013-2014": {"total": 216, "snow_days": 53, "notes": "RECORD SEASON"},
            "2014-2015": {"total": 114, "snow_days": 29},
            "2015-2016": {"total": 113, "snow_days": 26},
            "2016-2017": {"total": 86, "snow_days": 16},
            "2017-2018": {"total": 69, "snow_days": 21},
            "2018-2019": {"total": 148, "snow_days": 27},
            "2019-2020": {"total": 77, "snow_days": 18},
            "2020-2021": {"total": 20, "snow_days": 7, "notes": "HISTORIC LOW"},
            "2021-2022": {"total": 100, "snow_days": 34},
            "2022-2023": {"total": 100, "snow_days": 24},
            "2023-2024": {"total": 46, "snow_days": 12},
            "2024-2025": {"total": 161, "snow_days": 41},
        }
    },

    "Marquette Mountain": {
        "location": "Marquette, MI (Upper Peninsula)",
        "distance_from_phelps_wi": "~100 miles",
        "avg_annual_snowfall": 40,
        "seasons": {
            "2012-2013": {"total": 25, "snow_days": 3},
            "2013-2014": {"total": 75, "snow_days": 27},
            "2014-2015": {"total": 12, "snow_days": 6},
            "2015-2016": {"total": 13, "snow_days": 3},
            "2016-2017": {"total": 8, "snow_days": 3},
            "2017-2018": {"total": 56, "snow_days": 8},
            "2019-2020": {"total": 0, "snow_days": 0, "notes": "No recorded snowfall"},
            "2020-2021": {"total": 2, "snow_days": 2},
            "2021-2022": {"total": 48, "snow_days": 11},
            "2022-2023": {"total": 111, "snow_days": 14},
            "2023-2024": {"total": 33, "snow_days": 7},
            "2024-2025": {"total": 123, "snow_days": 26, "notes": "RECORD - Current season"},
        }
    },

    # Northern Wisconsin - Further south
    "Granite Peak": {
        "location": "Wausau, WI (Rib Mountain)",
        "distance_from_phelps_wi": "~100 miles south",
        "avg_annual_snowfall": 45,
        "seasons": {
            "2012-2013": {"total": 41, "snow_days": 12, "biggest_day": 5},
            "2013-2014": {"total": 68, "snow_days": 25, "biggest_day": 8},
            "2014-2015": {"total": 41, "snow_days": 14, "biggest_day": 5},
            "2015-2016": {"total": 36, "snow_days": 10, "biggest_day": 12},
            "2016-2017": {"total": 31, "snow_days": 9, "biggest_day": 10},
            "2017-2018": {"total": 28, "snow_days": 7, "biggest_day": 7},
            "2018-2019": {"total": 68, "snow_days": 19, "biggest_day": 9},
            "2019-2020": {"total": 105, "snow_days": 23, "biggest_day": 11, "notes": "ABOVE AVERAGE"},
            "2020-2021": {"total": 30, "snow_days": 13, "biggest_day": 9},
            "2021-2022": {"total": 45, "snow_days": 16, "biggest_day": 7},
            "2022-2023": {"total": 54, "snow_days": 12, "biggest_day": 10},
            "2023-2024": {"total": 29, "snow_days": 8, "biggest_day": 12},
            "2024-2025": {"total": 32, "snow_days": 9, "biggest_day": 7},
        }
    },

    "Cascade Mountain": {
        "location": "Portage, WI (Wisconsin Dells area)",
        "distance_from_phelps_wi": "~200 miles south",
        "avg_annual_snowfall": 34,
        "notes": "Southernmost resort - less lake effect",
        "seasons": {
            "2012-2013": {"total": 40, "snow_days": 8, "biggest_day": 20},
            "2013-2014": {"total": 64, "snow_days": 28, "biggest_day": 8},
            "2014-2015": {"total": 19, "snow_days": 10, "biggest_day": 4},
            "2015-2016": {"total": 34, "snow_days": 9, "biggest_day": 11},
            "2016-2017": {"total": 39, "snow_days": 12, "biggest_day": 5},
            "2017-2018": {"total": 34, "snow_days": 17, "biggest_day": 6},
            "2018-2019": {"total": 63, "snow_days": 23, "biggest_day": 9},
            "2019-2020": {"total": 12, "snow_days": 2, "biggest_day": 10},
            "2020-2021": {"total": 34, "snow_days": 14, "biggest_day": 7},
            "2021-2022": {"total": 23, "snow_days": 7, "biggest_day": 6},
            "2022-2023": {"total": 49, "snow_days": 20, "biggest_day": 9},
            "2023-2024": {"total": 28, "snow_days": 12, "biggest_day": 9},
            "2024-2025": {"total": 29, "snow_days": 10, "biggest_day": 7},
        }
    },

    "Nordic Mountain": {
        "location": "Wild Rose, WI",
        "distance_from_phelps_wi": "~180 miles south",
        "avg_annual_snowfall": 38,
        "seasons": {
            "2012-2013": {"total": 58, "snow_days": 15, "biggest_day": 12},
            "2013-2014": {"total": 53, "snow_days": 21, "biggest_day": 8},
            "2014-2015": {"total": 26, "snow_days": 13, "biggest_day": 5},
            "2015-2016": {"total": 33, "snow_days": 8, "biggest_day": 12},
            "2016-2017": {"total": 43, "snow_days": 13, "biggest_day": 8},
            "2017-2018": {"total": 25, "snow_days": 16, "biggest_day": 4},
            "2018-2019": {"total": 65, "snow_days": 18, "biggest_day": 15},
            "2019-2020": {"total": 51, "snow_days": 17, "biggest_day": 7},
            "2020-2021": {"total": 23, "snow_days": 9, "biggest_day": 5},
            "2021-2022": {"total": 30, "snow_days": 15, "biggest_day": 4},
            "2022-2023": {"total": 49, "snow_days": 16, "biggest_day": 6},
            "2023-2024": {"total": 28, "snow_days": 7, "biggest_day": 12},
            "2024-2025": {"total": 25, "snow_days": 8, "biggest_day": 6},
        }
    },
}

# Key patterns and observations
patterns = {
    "2012-2013_winter": {
        "classification": "Above Average to Excellent",
        "big_powderhorn": 159,
        "whitecap": 75,
        "mount_bohemia": 119,
        "granite_peak": 41,
        "notes": "Good snow year, especially in UP. Big Powderhorn had 159 inches."
    },

    "2013-2014_winter": {
        "classification": "EXCEPTIONAL - Record-Breaking",
        "big_powderhorn": 122,
        "whitecap": 95,
        "mount_bohemia": 216,  # RECORD
        "granite_peak": 68,
        "marquette": 75,
        "notes": "Mount Bohemia record 216 inches. Strong across all resorts."
    },

    "2020-2021_winter": {
        "classification": "HISTORIC LOW - Catastrophic",
        "big_powderhorn": 33,
        "whitecap": 11,
        "mount_bohemia": 20,
        "granite_peak": 30,
        "notes": "Disastrous snow year across entire region. Climate pattern shift?"
    },

    "2022-2023_winter": {
        "classification": "EXCELLENT - Recovery Year",
        "big_powderhorn": 104,
        "whitecap": 163,  # Resort reported 260"
        "mount_bohemia": 100,
        "granite_peak": 54,
        "marquette": 111,
        "notes": "Strong recovery after low snow years. Whitecap's last good season before bankruptcy."
    },

    "2023-2024_winter": {
        "classification": "CATASTROPHIC - Resort Closures",
        "big_powderhorn": 32,
        "whitecap": 10,  # Led to bankruptcy
        "mount_bohemia": 46,
        "granite_peak": 29,
        "notes": "Second historic low. Whitecap filed bankruptcy Nov 2025 citing this season."
    },
}

def analyze_season(season):
    """Analyze a specific season across all resorts"""
    print(f"\n{'='*80}")
    print(f"SEASON: {season}")
    print(f"{'='*80}\n")

    for resort_name, data in ski_resort_data.items():
        if season in data["seasons"]:
            season_data = data["seasons"][season]
            print(f"{resort_name:25s} | {season_data.get('total', 0):3d}\" | "
                  f"{season_data.get('snow_days', 0):2d} days | "
                  f"Location: {data['location']}")
            if "notes" in season_data:
                print(f"  → {season_data['notes']}")

    if season in patterns:
        print(f"\n📊 Pattern: {patterns[season]['classification']}")
        print(f"   {patterns[season]['notes']}")

if __name__ == "__main__":
    print("=" * 80)
    print("SKI RESORT SNOWFALL VALIDATION DATA")
    print("Northern Wisconsin & Upper Peninsula Michigan")
    print("=" * 80)
    print("\nData Source: OnTheSnow.com Historical Records")
    print("Coverage: 2012-2025 winter seasons")
    print("\n")

    # Analyze key seasons
    key_seasons = ["2012-2013", "2013-2014", "2020-2021", "2022-2023", "2023-2024", "2024-2025"]

    for season in key_seasons:
        analyze_season(season)

    # Summary statistics
    print(f"\n{'='*80}")
    print("RESORT AVERAGES & DISTANCES FROM PHELPS, WI")
    print(f"{'='*80}\n")

    for resort_name, data in ski_resort_data.items():
        print(f"{resort_name:25s} | Avg: {data['avg_annual_snowfall']:3d}\" | "
              f"Distance: {data.get('distance_from_phelps_wi', 'N/A')}")
