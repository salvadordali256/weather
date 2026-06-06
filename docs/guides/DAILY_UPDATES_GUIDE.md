# Daily Snow Forecast Updates

## Quick Start

To get your daily snow forecast update, simply run:

```bash
./snow-update.sh
```

Or from any directory:

```bash
/Users/kyle.jurgens/weather/snow-update.sh
```

## What You'll Get

Each update provides:

- **7-Day Snow Forecast** - How much snow in next week
- **Days 8-14 Outlook** - Medium-range trends
- **Temperature Forecast** - Cold snaps and mild periods
- **Day-by-Day Details** - Specific forecasts for each day
- **Ski Conditions** - How conditions will be for skiing
- **Biggest Snow Days** - Which days to watch for major snow

## Automation Options

### Option 1: Manual Daily Check (Recommended)

Just run the script whenever you want an update:
```bash
./snow-update.sh
```

### Option 2: Automated Daily Updates with Cron

To get automatic updates every morning at 7 AM:

1. Open your crontab:
   ```bash
   crontab -e
   ```

2. Add this line:
   ```
   0 7 * * * /Users/kyle.jurgens/weather/snow-update.sh >> /Users/kyle.jurgens/weather/daily_update.log 2>&1
   ```

3. Save and exit

This will run the update at 7 AM daily and save output to `daily_update.log`.

### Option 3: Create a Desktop Shortcut (macOS)

1. Open **Automator**
2. Create new **Application**
3. Add "Run Shell Script" action
4. Paste: `/Users/kyle.jurgens/weather/snow-update.sh`
5. Save to Desktop as "Snow Update"
6. Double-click anytime to run

### Option 4: Set a Reminder

Set a daily reminder on your phone/computer to run the script manually.

## Tracking History

The script saves each update to `daily_update_history.json` so you can:
- Track forecast changes over time
- See how accurate forecasts were
- Spot trends (e.g., "forecast keeps increasing snow amounts")

## Example Output

```
NEXT 7 DAYS (Short-range forecast):
  Expected snowfall: 2.3 inches
  Status: 🌤️  Minimal snow expected

  Biggest snow days:
    • Saturday, January 10: 1.4 inches

TEMPERATURE OUTLOOK (Next 7 days):
  Average overnight low: 19°F
  Coldest expected: 13°F
  Status: 🌡️  Cold

🎿 SKI CONDITIONS OUTLOOK
  LIMITED - Minimal new snow
  New base: ~2 inches over next week
  Rely on existing base
```

## When to Pay Extra Attention

🚨 **If you see "MAJOR SNOW EXPECTED"** - Big storm coming, plan accordingly

❄️ **If you see "Significant snow expected"** - Good powder day ahead

🥶 **If you see "EXTREME COLD expected"** - Temps below -20°F, dangerous conditions

## Pro Tips

1. **Run it daily at the same time** - Easier to spot forecast changes
2. **Check morning and evening** - Forecasts can change during the day
3. **Compare to yesterday** - Did snow amounts increase or decrease?
4. **Watch the 7-day totals** - More reliable than individual day forecasts
5. **Trust 0-3 day forecasts most** - Accuracy drops after 3 days

## Files Created

- `daily_snow_update.py` - Main update script
- `snow-update.sh` - Easy run command
- `daily_update_history.json` - History of all updates (one line per update)
- `daily_update.log` - Output log (if using cron)

## Troubleshooting

**"No module named 'requests'"**
- Run: `source venv/bin/activate && pip install requests`

**"Permission denied"**
- Run: `chmod +x snow-update.sh`

**No internet connection**
- Script requires internet to fetch forecast data

## What Happened to the Original Forecast?

The original December 24 forecast predicted 18-28" for Jan 15-20. Current models show 0-3".

Key lesson learned: **Trust short-range forecasts (0-7 days) over long-range pattern predictions.**

This daily update script focuses on the reliable 7-day forecast window while giving you a heads-up on the 8-14 day outlook.

---

**Last Updated:** January 5, 2026
