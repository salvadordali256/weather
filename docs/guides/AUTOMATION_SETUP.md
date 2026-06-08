# Weather Dashboard Automation Setup

## Automatic Daily Updates

The system is configured to automatically update at **17:30 (5:30 PM)** every day.

### What Happens Automatically

1. **Regional Station Data Update** - Fetches last 7 days of data for:
   - Winnipeg, Thunder Bay (Alberta Clipper indicators)
   - Marquette, Duluth (Lake Effect indicators)
   - Green Bay, Iron Mountain (Regional indicators)
   - Phelps, Land O'Lakes (Target locations)

2. **Global Predictor Data Update** - Fetches last 14 days of data for:
   - Sapporo, Japan (6-day lead)
   - Chamonix, France (5-day lead)
   - Irkutsk, Russia (7-day lead)
   - Zermatt, Switzerland (5-day lead)
   - Niigata, Japan (3-day lead)

3. **Forecast Generation** - Creates new 7-day forecast using ensemble model

4. **Dashboard Update** - Automatically displays latest forecast

### Cron Schedule

```
30 17 * * * /Users/kyle.jurgens/weather/daily_update.sh
```

This runs at 17:30 every day.

### Log Files

Logs are stored in: `/Users/kyle.jurgens/weather/logs/`
- File format: `daily_update_YYYYMMDD.log`
- Each run creates a new log file
- Check logs to verify successful updates

### Manual Commands

Run update manually anytime:
```bash
cd /Users/kyle.jurgens/weather
./daily_update.sh
```

Run individual components:
```bash
source venv/bin/activate
python update_recent_data.py           # Regional stations only
python update_global_predictors.py     # Global predictors only
python daily_automated_forecast.py     # Forecast generation only
```

### Managing the Cron Job

**View current schedule:**
```bash
crontab -l
```

**Edit schedule:**
```bash
crontab -e
```

**Disable automatic updates:**
```bash
crontab -r  # Removes all cron jobs (be careful!)
# Or edit with 'crontab -e' and comment out the line with #
```

**Change update time:**
Edit the cron job time format: `MIN HOUR * * *`
- `30 17` = 17:30 (5:30 PM)
- `0 6` = 06:00 (6:00 AM)
- `45 20` = 20:45 (8:45 PM)

Examples:
```bash
0 6 * * *    # Run at 06:00 daily
30 17 * * *  # Run at 17:30 daily (current)
0 */6 * * *  # Run every 6 hours
```

### Dashboard Access

The dashboard is served by Flask and needs to be running:

**Start dashboard:**
```bash
cd /Users/kyle.jurgens/weather
source venv/bin/activate
python forecast_web_dashboard.py
```

**Access at:**
- Main: http://localhost:5000
- History: http://localhost:5000/history
- API: http://localhost:5000/api/forecast

### Dashboard Auto-Start (Optional)

To start the dashboard automatically on system boot, you can:

1. **Using launchd (macOS):**
   Create file: `~/Library/LaunchAgents/com.weather.dashboard.plist`

2. **Using a shell startup script:**
   Add to `~/.zshrc` or `~/.bash_profile`

3. **Using screen/tmux:**
   Start in a persistent terminal session

### Troubleshooting

**Check if cron is working:**
```bash
# View recent system logs
grep CRON /var/log/system.log

# Check log files
tail -f logs/daily_update_$(date +%Y%m%d).log
```

**Common issues:**
- Cron environment doesn't load your PATH → Use absolute paths in script
- Python venv not activated → Script handles this automatically
- API rate limits → Script includes delays between requests
- Network issues → Check internet connection

**Test the script manually:**
```bash
./daily_update.sh
cat logs/daily_update_$(date +%Y%m%d).log
```

### Data Freshness

- **Regional data**: Updates last 7 days (handles any missed days)
- **Global data**: Updates last 14 days (ensures long lead-time coverage)
- **Forecast**: 7-day rolling outlook
- **API source**: Open-Meteo (free, no API key needed)

### System Requirements

- Python 3.11+
- Virtual environment: `/Users/kyle.jurgens/weather/venv`
- Internet connection for API calls
- ~10MB disk space per year of logs
