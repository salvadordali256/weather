#!/bin/bash
# Daily Snow Forecast Update Script
# Run this anytime to get latest forecast

cd "$(dirname "$0")"
source venv/bin/activate
python daily_snow_update.py
