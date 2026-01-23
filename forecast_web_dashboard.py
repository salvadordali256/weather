#!/usr/bin/env python3
"""
Forecast Web Dashboard
Flask web application to display Wisconsin snowfall forecasts

Access at: http://localhost:5000
"""

from flask import Flask, render_template, jsonify
from pathlib import Path
from collections import Counter
import json
from datetime import datetime
import os

app = Flask(__name__)

# Configuration
FORECAST_DIR = Path('forecast_output')
TEMPLATE_DIR = Path('templates')
STATIC_DIR = Path('static')

# Create directories if they don't exist
FORECAST_DIR.mkdir(exist_ok=True)
TEMPLATE_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

def load_latest_forecast():
    """Load the latest forecast from JSON"""
    latest_file = FORECAST_DIR / 'latest_forecast.json'

    if not latest_file.exists():
        return None

    try:
        with open(latest_file, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as exc:
        app.logger.error("Latest forecast JSON decode failed: %s", exc)
        return None
    except OSError as exc:
        app.logger.error("Latest forecast read failed: %s", exc)
        return None

def get_forecast_history():
    """Get list of all historical forecasts"""
    forecast_files = sorted(FORECAST_DIR.glob('forecast_*.json'), reverse=True)

    history = []
    for file in forecast_files[:20]:  # Last 20 forecasts
        try:
            with open(file, 'r') as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            app.logger.warning("History JSON decode failed for %s: %s", file, exc)
            continue
        except OSError as exc:
            app.logger.warning("History read failed for %s: %s", file, exc)
            continue

        history.append({
            'filename': file.name,
            'generated_at': data.get('generated_at_human', 'Unknown'),
            'timestamp': data.get('generated_at', '')
        })

    return history

def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def build_forecast_stats(forecast_data):
    forecasts = forecast_data.get('forecasts') or []
    if not forecasts:
        return {}

    max_item = None
    max_prob = None
    probs = []
    alert_counts = Counter()

    for item in forecasts:
        level = (item.get('alert_level') or '').lower()
        if level:
            alert_counts[level] += 1

        prob = _safe_float(item.get('probability'))
        if prob is None:
            continue
        probs.append(prob)
        if max_prob is None or prob > max_prob:
            max_prob = prob
            max_item = item

    avg_prob = round(sum(probs) / len(probs), 1) if probs else None
    max_day = "N/A"
    if max_item:
        day_label = max_item.get('day_of_week', '')
        date_label = max_item.get('date', '')
        max_day = f"{day_label} {date_label}".strip() or "N/A"

    return {
        'days': len(forecasts),
        'max_probability': int(round(max_prob)) if max_prob is not None else None,
        'max_day': max_day,
        'avg_probability': avg_prob,
        'alert_counts': {
            'high': alert_counts.get('high', 0),
            'moderate': alert_counts.get('moderate', 0),
            'low': alert_counts.get('low', 0),
            'minimal': alert_counts.get('minimal', 0),
        }
    }

def get_build_meta():
    sha = (
        os.getenv("CF_PAGES_COMMIT_SHA")
        or os.getenv("GITHUB_SHA")
        or os.getenv("GIT_COMMIT_SHA")
    )
    version = sha[:7] if sha else "local"
    return {
        "version": version,
        "badge_url": os.getenv("BUILD_BADGE_URL"),
    }

@app.context_processor
def inject_globals():
    return {
        'render_time': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
        'build_meta': get_build_meta(),
    }

@app.route('/')
def index():
    """Main dashboard page"""
    forecast_data = load_latest_forecast()

    if forecast_data is None:
        return render_template('no_forecast.html')

    forecast_stats = build_forecast_stats(forecast_data)
    return render_template('dashboard.html',
                          forecast=forecast_data,
                          forecast_stats=forecast_stats,
                          current_time=datetime.now().strftime('%B %d, %Y at %I:%M %p'))

@app.route('/api/forecast')
@app.route('/api/forecast.json')
def api_forecast():
    """API endpoint for latest forecast"""
    forecast_data = load_latest_forecast()

    if forecast_data is None:
        return jsonify({'error': 'No forecast available'}), 404

    return jsonify(forecast_data)

@app.route('/api/history')
@app.route('/api/history.json')
def api_history():
    """API endpoint for forecast history"""
    history = get_forecast_history()
    return jsonify(history)

@app.route('/history')
def history():
    """Historical forecasts page"""
    history_data = get_forecast_history()
    return render_template('history.html', history=history_data)

@app.route('/about')
def about():
    """About page"""
    return render_template('about.html')

if __name__ == '__main__':
    print("\n" + "="*80)
    print("WISCONSIN SNOWFALL FORECAST DASHBOARD")
    print("="*80)
    print("\nStarting web server...")
    print("Access dashboard at: http://localhost:5000")
    print("\nPress Ctrl+C to stop")
    print("="*80 + "\n")

    debug_mode = os.getenv("FLASK_DEBUG", "false").lower() in {"1", "true", "yes"}
    host = os.getenv("FLASK_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_PORT", "5000"))
    app.run(debug=debug_mode, host=host, port=port)
