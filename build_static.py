#!/usr/bin/env python3
"""Build static HTML + JSON for Cloudflare Pages."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path
import json
import shutil
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parent
TEMPLATES = ROOT / "templates"
STATIC = ROOT / "static"
FORECAST_DIR = ROOT / "forecast_output"
DIST = ROOT / "dist"


def load_latest_forecast():
    latest_file = FORECAST_DIR / "latest_forecast.json"
    if not latest_file.exists():
        return None

    try:
        with open(latest_file, "r") as file_handle:
            return json.load(file_handle)
    except (OSError, json.JSONDecodeError):
        return None


def get_forecast_history():
    forecast_files = sorted(FORECAST_DIR.glob("forecast_*.json"), reverse=True)
    history = []

    for file_path in forecast_files[:20]:
        try:
            with open(file_path, "r") as file_handle:
                data = json.load(file_handle)
        except (OSError, json.JSONDecodeError):
            continue

        history.append(
            {
                "filename": file_path.name,
                "generated_at": data.get("generated_at_human", "Unknown"),
                "timestamp": data.get("generated_at", ""),
            }
        )

    return history


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_forecast_stats(forecast_data):
    forecasts = forecast_data.get("forecasts") or []
    if not forecasts:
        return {}

    max_item = None
    max_prob = None
    probs = []
    alert_counts = Counter()

    for item in forecasts:
        level = (item.get("alert_level") or "").lower()
        if level:
            alert_counts[level] += 1

        prob = _safe_float(item.get("probability"))
        if prob is None:
            continue
        probs.append(prob)
        if max_prob is None or prob > max_prob:
            max_prob = prob
            max_item = item

    avg_prob = round(sum(probs) / len(probs), 1) if probs else None
    max_day = "N/A"
    if max_item:
        day_label = max_item.get("day_of_week", "")
        date_label = max_item.get("date", "")
        max_day = f"{day_label} {date_label}".strip() or "N/A"

    return {
        "days": len(forecasts),
        "max_probability": int(round(max_prob)) if max_prob is not None else None,
        "max_day": max_day,
        "avg_probability": avg_prob,
        "alert_counts": {
            "high": alert_counts.get("high", 0),
            "moderate": alert_counts.get("moderate", 0),
            "low": alert_counts.get("low", 0),
            "minimal": alert_counts.get("minimal", 0),
        },
    }


def write_text(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build():
    if DIST.exists():
        shutil.rmtree(DIST)
    DIST.mkdir(parents=True, exist_ok=True)

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.globals["url_for"] = lambda endpoint, filename=None, **_: (
        f"/static/{filename}" if endpoint == "static" and filename else "/"
    )

    now = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    sha = (
        os.getenv("CF_PAGES_COMMIT_SHA")
        or os.getenv("GITHUB_SHA")
        or os.getenv("GIT_COMMIT_SHA")
    )
    build_meta = {
        "version": sha[:7] if sha else "local",
        "badge_url": os.getenv("BUILD_BADGE_URL"),
    }
    history = get_forecast_history()
    forecast = load_latest_forecast()

    if forecast:
        forecast_stats = build_forecast_stats(forecast)
        dashboard_html = env.get_template("dashboard.html").render(
            forecast=forecast,
            forecast_stats=forecast_stats,
            current_time=now,
            render_time=now,
            build_meta=build_meta,
        )
    else:
        dashboard_html = env.get_template("no_forecast.html").render(
            render_time=now,
            build_meta=build_meta,
        )

    write_text(DIST / "index.html", dashboard_html)

    about_html = env.get_template("about.html").render(
        render_time=now,
        build_meta=build_meta,
    )
    write_text(DIST / "about" / "index.html", about_html)

    history_html = env.get_template("history.html").render(
        history=history,
        render_time=now,
        build_meta=build_meta,
    )
    write_text(DIST / "history" / "index.html", history_html)

    if STATIC.exists():
        shutil.copytree(STATIC, DIST / "static", dirs_exist_ok=True)

    forecast_payload = forecast if forecast is not None else {"error": "No forecast available"}
    write_json(DIST / "api" / "forecast.json", forecast_payload)
    write_json(DIST / "api" / "history.json", history)

    print("Static build complete: dist/")


if __name__ == "__main__":
    build()
