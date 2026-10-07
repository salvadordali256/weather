#!/usr/bin/env python3
"""
Generate the NWP-based 7-day snowfall probability forecast.

Runs side-by-side with daily_automated_forecast.py and writes to a separate
file, latest_forecast_nwp.json, which push_forecast.sh does NOT publish. It
exists so the two engines can be compared on live days before any cutover.

Usage:
    python scripts/pipeline/nwp_forecast.py
    python scripts/pipeline/nwp_forecast.py --output-dir forecast_output
"""

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from snowforecast.engines.nwp_snowfall_forecast import NwpSnowfallForecast

load_dotenv()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", default=os.environ.get("FORECAST_OUTPUT_DIR", "forecast_output"))
    args = parser.parse_args()

    forecast = NwpSnowfallForecast().generate()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "latest_forecast_nwp.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(forecast, indent=2))
    tmp.replace(out)  # atomic: a failed run never leaves a half-written file

    print(f"NWP forecast written to {out}")
    for d in forecast["forecasts"]:
        snow = "--" if d["forecast_snowfall_in"] is None else f'{d["forecast_snowfall_in"]}"'
        print(f'  {d["period_label"]:<32} {d["probability"]:>3}%  forecast {snow:<6} ({d["basis"]})')


if __name__ == "__main__":
    main()
