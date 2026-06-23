#!/usr/bin/env python3
"""Validate that web/public/ is a fully-connected, deployable static site.

Run by .github/workflows/forecast-pages-rebuild.yml on any change to
web/public/. Exits non-zero (failing the check, blocking the deploy) if the
site would be broken in production:

  * a page is missing
  * a page references a local CSS/JS/href that doesn't exist (404)
  * a JS fetch() points at a JSON file that's missing
  * any JSON is malformed
  * the data files the pages read are empty / wrong shape

It only reads files; it never modifies anything.
"""
import json
import re
import sys
from pathlib import Path

PUBLIC = Path("web/public")

# Pages that must exist, and the data each page's script depends on.
REQUIRED_PAGES = ["index.html", "planner.html", "report.html"]

# JSON file -> (top-level key that must be a non-empty list/dict).
JSON_CONTRACTS = {
    "latest_forecast.json": "forecasts",
    "station_data.json": "stations",
    "daily_report.json": "snow_alerts",
}

ASSET_RE = re.compile(r'(?:href|src)\s*=\s*"([^"]+)"')
FETCH_RE = re.compile(r"""fetch\(\s*['"]([^'"]+)['"]""")


def is_local(ref: str) -> bool:
    ref = ref.strip()
    return bool(ref) and not re.match(r"^(https?:)?//|^#|^mailto:|^data:|^\{", ref)


def main() -> int:
    errors = []

    if not PUBLIC.is_dir():
        print(f"FAILED: {PUBLIC} does not exist")
        return 1

    # 1. Required pages present.
    for page in REQUIRED_PAGES:
        if not (PUBLIC / page).is_file():
            errors.append(f"missing page: web/public/{page}")

    # 2. Every local asset/link referenced by a page must resolve.
    for html in PUBLIC.glob("*.html"):
        text = html.read_text(encoding="utf-8", errors="replace")
        for ref in ASSET_RE.findall(text):
            if not is_local(ref):
                continue
            target = (PUBLIC / ref.split("?")[0].split("#")[0]).resolve()
            if not target.is_file():
                errors.append(f"{html.name} references missing file: {ref}")

    # 3. Every fetch() target in the JS must exist and parse.
    fetched = set()
    for js in PUBLIC.glob("*.js"):
        for ref in FETCH_RE.findall(js.read_text(encoding="utf-8", errors="replace")):
            if is_local(ref) and ref.endswith(".json"):
                fetched.add(ref)
                jf = PUBLIC / ref
                if not jf.is_file():
                    errors.append(f"{js.name} fetches missing JSON: {ref}")

    # 4. Every JSON file parses.
    for jf in PUBLIC.glob("*.json"):
        try:
            json.loads(jf.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{jf.name}: invalid JSON ({exc})")

    # 5. The data files the pages depend on have the expected, non-empty shape.
    for name, key in JSON_CONTRACTS.items():
        jf = PUBLIC / name
        if not jf.is_file():
            errors.append(f"missing data file: web/public/{name}")
            continue
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
        except Exception:  # already reported above
            continue
        if not isinstance(data, dict) or not data.get(key):
            errors.append(f"{name}: '{key}' is missing or empty (site would render blank)")

    if errors:
        print("Site validation FAILED:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print("web/public/ is fully connected and deployable:")
    print(f"  pages: {', '.join(REQUIRED_PAGES)}")
    print(f"  data:  {', '.join(sorted(fetched))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
