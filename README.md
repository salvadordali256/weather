# Wisconsin Snowfall Forecast (Cloudflare Pages)

This repository includes a static build pipeline for Cloudflare Pages. The Flask app is used locally, while Pages hosts the rendered HTML + JSON output from `build_static.py`.

## Cloudflare Pages (Git integration)

1) Push to GitHub:
```
git remote add origin <your-repo-url>
git add .
git commit -m "Prepare for Pages"
git push -u origin main
```

2) Create a Pages project:
- Cloudflare Dashboard -> Workers & Pages -> Create application -> Pages
- Connect GitHub and select this repo/branch

3) Build settings:
- Build command:
```
python3 -m pip install -r requirements.txt && python3 build_static.py
```
- Build output directory: `dist`
- Root directory: (leave blank unless you are in a monorepo)

4) Deploy: Click **Save and Deploy**.

## Scheduled refresh (GitHub Actions)

The workflow `.github/workflows/forecast-pages-rebuild.yml` runs daily and on-demand:
- Generates a new forecast (`daily_automated_forecast.py`)
- Rebuilds the static site (`build_static.py`)
- Commits updated forecast data back to the repo

If you want a different run time, edit the cron in:
```
.github/workflows/forecast-pages-rebuild.yml
```

## Build status badge + version stamp

The footer includes a build version stamp and optional badge image.

Set this environment variable in Cloudflare Pages (Project -> Settings -> Environment variables):
```
BUILD_BADGE_URL=https://github.com/<OWNER>/<REPO>/actions/workflows/forecast-pages-rebuild.yml/badge.svg
```

The build version uses these environment variables if available:
- `CF_PAGES_COMMIT_SHA` (Cloudflare Pages)
- `GITHUB_SHA` (GitHub Actions)

## Local preview

Run the Flask app locally:
```
source venv/bin/activate
python forecast_web_dashboard.py
```

Generate static output locally:
```
source venv/bin/activate
python build_static.py
```
