FROM python:3.11-slim

WORKDIR /app

# ---- Python deps -------------------------------------------------------------
# Copy only the dependency manifests first so this layer caches across
# source-only changes.
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# ---- Application source -------------------------------------------------------
# After the reorg the importable library lives in src/snowforecast and the
# Flask dashboard lives in web/apps. Copy both, plus templates/static.
COPY src/ src/
COPY web/ web/

# Install the package so `import snowforecast` resolves like it does on the
# NAS / CI (`pip install -e .`).
RUN pip install --no-cache-dir -e .

# The dashboard reads forecast_output/latest_forecast.json. That dir is
# gitignored (pipeline-generated), so seed it from the tracked copy in
# web/public/ to guarantee the container has something to render.
RUN mkdir -p forecast_output \
    && if [ -f web/public/latest_forecast.json ]; then \
         cp web/public/latest_forecast.json forecast_output/latest_forecast.json; \
       fi

EXPOSE 5000
ENV FORECAST_OUTPUT_DIR=/app/forecast_output \
    FLASK_HOST=0.0.0.0 \
    FLASK_PORT=5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:5000/api/forecast').status==200 else 1)" || exit 1

# Serve with gunicorn (in requirements.txt). The Flask app object is `app`
# in web/apps/forecast_web_dashboard.py.
CMD ["gunicorn", "--chdir", "web/apps", "--bind", "0.0.0.0:5000", "--workers", "2", "forecast_web_dashboard:app"]
