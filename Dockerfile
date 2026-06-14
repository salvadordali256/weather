FROM python:3.11-slim

WORKDIR /app

# ---- Python deps -------------------------------------------------------------
# Copy only the dependency manifests first so this layer is cached across
# source-only changes.
COPY requirements.txt pyproject.toml ./

RUN pip install --no-cache-dir -r requirements.txt

# ---- Application source -------------------------------------------------------
# After the reorg the importable library lives in src/snowforecast a