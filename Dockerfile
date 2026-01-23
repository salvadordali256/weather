FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY forecast_web_dashboard.py .
COPY templates/ templates/
COPY forecast_output/ forecast_output/

# Create directories that might be needed
RUN mkdir -p static forecast_output

# Expose Flask port
EXPOSE 5000

# Run the dashboard
CMD ["python", "forecast_web_dashboard.py"]
