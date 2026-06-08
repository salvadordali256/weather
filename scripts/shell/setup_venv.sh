#!/bin/bash
#
# Setup Virtual Environment for Forecast System
# Creates isolated Python environment with all dependencies
#

echo "================================================================================"
echo "WISCONSIN SNOWFALL FORECAST - VIRTUAL ENVIRONMENT SETUP"
echo "================================================================================"
echo ""

# Get the current directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Setting up virtual environment in: $SCRIPT_DIR/venv"
echo ""

# Create virtual environment
python3 -m venv "$SCRIPT_DIR/venv"

if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to create virtual environment"
    exit 1
fi

echo "✅ Virtual environment created"
echo ""

# Activate virtual environment
source "$SCRIPT_DIR/venv/bin/activate"

echo "Installing dependencies..."
echo ""

# Upgrade pip
pip install --upgrade pip --quiet

# Install dependencies
pip install flask pandas numpy --quiet

if [ $? -eq 0 ]; then
    echo "✅ Dependencies installed successfully!"
    echo ""
else
    echo "❌ Error: Failed to install dependencies"
    exit 1
fi

echo "================================================================================"
echo "SETUP COMPLETE"
echo "================================================================================"
echo ""
echo "To use the forecast system:"
echo ""
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Generate a forecast:"
echo "   python3 daily_automated_forecast.py"
echo ""
echo "3. Start the web dashboard:"
echo "   python3 forecast_web_dashboard.py"
echo ""
echo "4. View dashboard at:"
echo "   http://localhost:5000"
echo ""
echo "To deactivate when done:"
echo "   deactivate"
echo ""
echo "================================================================================"
