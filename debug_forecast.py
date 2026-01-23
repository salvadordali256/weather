from datetime import datetime, timedelta

# Mount Baker has lag = -4 (Wisconsin LEADS by 4 days)
# So to check Mount Baker, we need to look 4 days in the FUTURE, which doesn't exist!

# Current logic issue:
current_date = datetime.now()
lag = -4  # Mount Baker lag

# The script does: days_ago = lag
# For lag = -4, days_ago = -4
# Then: target_date = current_date - timedelta(days=-4) = current_date + 4 days = FUTURE!

print(f"Current date: {current_date.strftime('%Y-%m-%d')}")
print(f"Lag: {lag}")
print(f"Days ago calculation: {lag}")
print(f"Target date: {(current_date - timedelta(days=lag)).strftime('%Y-%m-%d')}")
print(f"\nThis looks 4 days in the FUTURE, which has no data!")

# Correct approach for NEGATIVE lags (Wisconsin leads):
# These stations aren't useful for FORECASTING Wisconsin
# They show pattern correlation, not prediction
# We should either:
# 1. Skip them (not useful for forecasting)
# 2. Use them differently (pattern validation, not prediction)

print(f"\n📌 KEY INSIGHT:")
print(f"Stations with negative lag (WI leads) can't predict Wisconsin!")
print(f"They're pattern indicators, not forecasters.")
