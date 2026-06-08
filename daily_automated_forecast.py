"""Root shim — keeps the NAS crontab working after reorg.
Real script: scripts/pipeline/daily_automated_forecast.py
"""
import runpy
runpy.run_module("scripts.pipeline.daily_automated_forecast", run_name="__main__", alter_sys=True)
