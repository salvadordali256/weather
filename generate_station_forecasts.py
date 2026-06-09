"""Root shim — see scripts/generate/generate_station_forecasts.py"""
import runpy
runpy.run_module("scripts.generate.generate_station_forecasts", run_name="__main__", alter_sys=True)
