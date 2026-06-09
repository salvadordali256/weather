"""Root shim — see scripts/collect/collect_hourly_data.py"""
import runpy
runpy.run_module("scripts.collect.collect_hourly_data", run_name="__main__", alter_sys=True)
