"""Root shim — see scripts/generate/generate_daily_report.py"""
import runpy
runpy.run_module("scripts.generate.generate_daily_report", run_name="__main__", alter_sys=True)
