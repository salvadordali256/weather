"""Root shim — see scripts/collect/collect_resort_reports.py"""
import runpy
runpy.run_module("scripts.collect.collect_resort_reports", run_name="__main__", alter_sys=True)
