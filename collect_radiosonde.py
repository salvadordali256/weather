"""Root shim — see scripts/collect/collect_radiosonde.py"""
import runpy
runpy.run_module("scripts.collect.collect_radiosonde", run_name="__main__", alter_sys=True)
