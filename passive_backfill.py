"""Root shim — see scripts/collect/passive_backfill.py"""
import runpy
runpy.run_module("scripts.collect.passive_backfill", run_name="__main__", alter_sys=True)
