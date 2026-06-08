"""Root shim — see scripts/pipeline/update_recent_data.py"""
import runpy
runpy.run_module("scripts.pipeline.update_recent_data", run_name="__main__", alter_sys=True)
