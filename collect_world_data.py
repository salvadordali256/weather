"""Root shim — see scripts/pipeline/collect_world_data.py (argparse preserved via runpy)"""
import runpy
runpy.run_module("scripts.pipeline.collect_world_data", run_name="__main__", alter_sys=True)
