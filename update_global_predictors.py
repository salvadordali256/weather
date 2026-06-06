"""Root shim — see scripts/pipeline/update_global_predictors.py"""
import runpy
runpy.run_module("scripts.pipeline.update_global_predictors", run_name="__main__", alter_sys=True)
