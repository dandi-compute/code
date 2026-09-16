import importlib.util

# The LFP pipeline depends on the runtime environment provided by the pipeline
# container (see containers/) or `pip install .[lfp]`. When that environment is
# not present, skip collecting these tests instead of failing on import.
_LFP_RUNTIME_MODULES = ("numpy", "jsonschema", "spikeinterface", "neuroconv", "pynwb")

if any(importlib.util.find_spec(module_name) is None for module_name in _LFP_RUNTIME_MODULES):
    collect_ignore_glob = ["test_*.py"]
