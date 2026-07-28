"""
Including these directly within the top-level `__init__.py` makes them visible to autocompletion.

But we only want the imports to trigger, not for them to actually be exposed.
"""

import importlib.metadata

from .aind_ephys_pipeline import prepare_aind_ephys_job

try:
    __version__ = importlib.metadata.version(distribution_name="dandi-compute-code")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
