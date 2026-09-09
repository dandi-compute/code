import json
import pathlib
import re

_AIND_EPHYS_PARAMS_REGISTRY_PATH = (
    pathlib.Path(__file__).parent.parent / "aind_ephys_pipeline" / "registries" / "registered_params.json"
)
_QUEUE_CONFIG_SCHEMA_PATH = pathlib.Path(__file__).parent / "schemas" / "queue_config.linkml.yaml"
# Packaged pipeline configuration, committed directly to this repo. This is the canonical
# source of truth for the queue's pipeline definitions -- it replaces the ``queue_config.json``
# that previously lived only in the (soon to be retired) dandi-compute/queue repository. There
# is no local override for this file; see ``_load_queue_config``.
_PACKAGED_PIPELINE_CONFIGS_PATH = pathlib.Path(__file__).parent / "pipeline_configs.json"
# TODO: consolidate this RE with the other globals and generalize to any job capsule
_FLAT_ATTEMPT_DIR_RE = re.compile(r"^version-(?P<version>.+?)_codebase-[^_]+_params-[^_]+_config-[^_]+_attempt-\d+$")
_DURATION_PART_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ms|s|m|h|d)\b")
TEST_QUEUE_CONTENT_ID = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
_QUALIFYING_AIND_CONTENT_IDS_URL = (
    "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/dist/"
    "derivatives/qualifying_aind_content_ids.jsonl.gz"
)

try:
    _AIND_EPHYS_PARAMS_REGISTRY: dict = json.loads(_AIND_EPHYS_PARAMS_REGISTRY_PATH.read_text())
except (OSError, json.JSONDecodeError):
    _AIND_EPHYS_PARAMS_REGISTRY = {}
