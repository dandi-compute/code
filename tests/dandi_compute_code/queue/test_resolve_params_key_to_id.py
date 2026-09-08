import importlib.resources
import json

import pytest

from dandi_compute_code.queue import QueueState


def _get_default_params_id() -> str:
    params_registry_path = importlib.resources.files("dandi_compute_code.aind_ephys_pipeline").joinpath(
        "registries/registered_params.json"
    )
    return json.loads(params_registry_path.read_text())["default"]["md5"][:7]


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_aind_ephys_default() -> None:
    """_resolve_params_key_to_id returns the 7-char hash for a known aind+ephys key."""
    result = QueueState.resolve_params_key_to_id("aind+ephys", "default")
    assert result == _get_default_params_id()


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_unknown_pipeline_returns_key() -> None:
    """_resolve_params_key_to_id returns the key unchanged for an unknown pipeline."""
    result = QueueState.resolve_params_key_to_id("unknown-pipeline", "default")
    assert result == "default"


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_already_hash_passthrough() -> None:
    """_resolve_params_key_to_id returns the value unchanged if it is already an ID (not a registered key)."""
    result = QueueState.resolve_params_key_to_id("aind+ephys", "98fd947")
    # '98fd947' is not a registered key name, so it is returned as-is
    assert result == "98fd947"
