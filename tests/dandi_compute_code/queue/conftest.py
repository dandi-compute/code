"""
Shared fixtures for the queue test suite.

The literal example queue lives in ``example_state_files/state.jsonl`` and is
loaded and materialized by the plain helpers in ``_example_queue.py``. These
fixtures provide only what genuinely needs pytest: temporary directories and
environment or network setup.
"""

import json
import os
import pathlib
from collections.abc import Iterator
from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetsJsonldMetadata

#: Consolidated queue config used by tests that need a populated queue directory.
EXAMPLE_QUEUE_CONFIG = {
    "pipelines": {
        "test": {
            "version_priority": ["v1.0"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 2,
            "asset_overrides": {"asset-aaa": 1},
            "max_fail_per_dandiset": 2,
        }
    }
}


@pytest.fixture(autouse=True)
def mock_dandi_assets_metadata() -> Iterator[None]:
    """
    Default the DANDI ``assets.jsonld`` loaders to empty so no test hits the network.

    ``write_queue_state`` (and therefore ``queue refresh``) fetches assets metadata
    from the DANDI archive. This guard makes that return empty by default. Tests
    that need specific metadata override these with their own ``mock.patch``.
    """
    empty_metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={})
    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=empty_metadata
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=empty_metadata,
        ),
    ):
        yield


@pytest.fixture
def queue_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A queue directory containing the example ``queue_config.json``."""
    directory = tmp_path / "queue"
    directory.mkdir()
    (directory / "queue_config.json").write_text(json.dumps(EXAMPLE_QUEUE_CONFIG))
    return directory


@pytest.fixture
def processing_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A directory for the temporary per-job working trees used during submission."""
    directory = tmp_path / "processing"
    directory.mkdir()
    return directory


@pytest.fixture
def dandi_api_key() -> Iterator[None]:
    """Provide a dummy DANDI_API_KEY for helpers that require it to be set."""
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        yield
