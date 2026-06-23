"""
Shared fixtures for the OOP ``QueueState`` model test suite.

This mirrors the queue suite's ``conftest.py`` but the network guard targets the
binding used by the model (:mod:`dandi_compute_code.queue._queue_state`) rather
than the legacy free-function module.
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

    ``QueueState.write_state`` (and ``from_assets`` / ``pending_code_dirs``) fetch
    assets metadata from the DANDI archive via the ``_queue_state`` binding. This
    guard makes that return empty by default. Tests that need specific metadata
    override these with their own ``mock.patch``.
    """
    empty_metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={})
    with (
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=empty_metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
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
