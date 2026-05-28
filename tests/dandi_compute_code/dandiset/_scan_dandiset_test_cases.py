import json
import logging
import pathlib
from collections.abc import Iterator
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.queue import refresh_queue_state

_SHARED_TEST_SYMBOLS = {
    "CliRunner": CliRunner,
    "_dandicompute_group": _dandicompute_group,
    "json": json,
    "logging": logging,
    "mock": mock,
    "pathlib": pathlib,
    "pytest": pytest,
    "refresh_queue_state": refresh_queue_state,
}


def _build_assets_metadata(
    *,
    content_id: str,
    asset_path: str,
    content_size: int | str | None = None,
    blob_date_modified: str | None = None,
    log_asset_path: str | None = None,
    log_date_modified: str | None = None,
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    """Build source-asset and path timestamp indexes from one synthetic asset.

    Returns a tuple of ``(content_id_to_asset, path_to_date_modified)`` matching
    the structure consumed by queue state writer tests.
    """
    source_asset: dict[str, object] = {"path": asset_path}
    if content_size is not None:
        source_asset["contentSize"] = content_size
    if blob_date_modified is not None:
        source_asset["blobDateModified"] = blob_date_modified
    path_to_date_modified: dict[str, str] = {}
    if log_asset_path is not None and log_date_modified is not None:
        path_to_date_modified[log_asset_path] = log_date_modified
    return {content_id: source_asset}, path_to_date_modified


@pytest.fixture(autouse=True)
def _mock_dandi_api_asset_lookup() -> Iterator[None]:
    """Prevent network calls by defaulting assets metadata loading to empty indexes."""
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=({}, {}),
    ):
        yield
