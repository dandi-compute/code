import os
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import QueueState

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"


@pytest.mark.ai_generated
def test_write_dandiset_state_table_builds_state_and_uploads(tmp_path: pathlib.Path) -> None:
    """write_dandiset_state_table builds the state from the given Dandiset and uploads a TSV."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1/code/submit.sh"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            attempt_path: AssetMetadata(
                path=attempt_path,
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=1,
                content_id="attempt-code-id",
            )
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="content-id-1",
            )
        },
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
        mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file") as mock_write_file,
    ):
        QueueState.write_dandiset_state_table(dandiset_id=_JOB_CAPSULES_DANDISET_ID)

    mock_write_file.assert_called_once()
    call_kwargs = mock_write_file.call_args.kwargs
    assert call_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert call_kwargs["relative_path"] == "derivatives/state.tsv"
    assert "dandiset_id\t" in call_kwargs["content"].splitlines()[0]
    assert source_path in call_kwargs["content"]


@pytest.mark.ai_generated
def test_write_dandiset_state_table_empty_state_writes_header_only() -> None:
    """write_dandiset_state_table uploads a header-only table when there are no entries."""
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
        mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file") as mock_write_file,
    ):
        QueueState.write_dandiset_state_table(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)

    call_kwargs = mock_write_file.call_args.kwargs
    assert call_kwargs["dandiset_id"] == _FAILED_RUNS_ARCHIVE_DANDISET_ID
    assert len(call_kwargs["content"].splitlines()) == 1


@pytest.mark.ai_generated
def test_cli_queue_publish_tables_fails_without_api_key() -> None:
    """dandicompute queue publish-tables errors immediately when DANDI_API_KEY is missing."""
    runner = CliRunner()
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(_dandicompute_group, ["queue", "publish-tables"])
    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_queue_publish_tables_fails_without_dandi_devel() -> None:
    """dandicompute queue publish-tables errors when DANDI_DEVEL is missing."""
    runner = CliRunner()
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}, clear=True):
        result = runner.invoke(_dandicompute_group, ["queue", "publish-tables"])
    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output


@pytest.mark.ai_generated
def test_cli_queue_publish_tables_publishes_source_and_archived() -> None:
    """dandicompute queue publish-tables writes the table into both the source and archived Dandisets."""
    runner = CliRunner()
    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.QueueState.write_dandiset_state_table") as mock_publish,
    ):
        result = runner.invoke(_dandicompute_group, ["queue", "publish-tables"])
    assert result.exit_code == 0, result.output
    assert mock_publish.call_count == 2
    called_dandiset_ids = {call.kwargs["dandiset_id"] for call in mock_publish.call_args_list}
    assert called_dandiset_ids == {_JOB_CAPSULES_DANDISET_ID, _FAILED_RUNS_ARCHIVE_DANDISET_ID}


@pytest.mark.ai_generated
def test_cli_queue_publish_tables_forwards_custom_dandiset_ids() -> None:
    """dandicompute queue publish-tables forwards --dandiset-id/--archive-dandiset-id."""
    runner = CliRunner()
    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.QueueState.write_dandiset_state_table") as mock_publish,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "publish-tables", "--dandiset-id", "000123", "--archive-dandiset-id", "000456"],
        )
    assert result.exit_code == 0, result.output
    called_dandiset_ids = {call.kwargs["dandiset_id"] for call in mock_publish.call_args_list}
    assert called_dandiset_ids == {"000123", "000456"}
