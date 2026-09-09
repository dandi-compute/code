import json
import os
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import JobEntry, JobInfo, QueueState

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"
_DANDI_ENV = {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}


@pytest.mark.ai_generated
def test_cli_queue_refresh_with_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh writes state.tsv from assets metadata."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0"],
                        "params_priority": ["default"],
                        "max_fail_per_dandiset": 3,
                    }
                }
            }
        )
    )
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-default_config-0d4bf36_attempt-1"
    )
    runner = CliRunner()
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                        path=f"{attempt_prefix}/code/submit.sh",
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="attempt-code-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1234,
                        content_id=content_id,
                    )
                },
            ),
        ),
        mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file") as mock_write_file,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir)],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output
    assert (queue_dir / "state.tsv").exists()
    state_records = [entry.to_dict() for entry in QueueState.from_tsv(queue_dir / "state.tsv")]
    assert len(state_records) == 1
    assert state_records[0]["dandiset_id"] == "001697"
    assert state_records[0]["content_id"] == content_id
    assert mock_write_file.call_count == 2


@pytest.mark.ai_generated
def test_cli_queue_refresh_publishes_tables_to_source_and_archived(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh republishes derivatives/state.tsv into both Dandisets."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    runner = CliRunner()
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
        mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file") as mock_write_file,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir)],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output
    called_dandiset_ids = {call.kwargs["dandiset_id"] for call in mock_write_file.call_args_list}
    assert called_dandiset_ids == {_JOB_CAPSULES_DANDISET_ID, _FAILED_RUNS_ARCHIVE_DANDISET_ID}
    assert all(call.kwargs["relative_path"] == "derivatives/state.tsv" for call in mock_write_file.call_args_list)


@pytest.mark.ai_generated
def test_cli_queue_refresh_forwards_custom_dandiset_ids(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh forwards --dandiset-id/--archive-dandiset-id to both the local and table writes."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    runner = CliRunner()
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
        mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file") as mock_write_file,
    ):
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "refresh",
                "--queue",
                str(queue_dir),
                "--dandiset-id",
                "000123",
                "--archive-dandiset-id",
                "000456",
            ],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output
    called_dandiset_ids = {call.kwargs["dandiset_id"] for call in mock_write_file.call_args_list}
    assert called_dandiset_ids == {"000123", "000456"}


@pytest.mark.ai_generated
def test_cli_queue_refresh_does_not_require_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh runs without --dandiset."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    entry = JobEntry(
        job=JobInfo(
            dandiset_id="000001",
            dandi_path="sub-mouse01",
            pipeline="aind+ephys",
            version="v1.0",
            params="abc1234",
            config="def5678",
            attempt=1,
            codebase="v0.3.0",
        ),
        content_id=None,
        asset_size_bytes=None,
        has_code=True,
        has_output=False,
        has_logs=False,
        created_at="2024-01-01T00:00:00+00:00",
    )
    QueueState(entries=[entry]).to_tsv(queue_dir / "state.tsv")
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0+abc1234+def5678"],
                        "params_priority": ["default"],
                        "max_fail_per_dandiset": 3,
                    }
                }
            }
        )
    )
    runner = CliRunner()
    with mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file"):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir)],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_fails_without_api_key() -> None:
    """dandicompute queue refresh errors immediately when DANDI_API_KEY is missing (it publishes to DANDI)."""
    runner = CliRunner()
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(_dandicompute_group, ["queue", "refresh", "--queue", "."])
    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_fails_without_dandi_devel() -> None:
    """dandicompute queue refresh errors when DANDI_DEVEL is missing."""
    runner = CliRunner()
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}, clear=True):
        result = runner.invoke(_dandicompute_group, ["queue", "refresh", "--queue", "."])
    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_falls_back_to_packaged_pipeline_config(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh uses the packaged pipeline config when --queue has none."""
    queue_dir = tmp_path / "queue_directory"
    queue_dir.mkdir()
    runner = CliRunner()
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
        mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file"),
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir)],
            env=_DANDI_ENV,
        )
    assert result.exit_code == 0, result.output
    assert (queue_dir / "state.tsv").exists()


@pytest.mark.ai_generated
def test_cli_queue_refresh_required_queue_directory() -> None:
    """dandicompute queue refresh requires --queue."""
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["queue", "refresh"], env=_DANDI_ENV)
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output
