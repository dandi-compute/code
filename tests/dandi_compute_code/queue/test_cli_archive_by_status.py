"""Unit tests for the ``dandicompute archive --status`` CLI option."""

import os
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID, _JOB_CAPSULES_DANDISET_ID

_GROUP = "dandi_compute_code._cli._dandicompute_group"


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """A queue directory containing an empty state.jsonl."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("")
    return queue_dir


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_cli_archive_by_status_fails_without_api_key(status: str, tmp_path: pathlib.Path) -> None:
    """CLI errors immediately when DANDI_API_KEY is missing."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    env_without_key = {key: value for key, value in os.environ.items() if key != "DANDI_API_KEY"}

    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "--status", status, "--queue", str(queue_dir)],
        )

    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_archive_by_status_rejects_unknown_status(tmp_path: pathlib.Path) -> None:
    """CLI rejects a --status value other than 'failed'/'pending' before touching the queue."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)

    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "--status", "successful", "--queue", str(queue_dir)],
        )

    assert result.exit_code != 0
    assert "Invalid value for '--status'" in result.output


@pytest.mark.ai_generated
def test_cli_archive_by_status_fails_without_dandi_devel(tmp_path: pathlib.Path) -> None:
    """CLI errors when DANDI_DEVEL is missing, since archive_by_status relies on `dandi upload`'s devel-only flags."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)

    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}, clear=True):
        result = runner.invoke(_dandicompute_group, ["archive", "--status", "failed", "--queue", str(queue_dir)])

    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_cli_archive_by_status_requires_queue(status: str, tmp_path: pathlib.Path) -> None:
    """dandicompute archive --status without --queue is rejected with a clear error."""
    runner = CliRunner()

    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}):
        result = runner.invoke(_dandicompute_group, ["archive", "--status", status])

    assert result.exit_code != 0
    assert "--queue is required" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_cli_archive_by_status_invokes_archive_by_status_with_defaults(status: str, tmp_path: pathlib.Path) -> None:
    """dandicompute archive --status calls QueueState.archive_by_status with the default Dandiset IDs."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch(
            f"{_GROUP}.QueueState.archive_by_status", return_value=["derivatives/example-attempt"]
        ) as mock_archive,
    ):
        result = runner.invoke(_dandicompute_group, ["archive", "--status", status, "--queue", str(queue_dir)])

    assert result.exit_code == 0, result.output
    assert f"Archived 1 {status} job capsule(s)" in result.output
    assert "derivatives/example-attempt" in result.output
    mock_archive.assert_called_once_with(
        status=status,
        dandiset_id=_JOB_CAPSULES_DANDISET_ID,
        archive_dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        processing_directory=None,
        test=False,
    )


@pytest.mark.ai_generated
def test_cli_archive_by_status_forwards_custom_dandiset_ids(tmp_path: pathlib.Path) -> None:
    """dandicompute archive --status forwards --dandiset-id/--archive-dandiset-id and other options."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch(
            f"{_GROUP}.QueueState.archive_by_status", return_value=["derivatives/example-attempt"]
        ) as mock_archive,
    ):
        result = runner.invoke(
            _dandicompute_group,
            [
                "archive",
                "--status",
                "failed",
                "--queue",
                str(queue_dir),
                "--dandiset-id",
                "000123",
                "--archive-dandiset-id",
                "000456",
                "--processing",
                str(processing_dir),
                "--test",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_archive.assert_called_once_with(
        status="failed",
        dandiset_id="000123",
        archive_dandiset_id="000456",
        processing_directory=processing_dir,
        test=True,
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_cli_archive_by_status_reports_nothing_to_archive(status: str, tmp_path: pathlib.Path) -> None:
    """dandicompute archive --status reports when there are no matching capsules to archive."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"}),
        mock.patch(f"{_GROUP}.QueueState.archive_by_status", return_value=[]) as mock_archive,
    ):
        result = runner.invoke(_dandicompute_group, ["archive", "--status", status, "--queue", str(queue_dir)])

    assert result.exit_code == 0, result.output
    assert f"No {status} job capsules to archive" in result.output
    mock_archive.assert_called_once_with(
        status=status,
        dandiset_id=_JOB_CAPSULES_DANDISET_ID,
        archive_dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
        processing_directory=None,
        test=False,
    )
