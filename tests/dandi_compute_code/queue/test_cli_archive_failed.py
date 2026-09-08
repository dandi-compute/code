"""Unit tests for the ``dandicompute archive failed`` CLI command."""

import os
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group

_GROUP = "dandi_compute_code._cli._dandicompute_group"


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """A queue directory containing an empty state.jsonl."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("")
    return queue_dir


@pytest.mark.ai_generated
def test_cli_archive_failed_fails_without_api_key(tmp_path: pathlib.Path) -> None:
    """CLI errors immediately when DANDI_API_KEY is missing."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    env_without_key = {key: value for key, value in os.environ.items() if key != "DANDI_API_KEY"}

    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "failed", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        )

    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_archive_failed_invokes_archive_failed_with_options(tmp_path: pathlib.Path) -> None:
    """dandicompute archive failed calls QueueState.archive_failed with the provided options."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.QueueState.archive_failed", return_value=["derivatives/example-attempt"]) as mock_archive,
    ):
        result = runner.invoke(
            _dandicompute_group,
            [
                "archive",
                "failed",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
                "--processing",
                str(processing_dir),
                "--test",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "Archived 1 failed job capsule(s)" in result.output
    assert "derivatives/example-attempt" in result.output
    mock_archive.assert_called_once_with(
        dandiset_directory=dandiset_dir, processing_directory=processing_dir, test=True
    )


@pytest.mark.ai_generated
def test_cli_archive_failed_reports_nothing_to_archive(tmp_path: pathlib.Path) -> None:
    """dandicompute archive failed reports when there are no failed capsules to archive."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.QueueState.archive_failed", return_value=[]) as mock_archive,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", "failed", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        )

    assert result.exit_code == 0, result.output
    assert "No failed job capsules to archive" in result.output
    mock_archive.assert_called_once_with(dandiset_directory=dandiset_dir, processing_directory=None, test=False)
