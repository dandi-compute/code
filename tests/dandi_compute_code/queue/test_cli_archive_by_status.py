"""Unit tests for the ``dandicompute archive failed``/``archive unsubmitted`` CLI commands."""

import os
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group

_GROUP = "dandi_compute_code._cli._dandicompute_group"

#: (CLI subcommand name, QueueState.archive_by_status status, CLI display label).
_COMMAND_CASES = [
    ("failed", "failed", "failed"),
    ("unsubmitted", "pending", "unsubmitted"),
]


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """A queue directory containing an empty state.jsonl."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("")
    return queue_dir


@pytest.mark.ai_generated
@pytest.mark.parametrize(("command_name", "status", "label"), _COMMAND_CASES)
def test_cli_archive_by_status_fails_without_api_key(
    command_name: str, status: str, label: str, tmp_path: pathlib.Path
) -> None:
    """CLI errors immediately when DANDI_API_KEY is missing."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    env_without_key = {key: value for key, value in os.environ.items() if key != "DANDI_API_KEY"}

    with mock.patch.dict(os.environ, env_without_key, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", command_name, "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        )

    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(("command_name", "status", "label"), _COMMAND_CASES)
def test_cli_archive_by_status_invokes_archive_by_status_with_options(
    command_name: str, status: str, label: str, tmp_path: pathlib.Path
) -> None:
    """dandicompute archive <command_name> calls QueueState.archive_by_status with the provided options."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            f"{_GROUP}.QueueState.archive_by_status", return_value=["derivatives/example-attempt"]
        ) as mock_archive,
    ):
        result = runner.invoke(
            _dandicompute_group,
            [
                "archive",
                command_name,
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
    assert f"Archived 1 {label} job capsule(s)" in result.output
    assert "derivatives/example-attempt" in result.output
    mock_archive.assert_called_once_with(
        status=status, dandiset_directory=dandiset_dir, processing_directory=processing_dir, test=True
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize(("command_name", "status", "label"), _COMMAND_CASES)
def test_cli_archive_by_status_reports_nothing_to_archive(
    command_name: str, status: str, label: str, tmp_path: pathlib.Path
) -> None:
    """dandicompute archive <command_name> reports when there are no matching capsules to archive."""
    runner = CliRunner()
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    with (
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
        mock.patch(f"{_GROUP}.QueueState.archive_by_status", return_value=[]) as mock_archive,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["archive", command_name, "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        )

    assert result.exit_code == 0, result.output
    assert f"No {label} job capsules to archive" in result.output
    mock_archive.assert_called_once_with(
        status=status, dandiset_directory=dandiset_dir, processing_directory=None, test=False
    )
