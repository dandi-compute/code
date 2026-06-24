"""
Tests for the soft-rollout failsafe shared by the queue CLI commands.

Each command first attempts the new ``QueueState`` OOP model and, on any failure,
records the failure to a log file and falls back to the current free-function
behavior. These tests drive that path through the public CLI surface.
"""

import json
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group

_GROUP = "dandi_compute_code._cli._dandicompute_group"


@pytest.fixture
def failsafe_log(tmp_path: pathlib.Path) -> pathlib.Path:
    """A temporary failsafe log path wired through the override environment variable."""
    return tmp_path / "oop_failsafe.jsonl"


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps({"pipelines": {"test": {"version_priority": ["v1.0"], "params_priority": ["default"]}}})
    )
    return queue_dir


@pytest.mark.ai_generated
def test_oop_failure_falls_back_and_logs(tmp_path: pathlib.Path, failsafe_log: pathlib.Path) -> None:
    """When the OOP path raises, the command logs the failure and runs the fallback."""
    queue_dir = _make_queue_dir(tmp_path)
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with (
        mock.patch(f"{_GROUP}.QueueState.process_queue", side_effect=RuntimeError("boom")) as mock_oop,
        mock.patch(f"{_GROUP}.process_queue", return_value="submitted") as mock_fallback,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--queue", str(queue_dir), "--processing", str(processing_dir)],
            env={
                "DANDI_API_KEY": "test-key",
                "DANDI_DEVEL": "1",
                "DANDICOMPUTE_OOP_FAILSAFE_LOG": str(failsafe_log),
            },
        )

    assert result.exit_code == 0, result.output
    mock_oop.assert_called_once()
    mock_fallback.assert_called_once_with(
        queue_directory=queue_dir,
        processing_directory=processing_dir,
        max_concurrent_aind_jobs=2,
        jitter_seconds=30.0,
        test=False,
    )

    assert failsafe_log.exists()
    records = [json.loads(line) for line in failsafe_log.read_text().splitlines() if line.strip()]
    assert len(records) == 1
    assert records[0]["command"] == "queue process"
    assert records[0]["error_type"] == "RuntimeError"
    assert records[0]["error_message"] == "boom"
    assert "RuntimeError: boom" in records[0]["traceback"]
    assert records[0]["logged_at"]


@pytest.mark.ai_generated
def test_oop_success_skips_fallback_and_does_not_log(tmp_path: pathlib.Path, failsafe_log: pathlib.Path) -> None:
    """When the OOP path succeeds, the fallback is not used and nothing is logged."""
    queue_dir = _make_queue_dir(tmp_path)
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with (
        mock.patch(f"{_GROUP}.QueueState.process_queue", return_value="submitted") as mock_oop,
        mock.patch(f"{_GROUP}.process_queue") as mock_fallback,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--queue", str(queue_dir), "--processing", str(processing_dir)],
            env={
                "DANDI_API_KEY": "test-key",
                "DANDI_DEVEL": "1",
                "DANDICOMPUTE_OOP_FAILSAFE_LOG": str(failsafe_log),
            },
        )

    assert result.exit_code == 0, result.output
    mock_oop.assert_called_once()
    mock_fallback.assert_not_called()
    assert not failsafe_log.exists()


@pytest.mark.ai_generated
def test_failure_log_appends_across_invocations(tmp_path: pathlib.Path, failsafe_log: pathlib.Path) -> None:
    """Repeated OOP failures append, leaving an investigable record per invocation."""
    runner = CliRunner()
    env = {"DANDICOMPUTE_OOP_FAILSAFE_LOG": str(failsafe_log)}

    for _ in range(2):
        with (
            mock.patch(f"{_GROUP}.QueueState.has_pending_jobs", side_effect=ValueError("nope")),
            mock.patch(f"{_GROUP}.has_pending_jobs", return_value=True),
        ):
            result = runner.invoke(_dandicompute_group, ["queue", "pending"], env=env)
        assert result.exit_code == 0, result.output

    records = [json.loads(line) for line in failsafe_log.read_text().splitlines() if line.strip()]
    assert len(records) == 2
    assert all(record["command"] == "queue pending" for record in records)
    assert all(record["error_type"] == "ValueError" for record in records)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("command", "oop_target", "fallback_target", "expected_command_label"),
    [
        pytest.param("queue pending", "QueueState.has_pending_jobs", "has_pending_jobs", "queue pending", id="pending"),
        pytest.param("issues dump", "QueueState.dump_issues", "dump_issues", "issues dump", id="issues-dump"),
        pytest.param(
            "issues summarize",
            "QueueState.summarize_issues",
            "summarize_issues",
            "issues summarize",
            id="issues-summarize",
        ),
    ],
)
def test_each_command_records_its_own_label(
    tmp_path: pathlib.Path,
    failsafe_log: pathlib.Path,
    command: str,
    oop_target: str,
    fallback_target: str,
    expected_command_label: str,
) -> None:
    """Every wrapped command falls back on OOP failure and logs under its own label."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    runner = CliRunner()

    args_by_command = {
        "queue pending": ["queue", "pending"],
        "issues dump": ["issues", "dump", "--directory", str(dandiset_dir), "--queue", str(queue_dir)],
        "issues summarize": ["issues", "summarize", "--directory", str(dandiset_dir), "--queue", str(queue_dir)],
    }
    fallback_returns = {"queue pending": True, "issues dump": [], "issues summarize": {}}

    with (
        mock.patch(f"{_GROUP}.{oop_target}", side_effect=RuntimeError("boom")),
        mock.patch(f"{_GROUP}.{fallback_target}", return_value=fallback_returns[command]) as mock_fallback,
    ):
        result = runner.invoke(
            _dandicompute_group,
            args_by_command[command],
            env={"DANDICOMPUTE_OOP_FAILSAFE_LOG": str(failsafe_log)},
        )

    assert result.exit_code in (0, 1), result.output
    mock_fallback.assert_called_once()
    records = [json.loads(line) for line in failsafe_log.read_text().splitlines() if line.strip()]
    assert len(records) == 1
    assert records[0]["command"] == expected_command_label
