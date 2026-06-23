import logging
import pathlib
from collections.abc import Callable
from unittest import mock

import pytest

from dandi_compute_code.queue._process_queue import process_queue


@pytest.fixture
def processing_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    directory = tmp_path / "processing"
    directory.mkdir()
    return directory


@pytest.mark.ai_generated
def test_process_queue_handles_empty_scan_when_waiting_file_missing(
    queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue raises when state.jsonl is absent."""
    with pytest.raises(FileNotFoundError, match="State file not found"):
        process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_refreshes_state_when_empty(
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """process_queue logs and returns when state.jsonl is empty."""
    install_state_file(queue_directory=queue_directory, name="empty.jsonl")

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._process_queue"),
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=0,
        )

    mock_submit.assert_not_called()
    assert any("No entries in" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_process_queue_rejects_non_positive_max_concurrent_jobs(
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
) -> None:
    """process_queue raises when max_concurrent_aind_jobs is less than one."""
    install_state_file(queue_directory=queue_directory, name="single_pending.jsonl")

    with pytest.raises(ValueError, match="max_concurrent_aind_jobs must be at least 1"):
        process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            max_concurrent_aind_jobs=0,
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_rejects_negative_jitter_seconds(
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
) -> None:
    """process_queue raises when jitter_seconds is negative."""
    install_state_file(queue_directory=queue_directory, name="single_pending.jsonl")

    with pytest.raises(ValueError, match="jitter_seconds must be non-negative"):
        process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=-1.0,
        )


@pytest.mark.ai_generated
def test_process_queue_skips_sleep_when_jitter_is_zero(
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
) -> None:
    """process_queue does not sleep when jitter_seconds=0."""
    install_state_file(queue_directory=queue_directory, name="empty.jsonl")

    with (
        mock.patch("dandi_compute_code.queue._process_queue.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next"),
    ):
        process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=0,
        )

    mock_sleep.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_sleeps_within_jitter_range(
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
) -> None:
    """process_queue sleeps a duration in [0, jitter_seconds] when jitter_seconds > 0."""
    install_state_file(queue_directory=queue_directory, name="empty.jsonl")

    with (
        mock.patch("dandi_compute_code.queue._process_queue.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._process_queue.random.uniform", return_value=5.0) as mock_uniform,
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next"),
    ):
        process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=30.0,
        )

    mock_uniform.assert_called_once_with(0, 30.0)
    mock_sleep.assert_called_once_with(5.0)
