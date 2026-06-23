import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import QueueState


@pytest.mark.ai_generated
def test_process_queue_handles_empty_scan_when_waiting_file_missing(
    queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue raises when state.jsonl is absent."""
    with pytest.raises(FileNotFoundError, match="State file not found"):
        QueueState.process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_returns_no_pending_when_state_empty(
    queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue returns "no-pending" when state.jsonl is empty."""
    (queue_directory / "state.jsonl").write_text("")

    result = QueueState.process_queue(
        queue_directory=queue_directory,
        processing_directory=processing_directory,
        jitter_seconds=0,
    )

    assert result == "no-pending"


@pytest.mark.ai_generated
def test_process_queue_rejects_non_positive_max_concurrent_jobs(
    queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue raises when max_concurrent_aind_jobs is less than one."""
    (queue_directory / "state.jsonl").write_text("")

    with pytest.raises(ValueError, match="max_concurrent_aind_jobs must be at least 1"):
        QueueState.process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            max_concurrent_aind_jobs=0,
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_rejects_negative_jitter_seconds(
    queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue raises when jitter_seconds is negative."""
    (queue_directory / "state.jsonl").write_text("")

    with pytest.raises(ValueError, match="jitter_seconds must be non-negative"):
        QueueState.process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=-1.0,
        )


@pytest.mark.ai_generated
def test_process_queue_skips_sleep_when_jitter_is_zero(
    queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue does not sleep when jitter_seconds=0."""
    (queue_directory / "state.jsonl").write_text("")

    with mock.patch("dandi_compute_code.queue._queue_state.time.sleep") as mock_sleep:
        QueueState.process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=0,
        )

    mock_sleep.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_sleeps_within_jitter_range(
    queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue sleeps a duration in [0, jitter_seconds] when jitter_seconds > 0."""
    (queue_directory / "state.jsonl").write_text("")

    with (
        mock.patch("dandi_compute_code.queue._queue_state.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._queue_state.random.uniform", return_value=5.0) as mock_uniform,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            jitter_seconds=30.0,
        )

    mock_uniform.assert_called_once_with(0, 30.0)
    mock_sleep.assert_called_once_with(5.0)
