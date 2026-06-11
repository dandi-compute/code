import logging
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue._process_queue import process_queue


@pytest.mark.ai_generated
def test_process_queue_handles_empty_scan_when_waiting_file_missing(tmp_path: pathlib.Path) -> None:
    """process_queue raises when state.jsonl is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="State file not found"):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_refreshes_state_when_empty(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    """process_queue logs and returns when state.jsonl is empty."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("")
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._process_queue"),
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )

    mock_submit.assert_not_called()
    assert any("No entries in" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_process_queue_rejects_non_positive_max_concurrent_jobs(tmp_path: pathlib.Path) -> None:
    """process_queue raises when max_concurrent_aind_jobs is less than one."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("{}\n")
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with pytest.raises(ValueError, match="max_concurrent_aind_jobs must be at least 1"):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            max_concurrent_aind_jobs=0,
            jitter_seconds=0,
        )


@pytest.mark.ai_generated
def test_process_queue_rejects_negative_jitter_seconds(tmp_path: pathlib.Path) -> None:
    """process_queue raises when jitter_seconds is negative."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("{}\n")
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with pytest.raises(ValueError, match="jitter_seconds must be non-negative"):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=-1.0,
        )


@pytest.mark.ai_generated
def test_process_queue_skips_sleep_when_jitter_is_zero(tmp_path: pathlib.Path) -> None:
    """process_queue does not sleep when jitter_seconds=0."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("")
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next"),
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )

    mock_sleep.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_sleeps_within_jitter_range(tmp_path: pathlib.Path) -> None:
    """process_queue sleeps a duration in [0, jitter_seconds] when jitter_seconds > 0."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "state.jsonl").write_text("")
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue.time.sleep") as mock_sleep,
        mock.patch("dandi_compute_code.queue._process_queue.random.uniform", return_value=5.0) as mock_uniform,
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next"),
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=30.0,
        )

    mock_uniform.assert_called_once_with(0, 30.0)
    mock_sleep.assert_called_once_with(5.0)
