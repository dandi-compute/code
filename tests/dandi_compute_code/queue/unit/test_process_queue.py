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
        )

    mock_submit.assert_not_called()
    assert any("No entries in" in record.message for record in caplog.records)
