import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue._process_queue import process_queue


def _write_jsonl(file_path: pathlib.Path, entries: list[dict]) -> None:
    file_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


def _make_state_entry(**overrides: str | int | bool) -> dict:
    entry = {
        "dandiset_id": "000001",
        "dandi_path": "sub-mouse01",
        "pipeline": "test",
        "version": "v1.0",
        "params": "default",
        "config": "abc123",
        "attempt": 1,
        "has_code": True,
        "has_output": False,
        "has_logs": False,
        "created_at": "2024-01-01T00:00:00+00:00",
    }
    entry.update(overrides)
    return entry


@pytest.fixture
def mock_queue_state(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    _write_jsonl(
        queue_dir / "state.jsonl",
        [
            _make_state_entry(),
            _make_state_entry(
                dandi_path="sub-mouse02",
                params="alternate",
                config="xyz789",
                attempt=2,
                has_logs=True,
            ),
        ],
    )

    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    return queue_dir, processing_dir


@pytest.mark.ai_generated
def test_process_queue_skips_refresh_when_state_non_empty(mock_queue_state: tuple[pathlib.Path, pathlib.Path]) -> None:
    """process_queue runs without warning when state.jsonl already has entries."""
    queue_dir, processing_dir = mock_queue_state

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(mock_queue_state: tuple[pathlib.Path, pathlib.Path]) -> None:
    """process_queue requests two submissions when no AIND jobs are running."""
    queue_dir, processing_dir = mock_queue_state

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=2,
    )


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(mock_queue_state: tuple[pathlib.Path, pathlib.Path]) -> None:
    """process_queue does not submit when two AIND-Ephys-Pipeline jobs already run."""
    queue_dir, processing_dir = mock_queue_state

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_one_when_one_job_running(mock_queue_state: tuple[pathlib.Path, pathlib.Path]) -> None:
    """process_queue requests one submission when exactly one AIND-Ephys-Pipeline job is running."""
    queue_dir, processing_dir = mock_queue_state

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=1),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=1,
    )


@pytest.mark.ai_generated
def test_process_queue_passes_processing_directory_to_submit_next(
    mock_queue_state: tuple[pathlib.Path, pathlib.Path],
) -> None:
    """process_queue forwards processing_directory to _submit_next when idle."""
    queue_dir, processing_dir = mock_queue_state

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=2,
    )
