import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue._process_queue import process_queue


@pytest.mark.ai_generated
def test_process_queue_skips_refresh_when_state_non_empty(
    populated_queue: tuple[pathlib.Path, pathlib.Path],
) -> None:
    """process_queue runs without warning when state.jsonl already has entries."""
    queue_dir, processing_dir = populated_queue

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(populated_queue: tuple[pathlib.Path, pathlib.Path]) -> None:
    """process_queue requests two submissions when no AIND jobs are running."""
    queue_dir, processing_dir = populated_queue

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=2,
        test=False,
    )


@pytest.mark.ai_generated
def test_process_queue_respects_explicit_max_concurrent_jobs(
    populated_queue: tuple[pathlib.Path, pathlib.Path],
) -> None:
    """process_queue uses the explicit AIND concurrency limit to compute submissions."""
    queue_dir, processing_dir = populated_queue

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=1),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            max_concurrent_aind_jobs=3,
            jitter_seconds=0,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=2,
        test=False,
    )


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(
    populated_queue: tuple[pathlib.Path, pathlib.Path],
) -> None:
    """process_queue does not submit when two AIND-Ephys-Pipeline jobs already run."""
    queue_dir, processing_dir = populated_queue

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_one_when_one_job_running(
    populated_queue: tuple[pathlib.Path, pathlib.Path],
) -> None:
    """process_queue requests one submission when exactly one AIND-Ephys-Pipeline job is running."""
    queue_dir, processing_dir = populated_queue

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=1),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=1,
        test=False,
    )


@pytest.mark.ai_generated
def test_process_queue_passes_processing_directory_to_submit_next(
    populated_queue: tuple[pathlib.Path, pathlib.Path],
) -> None:
    """process_queue forwards processing_directory to _submit_next when idle."""
    queue_dir, processing_dir = populated_queue

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            jitter_seconds=0,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=2,
        test=False,
    )


@pytest.mark.ai_generated
def test_process_queue_forwards_test_flag(populated_queue: tuple[pathlib.Path, pathlib.Path]) -> None:
    """process_queue forwards test=True to _submit_next."""
    queue_dir, processing_dir = populated_queue

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            processing_directory=processing_dir,
            test=True,
            jitter_seconds=0,
        )

    mock_submit.assert_called_once_with(
        processing_directory=processing_dir,
        max_submissions=2,
        test=True,
    )
