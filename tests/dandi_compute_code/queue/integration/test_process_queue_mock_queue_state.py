import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import QueueState


@pytest.mark.ai_generated
def test_process_queue_skips_refresh_when_state_non_empty(
    example_queue_state: QueueState, queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue runs without warning when state.jsonl already has entries."""
    example_queue_state.to_file(queue_directory / "state.jsonl")

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.count_running_aind_ephys_pipeline_jobs", return_value=2
        ),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.submit_next") as mock_submit,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory, processing_directory=processing_directory, jitter_seconds=0
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(
    example_queue_state: QueueState, queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue requests two submissions when no AIND jobs are running."""
    example_queue_state.to_file(queue_directory / "state.jsonl")

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.count_running_aind_ephys_pipeline_jobs", return_value=0
        ),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.submit_next", return_value=True) as mock_submit,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory, processing_directory=processing_directory, jitter_seconds=0
        )

    mock_submit.assert_called_once_with(processing_directory=processing_directory, max_submissions=2, test=False)


@pytest.mark.ai_generated
def test_process_queue_respects_explicit_max_concurrent_jobs(
    example_queue_state: QueueState, queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue uses the explicit AIND concurrency limit to compute submissions."""
    example_queue_state.to_file(queue_directory / "state.jsonl")

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.count_running_aind_ephys_pipeline_jobs", return_value=1
        ),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.submit_next", return_value=True) as mock_submit,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            max_concurrent_aind_jobs=3,
            jitter_seconds=0,
        )

    mock_submit.assert_called_once_with(processing_directory=processing_directory, max_submissions=2, test=False)


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(
    example_queue_state: QueueState, queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue does not submit when two AIND-Ephys-Pipeline jobs already run."""
    example_queue_state.to_file(queue_directory / "state.jsonl")

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.count_running_aind_ephys_pipeline_jobs", return_value=2
        ),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.submit_next") as mock_submit,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory, processing_directory=processing_directory, jitter_seconds=0
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_one_when_one_job_running(
    example_queue_state: QueueState, queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue requests one submission when exactly one AIND-Ephys-Pipeline job is running."""
    example_queue_state.to_file(queue_directory / "state.jsonl")

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.count_running_aind_ephys_pipeline_jobs", return_value=1
        ),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.submit_next", return_value=True) as mock_submit,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory, processing_directory=processing_directory, jitter_seconds=0
        )

    mock_submit.assert_called_once_with(processing_directory=processing_directory, max_submissions=1, test=False)


@pytest.mark.ai_generated
def test_process_queue_passes_processing_directory_to_submit_next(
    example_queue_state: QueueState, queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue forwards processing_directory to _submit_next when idle."""
    example_queue_state.to_file(queue_directory / "state.jsonl")

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.count_running_aind_ephys_pipeline_jobs", return_value=0
        ),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.submit_next", return_value=True) as mock_submit,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory, processing_directory=processing_directory, jitter_seconds=0
        )

    mock_submit.assert_called_once_with(processing_directory=processing_directory, max_submissions=2, test=False)


@pytest.mark.ai_generated
def test_process_queue_forwards_test_flag(
    example_queue_state: QueueState, queue_directory: pathlib.Path, processing_directory: pathlib.Path
) -> None:
    """process_queue forwards test=True to _submit_next."""
    example_queue_state.to_file(queue_directory / "state.jsonl")

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.QueueState.count_running_aind_ephys_pipeline_jobs", return_value=0
        ),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.submit_next", return_value=True) as mock_submit,
    ):
        QueueState.process_queue(
            queue_directory=queue_directory,
            processing_directory=processing_directory,
            test=True,
            jitter_seconds=0,
        )

    mock_submit.assert_called_once_with(processing_directory=processing_directory, max_submissions=2, test=True)
