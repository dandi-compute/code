import os
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import QueueState
from model.testing_utilities import create_attempt_directory


@pytest.mark.ai_generated
def test_archive_unsubmitted_raises_without_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """archive_unsubmitted raises RuntimeError when DANDI_API_KEY is not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            QueueState(entries=[]).archive_unsubmitted(dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_archive_unsubmitted_returns_empty_list_when_nothing_pending(
    tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_unsubmitted returns an empty list and moves nothing when there are no pending entries."""
    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        archived = QueueState(entries=[]).archive_unsubmitted(dandiset_directory=tmp_path)

    assert archived == []
    mock_move.assert_not_called()


@pytest.mark.ai_generated
def test_archive_unsubmitted_moves_every_unsubmitted_entry(
    example_queue_state: QueueState, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_unsubmitted calls move_job_capsule once per pending entry, with its resolved capsule path."""
    dandiset_dir = tmp_path / "dandiset"
    pending_entry_1 = example_queue_state.entry_for(dandi_path="sub-pending")
    pending_entry_2 = example_queue_state.entry_for(dandi_path="sub-fresh")
    two_pending_state = QueueState(entries=[pending_entry_1, pending_entry_2])

    pending_attempt_1 = create_attempt_directory(base_dir=dandiset_dir, entry=pending_entry_1)
    pending_attempt_2 = create_attempt_directory(base_dir=dandiset_dir, entry=pending_entry_2)

    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        archived = two_pending_state.archive_unsubmitted(dandiset_directory=dandiset_dir)

    expected_paths = [
        pending_attempt_1.relative_to(dandiset_dir).as_posix(),
        pending_attempt_2.relative_to(dandiset_dir).as_posix(),
    ]
    assert archived == expected_paths
    assert mock_move.call_count == 2
    mock_move.assert_any_call(capsule_path=expected_paths[0], processing_directory=None, test=False)
    mock_move.assert_any_call(capsule_path=expected_paths[1], processing_directory=None, test=False)


@pytest.mark.ai_generated
def test_archive_unsubmitted_ignores_non_pending_entries(
    example_queue_state: QueueState, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_unsubmitted does not move successful or failed capsules."""
    dandiset_dir = tmp_path / "dandiset"
    create_attempt_directory(
        base_dir=dandiset_dir,
        entry=example_queue_state.entry_for(dandi_path="sub-successful"),
        with_logs=True,
        with_output=True,
    )
    create_attempt_directory(
        base_dir=dandiset_dir,
        entry=example_queue_state.entry_for(dandi_path="sub-failed/ses-repeated", attempt=1),
        with_logs=True,
    )

    non_pending_state = QueueState(entries=[entry for entry in example_queue_state if not entry.is_pending])
    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        archived = non_pending_state.archive_unsubmitted(dandiset_directory=dandiset_dir)

    assert archived == []
    mock_move.assert_not_called()


@pytest.mark.ai_generated
def test_archive_unsubmitted_forwards_processing_directory_and_test_flag(
    example_queue_state: QueueState, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_unsubmitted forwards processing_directory and test through to move_job_capsule."""
    dandiset_dir = tmp_path / "dandiset"
    processing_dir = tmp_path / "processing"
    single_entry = example_queue_state.entry_for(dandi_path="sub-pending")
    single_pending_state = QueueState(entries=[single_entry])
    pending_attempt = create_attempt_directory(base_dir=dandiset_dir, entry=single_entry)

    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        single_pending_state.archive_unsubmitted(
            dandiset_directory=dandiset_dir, processing_directory=processing_dir, test=True
        )

    mock_move.assert_called_once_with(
        capsule_path=pending_attempt.relative_to(dandiset_dir).as_posix(),
        processing_directory=processing_dir,
        test=True,
    )
