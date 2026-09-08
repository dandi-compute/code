import os
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import QueueState
from model.testing_utilities import create_attempt_directory

#: For each archivable status, two distinct example entries (by dandi_path/attempt)
#: that qualify, plus the create_attempt_directory kwargs needed to materialize them
#: with that status.
_STATUS_EXAMPLE_ENTRIES = {
    "failed": {
        "entry_kwargs": {"with_logs": True},
        "matching": [
            {"dandi_path": "sub-failed/ses-repeated", "attempt": 1},
            {"dandi_path": "sub-failed/ses-repeated", "attempt": 2},
        ],
    },
    "pending": {
        "entry_kwargs": {},
        "matching": [
            {"dandi_path": "sub-pending"},
            {"dandi_path": "sub-fresh"},
        ],
    },
}


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_archive_by_status_raises_without_dandi_api_key(status: str, tmp_path: pathlib.Path) -> None:
    """archive_by_status raises RuntimeError when DANDI_API_KEY is not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            QueueState(entries=[]).archive_by_status(status=status, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_archive_by_status_raises_on_unknown_status(tmp_path: pathlib.Path, dandi_api_key: None) -> None:
    """archive_by_status raises ValueError for a status other than 'failed'/'pending'."""
    with pytest.raises(ValueError, match="Unknown status"):
        QueueState(entries=[]).archive_by_status(status="successful", dandiset_directory=tmp_path)


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_archive_by_status_returns_empty_list_when_nothing_matches(
    status: str, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_by_status returns an empty list and moves nothing when no entries match."""
    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        archived = QueueState(entries=[]).archive_by_status(status=status, dandiset_directory=tmp_path)

    assert archived == []
    mock_move.assert_not_called()


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_archive_by_status_moves_every_matching_entry(
    status: str, example_queue_state: QueueState, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_by_status calls move_job_capsule once per matching entry, with its resolved capsule path."""
    example = _STATUS_EXAMPLE_ENTRIES[status]
    dandiset_dir = tmp_path / "dandiset"
    entries = [example_queue_state.entry_for(**selector) for selector in example["matching"]]
    matching_state = QueueState(entries=entries)
    attempt_dirs = [
        create_attempt_directory(base_dir=dandiset_dir, entry=entry, **example["entry_kwargs"]) for entry in entries
    ]

    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        archived = matching_state.archive_by_status(status=status, dandiset_directory=dandiset_dir)

    expected_paths = [attempt_dir.relative_to(dandiset_dir).as_posix() for attempt_dir in attempt_dirs]
    assert archived == expected_paths
    assert mock_move.call_count == len(expected_paths)
    for expected_path in expected_paths:
        mock_move.assert_any_call(capsule_path=expected_path, processing_directory=None, test=False)


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_archive_by_status_ignores_non_matching_entries(
    status: str, example_queue_state: QueueState, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_by_status does not move capsules that don't have the requested status."""
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

    non_matching_state = QueueState(
        entries=[entry for entry in example_queue_state if entry not in getattr(example_queue_state, status)]
    )
    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        archived = non_matching_state.archive_by_status(status=status, dandiset_directory=dandiset_dir)

    assert archived == []
    mock_move.assert_not_called()


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["failed", "pending"])
def test_archive_by_status_forwards_processing_directory_and_test_flag(
    status: str, example_queue_state: QueueState, tmp_path: pathlib.Path, dandi_api_key: None
) -> None:
    """archive_by_status forwards processing_directory and test through to move_job_capsule."""
    example = _STATUS_EXAMPLE_ENTRIES[status]
    dandiset_dir = tmp_path / "dandiset"
    processing_dir = tmp_path / "processing"
    single_entry = example_queue_state.entry_for(**example["matching"][0])
    single_matching_state = QueueState(entries=[single_entry])
    attempt_dir = create_attempt_directory(base_dir=dandiset_dir, entry=single_entry, **example["entry_kwargs"])

    with mock.patch("dandi_compute_code.queue._queue_state.move_job_capsule") as mock_move:
        single_matching_state.archive_by_status(
            status=status, dandiset_directory=dandiset_dir, processing_directory=processing_dir, test=True
        )

    mock_move.assert_called_once_with(
        capsule_path=attempt_dir.relative_to(dandiset_dir).as_posix(),
        processing_directory=processing_dir,
        test=True,
    )
