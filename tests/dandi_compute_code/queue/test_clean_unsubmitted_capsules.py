import os
import pathlib
from collections.abc import Callable, Iterator
from unittest import mock

import pytest

from dandi_compute_code.queue import JobEntry, clean_unsubmitted_capsules


@pytest.fixture
def _dandi_api_key() -> Iterator[None]:
    """clean_unsubmitted_capsules requires DANDI_API_KEY; provide a dummy value."""
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        yield


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_raises_without_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules raises RuntimeError when DANDI_API_KEY is not set."""
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            clean_unsubmitted_capsules(dandiset_directory=tmp_path, queue_directory=tmp_path)


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_queued_directories(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules removes capsule dirs that are queued (code, no logs, no output)."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-pending"))

    with mock.patch("subprocess.run") as mock_run:
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    mock_run.assert_called_once_with(["dandi", "delete", str(queued_dir)], input=b"y\n", check=True)


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_output(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules does not remove capsules that already have output."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    completed_dir = create_attempt_directory(
        base_dir=dandiset_dir, entry=entry_for("sub-successful"), with_logs=True, with_output=True
    )

    with mock.patch("subprocess.run") as mock_run:
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == []
    assert completed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_logs(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules does not remove capsules that have logs (already run)."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    failed_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-running"), with_logs=True)

    with mock.patch("subprocess.run") as mock_run:
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == []
    assert failed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_ignores_dataset_description_in_logs(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """A logs/ directory holding only dataset_description.json does not protect a queued capsule."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-pending"))
    logs_dir = queued_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "dataset_description.json").write_text("{}\n")

    with mock.patch("subprocess.run"):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [queued_dir]
    assert not queued_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_submitted_marker(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules does not remove capsules with a submitted marker file."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-pending"), submitted=True)

    with mock.patch("subprocess.run") as mock_run:
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == []
    assert queued_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_returns_empty_list_when_nothing_queued(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules returns an empty list when nothing is materialized on disk."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)

    removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == []


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_handles_session_in_path(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules correctly handles attempt dirs with a session component."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-with-session/ses-recording"))

    with mock.patch("subprocess.run"):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [queued_dir]
    assert not queued_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_empty_parent_directories(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules removes empty pipeline/session dirs after last capsule removal."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-sole-attempt/ses-recording"))
    pipeline_dir = queued_dir.parent
    session_dir = pipeline_dir.parent

    with mock.patch("subprocess.run"):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [queued_dir]
    assert not pipeline_dir.exists()
    assert not session_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_keeps_non_empty_parent_directories(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules keeps pipeline/version dirs when a sibling attempt remains."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-two-attempts", attempt=1))
    remaining_dir = create_attempt_directory(
        base_dir=dandiset_dir, entry=entry_for("sub-two-attempts", attempt=2), with_output=True
    )
    pipeline_dir = queued_dir.parent

    with mock.patch("subprocess.run"):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [queued_dir]
    assert remaining_dir.exists()
    assert pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_legacy_nested_layout(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules removes queued capsules in the legacy nested layout."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(
        base_dir=dandiset_dir, entry=entry_for("sub-sole-attempt/ses-recording"), legacy_nested=True
    )
    version_dir = queued_dir.parent
    pipeline_dir = version_dir.parent

    with mock.patch("subprocess.run"):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert not version_dir.exists()
    assert not pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_only_queued_not_submitted(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    entry_for: Callable[..., JobEntry],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules only removes queued capsules, leaving submitted ones intact."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)
    queued_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry_for("sub-pending"))
    submitted_dir = create_attempt_directory(
        base_dir=dandiset_dir, entry=entry_for("sub-already-submitted"), submitted=True
    )

    with mock.patch("subprocess.run"):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert submitted_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removed_entry_via_fallback_attempt_resolution(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
    _dandi_api_key: None,
) -> None:
    """clean_unsubmitted_capsules removes queued entry when dandi_path differs from on-disk attempt path."""
    dandiset_dir = tmp_path / "dandiset"
    install_state_file(queue_directory=queue_directory)

    # The "sourcedata" entry's on-disk attempt lives under sub-mouse01, so it must be
    # located via fallback resolution rather than the recorded dandi_path.
    attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-mouse01"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+a66c8df_codebase-v0.3.0_params-4af6a25_config-0d4bf36_attempt-1"
    )
    (attempt_dir / "code").mkdir(parents=True)
    (attempt_dir / "code" / "submit.sh").write_text("#!/bin/bash\necho hello\n")

    with mock.patch("subprocess.run") as mock_run:
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_directory)

    assert removed == [attempt_dir]
    assert not attempt_dir.exists()
    mock_run.assert_called_once_with(["dandi", "delete", str(attempt_dir)], input=b"y\n", check=True)
