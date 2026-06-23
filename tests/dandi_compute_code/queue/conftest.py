"""
Shared fixtures for the queue test suite.

The literal example ``state.jsonl`` files live in ``example_state_files/`` so the
input each test starts from is readable on its own. These fixtures only locate
and install those files; the content itself stays in the files.
"""

import json
import os
import pathlib
from collections.abc import Callable, Iterator
from unittest import mock

import pytest

from dandi_compute_code.queue import JobEntry, QueueState

#: Consolidated queue config used by tests that need a populated queue directory.
EXAMPLE_QUEUE_CONFIG = {
    "pipelines": {
        "test": {
            "version_priority": ["v1.0"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 2,
            "asset_overrides": {"asset-aaa": 1},
            "max_fail_per_dandiset": 2,
        }
    }
}

EXAMPLE_STATE_FILE = pathlib.Path(__file__).parent / "example_state_files" / "state.jsonl"


@pytest.fixture
def example_state_file() -> pathlib.Path:
    """Path to the single literal example ``state.jsonl`` queue."""
    return EXAMPLE_STATE_FILE


@pytest.fixture
def queue_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A queue directory containing the example ``queue_config.json``."""
    directory = tmp_path / "queue"
    directory.mkdir()
    (directory / "queue_config.json").write_text(json.dumps(EXAMPLE_QUEUE_CONFIG))
    return directory


@pytest.fixture
def processing_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A directory for the temporary per-job working trees used during submission."""
    directory = tmp_path / "processing"
    directory.mkdir()
    return directory


@pytest.fixture
def copy_state_file() -> Callable[..., pathlib.Path]:
    """
    Return a helper that copies the example queue into a queue directory.

    The queue functions locate state by ``queue_directory/state.jsonl`` and never
    mutate it, so the helper copies the literal contents of
    ``example_state_files/state.jsonl`` into a temporary ``<queue_directory>`` and
    returns the written path. The copy keeps any sibling outputs (such as
    ``queue_stats.json``) out of the committed example directory.
    """

    def _copy(*, queue_directory: pathlib.Path) -> pathlib.Path:
        destination = queue_directory / "state.jsonl"
        destination.write_text(EXAMPLE_STATE_FILE.read_text())
        return destination

    return _copy


@pytest.fixture
def populated_queue(
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    copy_state_file: Callable[..., pathlib.Path],
) -> tuple[pathlib.Path, pathlib.Path]:
    """A queue directory holding the example state, paired with a processing directory."""
    copy_state_file(queue_directory=queue_directory)
    return queue_directory, processing_directory


@pytest.fixture
def _dandi_api_key() -> Iterator[None]:
    """Provide a dummy DANDI_API_KEY for helpers that require it to be set."""
    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        yield


@pytest.fixture
def entry_for() -> Callable[..., JobEntry]:
    """
    Return a helper that selects an example entry by its ``dandi_path``.

    Entries are identified by the scenario-naming ``dandi_path`` (and ``attempt``
    when a scenario uses more than one attempt of the same asset).
    """
    queue_state = QueueState.from_jsonl(EXAMPLE_STATE_FILE)

    def _entry_for(dandi_path: str, *, attempt: int = 1) -> JobEntry:
        for entry in queue_state.entries:
            if entry.job.dandi_path == dandi_path and entry.job.attempt == attempt:
                return entry
        message = f"No example entry with dandi_path={dandi_path!r} and attempt={attempt}"
        raise KeyError(message)

    return _entry_for


@pytest.fixture
def create_attempt_directory() -> Callable[..., pathlib.Path]:
    """
    Return a helper that materializes an attempt directory for a state entry.

    The on-disk layout is derived from the entry itself (via the public
    ``JobEntry.attempt_dir_candidates``) so the directory tree always matches the
    ground-truth example state file rather than a separately specified set of
    coordinates.
    """

    def _create(
        *,
        base_dir: pathlib.Path,
        entry: JobEntry,
        with_code: bool = True,
        with_output: bool = False,
        with_logs: bool = False,
        submitted: bool = False,
        legacy_nested: bool = False,
    ) -> pathlib.Path:
        flat_attempt_dir, nested_attempt_dir = entry.attempt_dir_candidates(base_dir)
        attempt_dir = nested_attempt_dir if legacy_nested else flat_attempt_dir
        attempt_dir.mkdir(parents=True)
        if with_code:
            code_dir = attempt_dir / "code"
            code_dir.mkdir()
            (code_dir / "submit.sh").write_text("#!/bin/bash\necho hello\n")
            if submitted:
                (code_dir / "submitted_date-date-2025+01+01_time-00+00+00").touch()
        if with_output:
            (attempt_dir / "derivatives").mkdir()
        if with_logs:
            logs_dir = attempt_dir / "logs"
            logs_dir.mkdir()
            (logs_dir / "run.log").write_text("job output\n")
        return attempt_dir

    return _create
