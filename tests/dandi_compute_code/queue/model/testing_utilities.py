"""
Plain helpers for working with the example queue (``example_state_files/state.jsonl``).

These are ordinary functions, imported and called directly by the model tests.
They need nothing from pytest, so they are deliberately not fixtures. The pytest
fixtures (temporary directories, environment and network setup) live in
``conftest.py``.
"""

import pathlib

from dandi_compute_code.queue import JobEntry, QueueState

EXAMPLE_STATE_FILE = pathlib.Path(__file__).parent / "example_state_files" / "state.jsonl"


def example_queue_state() -> QueueState:
    """Load the committed example queue (``example_state_files/state.jsonl``) into a fresh model."""
    return QueueState.from_jsonl(EXAMPLE_STATE_FILE)


def create_attempt_directory(
    *,
    base_dir: pathlib.Path,
    entry: JobEntry,
    with_code: bool = True,
    with_output: bool = False,
    with_logs: bool = False,
    submitted: bool = False,
    legacy_nested: bool = False,
) -> pathlib.Path:
    """
    Materialize the on-disk attempt directory for *entry* under *base_dir*.

    The layout is derived from the entry itself (via the public
    ``JobEntry.attempt_dir_candidates``) so the directory tree always matches the
    ground-truth example state rather than a separately specified set of coordinates.
    """
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
