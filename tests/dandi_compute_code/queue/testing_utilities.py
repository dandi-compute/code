"""
Plain helpers for working with the example queue (``example_state_files/state.jsonl``).

These are ordinary functions, imported and called directly by the queue tests.
They need nothing from pytest, so they are deliberately not fixtures. The pytest
fixtures (temporary directories, environment and network setup) live in
``conftest.py``.
"""

import pathlib

from dandi_compute_code.queue import JobEntry, QueueState

EXAMPLE_STATE_FILE = pathlib.Path(__file__).parent / "example_state_files" / "state.jsonl"

_EXAMPLE_QUEUE = QueueState.from_jsonl(EXAMPLE_STATE_FILE)


def copy_state_file(queue_directory: pathlib.Path, /) -> pathlib.Path:
    """
    Copy the example queue into *queue_directory* and return the written path.

    The queue functions locate state by ``queue_directory/state.jsonl`` and never
    mutate it, so this simply copies the literal example contents into a temporary
    queue directory (keeping any sibling outputs out of the committed example).
    """
    destination = queue_directory / "state.jsonl"
    destination.write_text(EXAMPLE_STATE_FILE.read_text())
    return destination


def entry_for(dandi_path: str, *, attempt: int = 1) -> JobEntry:
    """
    Select an example entry by its scenario-naming ``dandi_path``.

    ``attempt`` disambiguates scenarios that use more than one attempt of the same
    asset.
    """
    for entry in _EXAMPLE_QUEUE.entries:
        if entry.job.dandi_path == dandi_path and entry.job.attempt == attempt:
            return entry
    message = f"No example entry with dandi_path={dandi_path!r} and attempt={attempt}"
    raise KeyError(message)


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
