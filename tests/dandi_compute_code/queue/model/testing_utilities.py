"""
Plain helpers for working with the example queue (``example_state_files/state.jsonl``).

These are ordinary functions, imported and called directly by the model tests.
They need nothing from pytest, so they are deliberately not fixtures. The pytest
fixtures (temporary directories, environment and network setup) live in
``conftest.py``.
"""

import pathlib

from dandi_compute_code.queue import JobEntry


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


def write_attempt_logs(
    *,
    dandiset_directory: pathlib.Path,
    dandiset_id: str,
    subject: str,
    attempt: int,
    nextflow_lines: list[str],
    slurm_lines_by_file: dict[str, list[str]],
) -> pathlib.Path:
    """Materialize an attempt ``logs/`` directory holding a nextflow log and slurm logs."""
    logs_dir = (
        dandiset_directory
        / "derivatives"
        / f"dandiset-{dandiset_id}"
        / f"sub-{subject}"
        / "pipeline-test"
        / f"version-v1.0_params-default_config-abc123_attempt-{attempt}"
        / "logs"
    )
    logs_dir.mkdir(parents=True)
    (logs_dir / "nextflow.log").write_text("\n".join(nextflow_lines) + "\n")
    for file_name, lines in slurm_lines_by_file.items():
        (logs_dir / file_name).write_text("\n".join(lines) + "\n")
    return logs_dir
