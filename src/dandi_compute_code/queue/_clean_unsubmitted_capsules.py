import json
import os
import pathlib
import shutil
import subprocess

from ..dandiset import scan_dandiset_directory
from ._entry_identity import _entry_identity
from ._remove_empty_parents import _remove_empty_parents
from ._resolve_attempt_dir import _resolve_attempt_dir


def clean_unsubmitted_capsules(
    *,
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
) -> list[pathlib.Path]:
    """
    Remove all queued (unsubmitted) capsule directories from the dandiset tree.

    A capsule is considered *queued* (prepared but not yet submitted) when its
    attempt directory has a ``code/`` subdirectory but neither a non-empty
    ``logs/`` subdirectory nor a ``derivatives/`` subdirectory, **and** the
    entry is not present in ``last_submitted.jsonl`` (which tracks recently
    submitted in-flight jobs).

    The function scans *dandiset_directory* for all attempt directories using
    the local filesystem as the ground truth, filters to the queued subset,
    then deletes each matching attempt directory tree from the DANDI archive
    (via ``dandi delete``) and from the local filesystem.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository.  The function scans
        ``{dandiset_directory}/derivatives/dandiset-*/`` to locate attempt
        directories.
    queue_directory : pathlib.Path
        Path to the queue root directory.  ``last_submitted.jsonl`` is read
        from here to exclude in-flight submissions from deletion.

    Returns
    -------
    list[pathlib.Path]
        List of attempt directory paths that were deleted.
    """
    if not os.environ.get("DANDI_API_KEY", "").strip():
        message = "`DANDI_API_KEY` environment variable is not set or is blank."
        raise RuntimeError(message)

    state_entries = scan_dandiset_directory(dandiset_directory=dandiset_directory)

    # Build the set of identities that have been recently submitted (in-flight).
    last_submitted_file = queue_directory / "last_submitted.jsonl"
    submitted_identities: set[tuple] = set()
    if last_submitted_file.exists():
        submitted_identities = {
            _entry_identity(json.loads(line.strip()))
            for line in last_submitted_file.read_text().splitlines()
            if line.strip()
        }

    # Filter to queued entries: has code, no logs, no output, not submitted.
    queued_entries = [
        e
        for e in state_entries
        if e.get("has_code")
        and not e.get("has_output")
        and not e.get("has_logs")
        and _entry_identity(e) not in submitted_identities
    ]

    removed: list[pathlib.Path] = []
    for entry in queued_entries:
        attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)

        if attempt_dir.is_dir():
            parent_dir = attempt_dir.parent
            subprocess.run(
                ["dandi", "delete", str(attempt_dir)],
                input=b"y\n",
                check=True,
            )
            shutil.rmtree(attempt_dir)
            _remove_empty_parents(start=parent_dir, stop=dandiset_directory / "derivatives")
            removed.append(attempt_dir)

    return removed
