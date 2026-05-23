import os
import pathlib
import shutil
import subprocess

from ._remove_empty_parents import _remove_empty_parents
from ._resolve_attempt_dir import _resolve_attempt_dir
from ..dandiset import scan_dandiset_directory


def clean_unsubmitted_capsules(
    *,
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
) -> list[pathlib.Path]:
    """
    Remove all queued (unsubmitted) capsule directories from the dandiset tree.

    A capsule is considered *queued* (prepared but not yet submitted) when its
    attempt directory has a ``code/`` subdirectory but neither a non-empty
    ``logs/`` subdirectory nor a ``derivatives/`` subdirectory, and the attempt
    directory does not contain a ``code/.submitted`` marker.

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
        Path to the queue root directory.

    Returns
    -------
    list[pathlib.Path]
        List of attempt directory paths that were deleted.
    """
    if not queue_directory.is_dir():
        message = f"Queue directory does not exist or is not a directory: {queue_directory}"
        raise NotADirectoryError(message)

    if not os.environ.get("DANDI_API_KEY", "").strip():
        message = "`DANDI_API_KEY` environment variable is not set or is blank."
        raise RuntimeError(message)

    state_entries = scan_dandiset_directory(dandiset_directory=dandiset_directory)

    queued_entries = [
        entry
        for entry in state_entries
        if entry.get("has_code") and not entry.get("has_output") and not entry.get("has_logs")
    ]

    removed: list[pathlib.Path] = []
    for entry in queued_entries:
        attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)
        submitted_marker = attempt_dir / "code" / ".submitted"
        if submitted_marker.exists():
            continue

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
