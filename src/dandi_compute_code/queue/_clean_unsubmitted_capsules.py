import os
import pathlib
import shutil
import subprocess

from ._read_state_entries import _read_state_entries
from ._remove_empty_parents import _remove_empty_parents
from ._resolve_unsubmitted_attempt_dir import _resolve_unsubmitted_attempt_dir
from ._write_queue_state import write_queue_state


# TODO: review if the return value here is needed at all
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
    directory does not contain a ``code/submitted`` marker.

    The function refreshes ``state.jsonl`` in *queue_directory* from DANDI
    ``assets.jsonld`` metadata, filters to the queued subset, then deletes each
    matching attempt directory tree from the DANDI archive (via ``dandi
    delete``) and from the local filesystem.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository used to resolve and
        delete matching attempt directories.
    queue_directory : pathlib.Path
        Path to the queue root directory.

    Returns
    -------
    list[pathlib.Path]
        List of attempt directory paths that were deleted.

    Raises
    ------
    NotADirectoryError
        If *queue_directory* does not exist or is not a directory.
    RuntimeError
        If the ``DANDI_API_KEY`` environment variable is not set or is blank.
    """
    if not queue_directory.is_dir():
        message = f"Queue directory does not exist or is not a directory: {queue_directory}"
        raise NotADirectoryError(message)

    if not os.environ.get("DANDI_API_KEY", "").strip():
        message = "`DANDI_API_KEY` environment variable is not set or is blank."
        raise RuntimeError(message)

    write_queue_state(queue_directory=queue_directory)
    state_entries = _read_state_entries(queue_directory / "state.jsonl")

    cleanable_attempt_dirs = [
        attempt_dir
        for entry in state_entries
        if (attempt_dir := _resolve_unsubmitted_attempt_dir(base_dir=dandiset_directory, entry=entry)) is not None
    ]

    removed: list[pathlib.Path] = []
    for attempt_dir in cleanable_attempt_dirs:
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
