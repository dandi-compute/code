import os
import pathlib
import shutil
import subprocess

from ._queue_state import QueueState


# TODO: refactor this to not use try/except pattern (shouldn't be necessary)
def _remove_empty_parents(*, start: pathlib.Path, stop: pathlib.Path) -> None:
    """
    Remove empty directories from ``start`` up to but not including ``stop``.

    If ``stop`` is not an ancestor of ``start``, this function returns without
    modifying the filesystem. Removal stops at the first non-empty directory.
    """
    if stop not in start.parents:
        return

    current = start
    while current != stop:
        if not current.exists() or not current.is_dir():
            break
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


# TODO: review if the return value here is needed at all
# TODO: make more efficient in terms of --preserve-tree
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
    directory does not contain a submitted-marker file (``code/submitted`` or
    ``code/submitted_date-*``).

    The function reads the queue state, then deletes each
    matching attempt directory tree from the DANDI archive (via ``dandi
    delete``) and from the local filesystem. This expects the local Dandiset copy to be
    up-to-date.

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

    queue_state = QueueState.from_jsonl(queue_directory / "state.jsonl")

    cleanable_attempt_dirs = [
        attempt_dir
        for entry in queue_state
        if (attempt_dir := entry.resolve_unsubmitted_attempt_dir(dandiset_directory)) is not None
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
