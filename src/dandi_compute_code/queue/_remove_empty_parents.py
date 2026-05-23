import pathlib


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
