import pathlib


def _list_capsule_log_directories(*, dandiset_directory: pathlib.Path) -> list[pathlib.Path]:
    """Return sorted logs/ directories that belong to attempt capsules."""
    derivatives_root = dandiset_directory / "derivatives"
    if not derivatives_root.is_dir():
        return []

    return sorted(path for path in derivatives_root.rglob("logs") if path.is_dir() and "_attempt-" in path.parent.name)
