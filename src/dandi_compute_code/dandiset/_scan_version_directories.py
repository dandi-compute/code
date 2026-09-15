import pathlib


def scan_version_directories(dandiset_directory: pathlib.Path, version: str) -> list[pathlib.Path]:
    """
    Find all ``version-{version}*`` directories under *dandiset_directory*.

    Scans ``{dandiset_directory}/derivatives/dandisets-{first 3 digits}/dandiset-*/``
    (i.e. any ``dandiset-*`` directory found anywhere under ``derivatives/``, at any
    nesting depth -- tolerating both the current ``dandisets-XYZ/dandiset-*`` layout
    and a legacy flat ``dandiset-*`` layout) and returns every directory whose name
    equals ``version-{version}`` or starts with ``version-{version}+`` (to capture
    hash-suffixed variants such as ``version-v1.0.0+fixes+20abeb6``).  Directories not
    inside a ``dandiset-*`` subtree are ignored.

    :param dandiset_directory: Path to a local clone of the dandiset repository.
    :type dandiset_directory: pathlib.Path
    :param version: The base version string to search for (e.g. ``"v1.0.0"``).
        Matches the exact directory ``version-v1.0.0`` as well as any
        hash-suffixed variant such as ``version-v1.0.0+fixes+20abeb6``.
    :type version: str
    :returns: Sorted list of matching version directory paths.
    :rtype: list[pathlib.Path]
    """
    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return []

    version_prefix = f"version-{version}"
    version_dirs: list[pathlib.Path] = []

    dandiset_paths = sorted(
        path for path in derivatives.rglob("dandiset-*") if path.is_dir() and path.name.startswith("dandiset-")
    )
    for dandiset_path in dandiset_paths:
        for candidate in sorted(dandiset_path.rglob(f"{version_prefix}*")):
            name = candidate.name
            if candidate.is_dir() and (name == version_prefix or name.startswith(version_prefix + "+")):
                version_dirs.append(candidate)

    return version_dirs
