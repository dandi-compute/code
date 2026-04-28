import os
import pathlib
import shutil
import subprocess


def scan_version_directories(dandiset_directory: pathlib.Path, version: str) -> list[pathlib.Path]:
    """
    Find all ``version-{version}*`` directories under *dandiset_directory*.

    Scans ``{dandiset_directory}/derivatives/dandiset-*/`` and returns every
    directory whose name equals ``version-{version}`` or starts with
    ``version-{version}+`` (to capture hash-suffixed variants such as
    ``version-v1.0.0+fixes+20abeb6``).  Directories not inside a
    ``dandiset-*`` subtree are ignored.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository.
    version : str
        The base version string to search for (e.g. ``"v1.0.0"``).
        Matches the exact directory ``version-v1.0.0`` as well as any
        hash-suffixed variant such as ``version-v1.0.0+fixes+20abeb6``.

    Returns
    -------
    list[pathlib.Path]
        Sorted list of matching version directory paths.
    """
    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return []

    version_prefix = f"version-{version}"
    version_dirs: list[pathlib.Path] = []

    for dandiset_path in sorted(derivatives.iterdir()):
        if not dandiset_path.is_dir() or not dandiset_path.name.startswith("dandiset-"):
            continue
        for candidate in sorted(dandiset_path.rglob(f"{version_prefix}*")):
            name = candidate.name
            if candidate.is_dir() and (name == version_prefix or name.startswith(version_prefix + "+")):
                version_dirs.append(candidate)

    return version_dirs


def delete_dandiset_version(dandiset_directory: pathlib.Path, version: str) -> list[pathlib.Path]:
    """
    Delete all ``version-{version}`` directories from the DANDI archive and the local filesystem.

    Scans ``{dandiset_directory}/derivatives/dandiset-*/`` for directories named
    ``version-{version}`` at any depth, runs ``dandi delete`` on each one (answering
    the interactive confirmation prompt automatically), and then removes the local
    directory tree.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository.
    version : str
        The base version string to delete (e.g. ``"v1.0.0"``).  Matches the
        exact directory ``version-v1.0.0`` as well as any hash-suffixed variant
        such as ``version-v1.0.0+fixes+20abeb6``.

    Returns
    -------
    list[pathlib.Path]
        A list of version directories that were deleted, in sorted order.
    """
    if not os.environ.get("DANDI_API_KEY", "").strip():
        message = "`DANDI_API_KEY` environment variable is not set or is blank."
        raise RuntimeError(message)

    version_dirs = scan_version_directories(dandiset_directory=dandiset_directory, version=version)

    for version_dir in version_dirs:
        subprocess.run(
            ["dandi", "delete", str(version_dir)],
            input=b"y\n",
            check=True,
        )
        shutil.rmtree(version_dir)

    return version_dirs
