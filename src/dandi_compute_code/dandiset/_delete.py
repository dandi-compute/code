import pathlib
import shutil
import subprocess


def scan_version_directories(dandiset_directory: pathlib.Path, version: str) -> list[pathlib.Path]:
    """
    Find all ``version-{version}`` directories under *dandiset_directory*.

    Scans ``{dandiset_directory}/derivatives/dandiset-*/`` and returns every
    directory named ``version-{version}`` found at any depth, in sorted order.
    Directories that are not inside a ``dandiset-*`` subtree are ignored.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository.
    version : str
        The version string to search for (e.g. ``"v1.0.0"`` or
        ``"v1.0.0+fixes+20abeb6"``).

    Returns
    -------
    list[pathlib.Path]
        Sorted list of matching version directory paths.
    """
    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return []

    version_dirname = f"version-{version}"
    version_dirs: list[pathlib.Path] = []

    for dandiset_path in sorted(derivatives.iterdir()):
        if not dandiset_path.is_dir() or not dandiset_path.name.startswith("dandiset-"):
            continue
        for candidate in sorted(dandiset_path.rglob(version_dirname)):
            if candidate.is_dir() and candidate.name == version_dirname:
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
        The version string to delete (e.g. ``"v1.0.0"`` or
        ``"v1.0.0+fixes+20abeb6"``).  The function looks for directories named
        ``version-{version}``.

    Returns
    -------
    list[pathlib.Path]
        A list of version directories that were deleted, in sorted order.
    """
    version_dirs = scan_version_directories(dandiset_directory=dandiset_directory, version=version)

    for version_dir in version_dirs:
        subprocess.run(
            ["dandi", "delete", str(version_dir)],
            input=b"y\n",
            check=True,
        )
        shutil.rmtree(version_dir)

    return version_dirs
