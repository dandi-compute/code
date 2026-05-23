import os
import pathlib
import shutil
import subprocess

from ._scan_version_directories import scan_version_directories


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
