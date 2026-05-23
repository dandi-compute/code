import pathlib
import re

from ._globals import _FLAT_ATTEMPT_DIR_RE


# TODO: determine where this is used
def _count_dandiset_failures(
    *,
    dandiset_directory: pathlib.Path,
    version: str,
) -> int:
    """
    Count failure attempt directories across all source dandisets for a given version.

    Scans every ``derivatives/dandiset-*`` sub-directory inside *dandiset_directory*
    (i.e. the local clone of the 001697 dandiset repository) and counts attempt
    directories that contain a ``code/`` subdirectory, a non-empty ``logs/``
    subdirectory, but **no** ``derivatives/`` subdirectory — the signature of a job that
    ran but did not produce output.  Pending entries (code present but logs empty or
    absent) are not counted.

    A directory is considered an attempt if its name ends with ``_attempt-<number>``
    and either:

    * its immediate parent is named ``version-{version}`` (legacy layout), or
    * its immediate parent is ``pipeline-*`` and the attempt directory name starts
      with ``version-{version}_`` (current flat layout).

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to the local clone of the 001697 dandiset repository.
    version : str
        The BIDS-encoded pipeline version string as stored in the directory name
        (e.g., ``'v1.0.0+fixes+47bd492'``).

    Returns
    -------
    int
        Total number of failed attempt directories across all source dandisets for
        the given *version*.
    """
    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return 0

    failure_count = 0
    attempt_re = re.compile(r"_attempt-\d+$")
    version_dir_name = f"version-{version}"

    for dandiset_path in derivatives.iterdir():
        if not dandiset_path.is_dir() or not dandiset_path.name.startswith("dandiset-"):
            continue
        for attempt_dir in dandiset_path.rglob("*_attempt-*"):
            if not attempt_dir.is_dir():
                continue
            if not attempt_re.search(attempt_dir.name):
                continue
            parent_name = attempt_dir.parent.name
            if parent_name == version_dir_name:
                is_matching_version = True
            elif parent_name.startswith("pipeline-"):
                flat_match = _FLAT_ATTEMPT_DIR_RE.fullmatch(attempt_dir.name)
                is_matching_version = bool(flat_match and flat_match.group("version") == version)
            else:
                is_matching_version = False
            if not is_matching_version:
                continue
            logs_dir = attempt_dir / "logs"
            has_logs = logs_dir.is_dir() and any(logs_dir.iterdir())
            if (attempt_dir / "code").is_dir() and has_logs and not (attempt_dir / "derivatives").is_dir():
                failure_count += 1

    return failure_count
