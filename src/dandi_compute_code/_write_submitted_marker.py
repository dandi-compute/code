import pathlib


def write_submitted_marker(*, submit_script_path: pathlib.Path) -> pathlib.Path:
    """
    Create a ``submitted`` marker file adjacent to ``submit.sh``.

    Parameters
    ----------
    submit_script_path : pathlib.Path
        Path to the ``submit.sh`` script whose parent directory should receive
        the marker.

    Returns
    -------
    pathlib.Path
        Path to the created ``submitted`` marker file.
    """
    submitted_marker = submit_script_path.parent / "submitted"
    submitted_marker.write_bytes(b"1")
    return submitted_marker
