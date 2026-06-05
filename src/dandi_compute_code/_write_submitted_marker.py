import pathlib


def write_submitted_marker(*, submit_script_path: pathlib.Path) -> pathlib.Path:
    """Create ``submitted`` alongside ``submit.sh`` with byte content ``b"1"``."""
    submitted_marker = submit_script_path.parent / "submitted"
    submitted_marker.write_bytes(b"1")
    return submitted_marker
