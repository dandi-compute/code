import pathlib


def _extract_error_lines(*, log_file: pathlib.Path) -> list[str]:
    """Return non-empty log lines containing 'error' (case-insensitive)."""
    if not log_file.is_file():
        return []

    return [
        line.strip()
        for line in log_file.read_text(errors="replace").splitlines()
        if line.strip() and "error" in line.lower()
    ]
