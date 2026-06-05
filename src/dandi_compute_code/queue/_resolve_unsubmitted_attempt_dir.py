import pathlib

from ._resolve_attempt_dir import _resolve_attempt_dir


def _resolve_unsubmitted_attempt_dir(*, base_dir: pathlib.Path, entry: dict) -> pathlib.Path | None:
    has_pending_state = entry.get("has_code") and not entry.get("has_output") and not entry.get("has_logs")
    if not has_pending_state:
        return None

    attempt_dir = _resolve_attempt_dir(base_dir=base_dir, entry=entry)
    code_dir = attempt_dir / "code"
    if (code_dir / "submitted").exists() or any(code_dir.glob("submitted_date-*")):
        return None
    return attempt_dir
