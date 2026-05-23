import json
import pathlib
import warnings

from ._resolve_attempt_dir import _resolve_attempt_dir
from ..aind_ephys_pipeline import submit_job


def _read_state_entries(state_file: pathlib.Path, /) -> list[dict]:
    if not state_file.exists():
        return []

    stripped_lines = [line.strip() for line in state_file.read_text().splitlines()]
    state_entries = [json.loads(line) for line in stripped_lines if line]
    return state_entries


def _submit_next(
    *,
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    max_submissions: int = 2,
) -> bool:
    """
    Submit the next eligible pending entry from ``state.jsonl``.

    Reads ``state.jsonl`` from the queue directory. If the state file is
    absent or empty, returns ``False``.

    Entries are filtered to those with no output and no logs. The first up to
    ``max_submissions`` eligible entries that do not already have a
    ``code/.submitted`` marker are submitted, and each marker is created
    immediately after submission succeeds.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to
        locate prepared submission scripts.
    max_submissions : int, optional
        Maximum number of pending jobs to submit from the ordered queue.

    Returns
    -------
    bool
        True if at least one job was submitted, False otherwise.
    """
    state_file = queue_directory / "state.jsonl"
    state_entries = _read_state_entries(state_file)

    if not state_entries:
        warnings.warn(f"No pending entries in `{state_file}`", stacklevel=2)
        return False

    if max_submissions < 1:
        return False

    pending_entries = [entry for entry in state_entries if not entry.get("has_output") and not entry.get("has_logs")]
    submitted_count = 0

    for entry in pending_entries:
        attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)
        submitted_marker = attempt_dir / "code" / ".submitted"
        if submitted_marker.exists():
            continue

        script_file_path = attempt_dir / "code" / "submit.sh"
        if not script_file_path.exists():
            message = f"Submit script not found: {script_file_path}"
            raise FileNotFoundError(message)

        submit_job(script_file_path=script_file_path)
        submitted_marker.touch()
        submitted_count += 1
        if submitted_count >= max_submissions:
            break

    has_submissions = submitted_count > 0
    if not has_submissions:
        warnings.warn("No eligible pending entries available for submission", stacklevel=2)
    return has_submissions
