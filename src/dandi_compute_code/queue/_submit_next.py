import pathlib
import warnings

from ._read_state_entries import _read_state_entries
from ._resolve_unsubmitted_attempt_dir import _resolve_unsubmitted_attempt_dir
from ..aind_ephys_pipeline import submit_job


def _submit_next(
    *,
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    max_submissions: int = 2,
) -> bool:
    """
    Submit the next eligible pending entry from ``state.jsonl``.

    Reads ``state.jsonl`` from the queue directory. If the state file is
    absent, raises ``FileNotFoundError``. If the file exists but has no
    entries, warns and returns ``False``.

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

    pending_attempt_dirs = [
        attempt_dir
        for entry in state_entries
        if (attempt_dir := _resolve_unsubmitted_attempt_dir(base_dir=dandiset_directory, entry=entry)) is not None
    ]
    submitted_count = 0

    for attempt_dir in pending_attempt_dirs:
        script_file_path = attempt_dir / "code" / "submit.sh"
        if not script_file_path.exists():
            message = f"Submit script not found: {script_file_path}"
            raise FileNotFoundError(message)

        submit_job(script_file_path=script_file_path)
        submitted_marker = attempt_dir / "code" / ".submitted"
        submitted_marker.touch()
        submitted_count += 1
        if submitted_count >= max_submissions:
            break

    has_submissions = submitted_count > 0
    if not has_submissions:
        warnings.warn("No eligible pending entries available for submission", stacklevel=2)
    return has_submissions
