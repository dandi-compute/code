import datetime
import logging
import pathlib

from ._read_state_entries import _read_state_entries
from ._resolve_attempt_dir import _resolve_attempt_dir
from ._resolve_unsubmitted_attempt_dir import _resolve_unsubmitted_attempt_dir
from ..aind_ephys_pipeline import submit_job

_log = logging.getLogger(__name__)


# TODO: make logic even cleaner and remove return
def _submit_next(
    *,
    queue_directory: pathlib.Path,
    datalad_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    max_submissions: int = 2,
) -> bool:
    """
    Submit the next eligible pending entry from ``state.jsonl``.

    Reads ``state.jsonl`` from the queue directory. If the file exists but
    has no entries, warns and returns ``False``.

    Entries are filtered to those with no output and no logs. The first up to
    ``max_submissions`` eligible entries that do not already have a
    ``code/submitted`` marker are submitted, and each marker is created
    immediately after submission succeeds.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    datalad_directory : pathlib.Path
        Path to the DataLad-backed work tree used to resolve unsubmitted
        attempt directories.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to
        write submission marker files after backend submission.
    max_submissions : int, optional
        Maximum number of pending jobs to submit from the ordered queue.

    Returns
    -------
    bool
        True if at least one job was submitted, False otherwise.

    Raises
    ------
    FileNotFoundError
        If ``state.jsonl`` is not found in *queue_directory*, or if a resolved
        attempt directory does not contain the expected ``code/submit.sh``
        submission script.
    """
    if max_submissions < 1:
        return False

    state_file = queue_directory / "state.jsonl"
    state_entries = _read_state_entries(state_file)

    if not state_entries:
        _log.info(f"No pending entries in `{state_file}`")
        return False

    pending_submissions: list[tuple[dict, pathlib.Path]] = []
    seen_script_file_paths: set[pathlib.Path] = set()
    for entry in state_entries:
        attempt_dir = _resolve_unsubmitted_attempt_dir(base_dir=datalad_directory, entry=entry)
        if attempt_dir is None:
            continue
        script_file_path = attempt_dir / "code" / "submit.sh"
        if script_file_path in seen_script_file_paths:
            continue
        seen_script_file_paths.add(script_file_path)
        pending_submissions.append((entry, script_file_path))

    if not pending_submissions:
        _log.info("No eligible pending entries available for submission")
        return False

    for entry, script_file_path in pending_submissions[:max_submissions]:
        attempt_dir_in_dandiset = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)
        script_file_path_in_dandiset = attempt_dir_in_dandiset / "code" / "submit.sh"

        script_file_to_submit = script_file_path
        if not script_file_to_submit.exists():
            script_file_to_submit = script_file_path_in_dandiset
        if not script_file_to_submit.exists():
            message = (
                "Submit script not found in either location: " f"{script_file_path} or {script_file_path_in_dandiset}"
            )
            raise FileNotFoundError(message)

        submit_job(script_file_path=script_file_to_submit)

        submitted_marker = script_file_to_submit.parent / "submitted"
        if not submitted_marker.parent.exists():
            message = f"Creating '{submitted_marker.parent.absolute()}'"
            _log.info(message)
            submitted_marker.parent.mkdir(parents=True, exist_ok=True)
        submitted_marker.write_text(datetime.datetime.now().isoformat())
    return True
