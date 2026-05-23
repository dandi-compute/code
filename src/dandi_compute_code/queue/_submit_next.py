import json
import pathlib

from ._load_queue_config import _load_queue_config
from ._order_queue import order_queue
from ._refresh_queue import refresh_queue
from ._resolve_attempt_dir import _resolve_attempt_dir
from ..aind_ephys_pipeline import submit_job


def _read_state_entries(state_file: pathlib.Path, /) -> list[dict]:
    if not state_file.exists():
        return []

    stripped_lines = [line.strip() for line in state_file.read_text().splitlines()]
    state_entries = [json.loads(line) for line in stripped_lines if line]
    return state_entries


def _submit_next(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> bool:
    """
    Submit the next eligible pending entry from ``state.jsonl``.

    Reads ``state.jsonl`` from the queue directory. If ``state.jsonl`` is
    absent or empty, :func:`refresh_queue` is called once to repopulate it.
    If the state file is still empty after refresh, returns ``False``.

    Entries are ordered via :func:`order_queue`. The first ordered entry that
    does not already have a ``code/.submitted`` marker is submitted, and that
    marker is created immediately after submission succeeds.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to
        locate prepared submission scripts.

    Returns
    -------
    bool
        True if a job was submitted, False if there are no eligible entries.
    """
    state_file = queue_directory / "state.jsonl"
    state_entries = _read_state_entries(state_file)

    if not state_entries:
        refresh_queue(queue_directory=queue_directory, dandiset_directory=dandiset_directory)
        state_entries = _read_state_entries(state_file)

    if not state_entries:
        print(f"No pending entries in `{state_file}`")
        return False

    queue_config = _load_queue_config(queue_directory=queue_directory)
    ordered_entries = order_queue(state_entries=state_entries, queue_config=queue_config)

    for entry in ordered_entries:
        attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)
        submitted_marker = attempt_dir / "code" / ".submitted"
        if submitted_marker.exists():
            continue

        script_file_path = attempt_dir / "code" / "submit.sh"
        if not script_file_path.exists():
            message = f"Submit script not found: {script_file_path}"
            raise FileNotFoundError(message)

        print(f"Submitting run capsule directory: {attempt_dir}")
        submit_job(script_file_path=script_file_path)
        submitted_marker.touch()
        return True

    print("No eligible pending entries available for submission")
    return False
