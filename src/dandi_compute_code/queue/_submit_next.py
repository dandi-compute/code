import json
import pathlib

from ._refresh_queue import refresh_queue
from ._resolve_attempt_dir import _resolve_attempt_dir
from ..aind_ephys_pipeline import submit_job


def _submit_next(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> bool:
    """
    Submit the next pending entry from ``waiting.jsonl``.

    Reads ``waiting.jsonl`` from the queue directory — this file is written by
    :func:`refresh_queue` and contains the priority-ordered list of pending
    entries produced by :func:`order_queue`.  If the file is absent
    or empty, :func:`refresh_queue` is called once to attempt to repopulate it
    from ``state.jsonl``.  If still empty after that, returns ``False``.

    The first entry from ``waiting.jsonl`` is submitted and then removed from
    ``waiting.jsonl``; the entry is simultaneously appended to
    ``last_submitted.jsonl``. The waiting queue is produced upstream by
    :func:`refresh_queue`; this function applies no additional
    submission gating beyond requiring the submit script to exist.

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
        True if a job was submitted, False if there are no pending entries or
        if the submit script cannot be found.
    """
    waiting_file = queue_directory / "waiting.jsonl"

    def _read_waiting() -> list[dict]:
        if not waiting_file.exists():
            return []
        return [json.loads(line.strip()) for line in waiting_file.read_text().splitlines() if line.strip()]

    waiting_entries = _read_waiting()

    if not waiting_entries:
        # Attempt to repopulate from dandiset scan before giving up.
        refresh_queue(queue_directory=queue_directory, dandiset_directory=dandiset_directory)
        waiting_entries = _read_waiting()

    if not waiting_entries:
        print(f"No pending entries in `{waiting_file}`")
        return False

    entry = waiting_entries[0]

    attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)

    script_file_path = attempt_dir / "code" / "submit.sh"
    if not script_file_path.exists():
        message = f"Submit script not found: {script_file_path}"
        raise FileNotFoundError(message)

    print(f"Submitting run capsule directory: {attempt_dir}")
    submit_job(script_file_path=script_file_path)

    # Pop the submitted entry from waiting.jsonl.
    waiting_entries.pop(0)
    waiting_file.write_text("".join(json.dumps(e) + "\n" for e in waiting_entries))

    # Append to last_submitted.jsonl.
    last_submitted_file = queue_directory / "last_submitted.jsonl"
    with last_submitted_file.open("a") as f:
        f.write(json.dumps(entry) + "\n")

    return True
