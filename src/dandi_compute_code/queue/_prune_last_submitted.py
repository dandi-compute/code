import json
import pathlib

from ._entry_identity import _entry_identity


# TODO: ensure this is removed after simplification
def _prune_last_submitted(*, queue_directory: pathlib.Path, state_entries: list[dict]) -> None:
    """Remove last_submitted entries that now have logs or output in state."""
    last_submitted_file = queue_directory / "last_submitted.jsonl"
    if not last_submitted_file.exists():
        return

    entries_to_prune = {_entry_identity(e) for e in state_entries if e.get("has_output") or e.get("has_logs")}

    last_submitted_entries = [
        json.loads(line.strip()) for line in last_submitted_file.read_text().splitlines() if line.strip()
    ]
    filtered = [entry for entry in last_submitted_entries if _entry_identity(entry) not in entries_to_prune]
    last_submitted_file.write_text("".join(json.dumps(entry) + "\n" for entry in filtered))
