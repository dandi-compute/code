import json
import pathlib


def _read_state_entries(state_file: pathlib.Path, /) -> list[dict]:
    if not state_file.exists():
        message = f"State file not found: {state_file}"
        raise FileNotFoundError(message)

    stripped_lines = [line.strip() for line in state_file.read_text().splitlines()]
    state_entries = [json.loads(line) for line in stripped_lines if line]
    return state_entries
