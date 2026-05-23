import json
import os
import pathlib

from ._load_queue_config import _load_queue_config
from ._order_queue import order_queue
from ._prune_last_submitted import _prune_last_submitted
from ..dandiset import scan_dandiset_directory


def refresh_queue(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
    """
    Scan *dandiset_directory*, regenerate ``state.jsonl``, and write ``waiting.jsonl``.

    Entries that are prepared (``has_code=True``) but not yet run
    (``has_logs=False``, ``has_output=False``) are ordered via
    :func:`order_queue` and written to ``waiting.jsonl`` so that subsequent
    calls to :func:`process_queue` can read them directly.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local dandiset clone used to rewrite ``state.jsonl``
        before ``waiting.jsonl`` is regenerated.

    Raises
    ------
    FileNotFoundError
        If ``queue_config.json`` is not found in *queue_directory*.
    """
    assert (
        "DANDI_API_KEY" in os.environ and os.environ["DANDI_API_KEY"]
    ), "`DANDI_API_KEY` environment variable must be set before refreshing queue state."

    state_file = queue_directory / "state.jsonl"
    records = scan_dandiset_directory(dandiset_directory=dandiset_directory)
    with state_file.open(mode="w") as file_stream:
        for record in records:
            file_stream.write(json.dumps(record) + "\n")

    state_entries = [json.loads(line.strip()) for line in state_file.read_text().splitlines() if line.strip()]
    queue_config = _load_queue_config(queue_directory=queue_directory)
    ordered = order_queue(state_entries=state_entries, queue_config=queue_config)
    waiting_file = queue_directory / "waiting.jsonl"
    waiting_file.write_text("".join(json.dumps(e) + "\n" for e in ordered))
    _prune_last_submitted(queue_directory=queue_directory, state_entries=state_entries)
