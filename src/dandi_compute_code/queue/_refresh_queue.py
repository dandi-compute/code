import json
import os
import pathlib

from ._load_queue_config import _load_queue_config
from ..dandiset import scan_dandiset_directory


# TODO: check this is removed after simplification
def refresh_queue_state(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
    """
    Scan *dandiset_directory* and regenerate ``state.jsonl``.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local dandiset clone used to rewrite ``state.jsonl``.

    Raises
    ------
    FileNotFoundError
        If ``queue_config.json`` is not found in *queue_directory*.
    """
    assert (
        "DANDI_API_KEY" in os.environ and os.environ["DANDI_API_KEY"]
    ), "`DANDI_API_KEY` environment variable must be set before refreshing queue state."

    _load_queue_config(queue_directory=queue_directory)
    state_file = queue_directory / "state.jsonl"
    records = scan_dandiset_directory(dandiset_directory=dandiset_directory)
    with state_file.open(mode="w") as file_stream:
        for record in records:
            file_stream.write(json.dumps(record) + "\n")


refresh_queue = refresh_queue_state
