import json
import pathlib

from ._validate_queue_config import _validate_queue_config


def _load_queue_config(*, queue_directory: pathlib.Path) -> dict:
    """Read queue_config.json and validate it against the LinkML schema."""
    queue_config_file = queue_directory / "queue_config.json"
    if not queue_config_file.exists():
        message = f"'queue_config.json' not found in '{queue_directory}'."
        raise FileNotFoundError(message)

    queue_config = json.loads(queue_config_file.read_text())
    _validate_queue_config(queue_config=queue_config)
    return queue_config
