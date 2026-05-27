import pathlib

from ._load_queue_config import _load_queue_config
from ._write_queue_state import write_queue_state


def refresh_queue_state(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
    """
    Regenerate ``state.jsonl`` from DANDI ``assets.jsonld`` metadata.

    :param queue_directory: Path to the queue root directory.
    :type queue_directory: pathlib.Path
    :param dandiset_directory: Unused legacy argument kept for API compatibility with existing
        CLI wiring and call sites.
    :type dandiset_directory: pathlib.Path
    :raises FileNotFoundError: If ``queue_config.json`` is not found in *queue_directory*.
    """
    _load_queue_config(queue_directory=queue_directory)
    write_queue_state(queue_directory=queue_directory)
