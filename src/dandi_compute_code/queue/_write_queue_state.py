import json
import pathlib
import re

from ._load_queue_config import _load_queue_config
from ..dandiset._globals import _ASSETS_JSONLD_URL
from ..dandiset._load_assets_jsonld_metadata import _load_assets_jsonld_metadata

_DANDISET_ID_RE = re.compile(r"/dandisets/(?P<dandiset_id>\d+)/")


def write_queue_state(
    *,
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path | None = None,
) -> None:
    """
    Write ``state.jsonl`` from DANDI ``assets.jsonld`` metadata.

    Each state entry represents a source asset and starts in an unprepared state
    (``has_code=False``, ``has_output=False``, ``has_logs=False``). ``created_at``
    prefers ``blobDateModified`` and falls back to ``dateModified`` when blob
    metadata is unavailable.

    ``dandiset_directory`` is accepted for backward compatibility and ignored.
    """
    _load_queue_config(queue_directory=queue_directory)
    state_file = queue_directory / "state.jsonl"
    content_id_to_asset, _path_to_date_modified = _load_assets_jsonld_metadata()
    dandiset_id_match = _DANDISET_ID_RE.search(_ASSETS_JSONLD_URL)
    dandiset_id = dandiset_id_match.group("dandiset_id") if dandiset_id_match is not None else ""

    records = []
    for content_id, source_asset_metadata in content_id_to_asset.items():
        dandi_path = source_asset_metadata.get("path")
        if not isinstance(dandi_path, str):
            continue
        content_size = source_asset_metadata.get("contentSize")
        if isinstance(content_size, int):
            asset_size_bytes = content_size
        elif isinstance(content_size, str) and content_size.isdigit():
            asset_size_bytes = int(content_size)
        else:
            asset_size_bytes = None
        created_at = source_asset_metadata.get("blobDateModified")
        if not isinstance(created_at, str):
            created_at = source_asset_metadata.get("dateModified")
        if not isinstance(created_at, str):
            created_at = ""
        records.append(
            {
                "dandiset_id": dandiset_id,
                "content_id": content_id,
                "dandi_path": dandi_path,
                "asset_size_bytes": asset_size_bytes,
                "pipeline": "",
                "version": "",
                "params": "",
                "config": "",
                "attempt": 0,
                "has_code": False,
                "has_output": False,
                "has_logs": False,
                "created_at": created_at,
                "job_completion_time": None,
            }
        )

    records.sort(
        key=lambda record: (
            record["dandiset_id"],
            record["dandi_path"],
            record["content_id"],
        )
    )

    with state_file.open(mode="w") as file_stream:
        for record in records:
            file_stream.write(json.dumps(record) + "\n")
