import json
import logging
import pathlib
import re
from dataclasses import dataclass

from ._load_queue_config import _load_queue_config
from ..dandiset._globals import _ASSETS_JSONLD_URL
from ..dandiset._load_assets_jsonld_metadata import AssetMetadata, AssetsJsonldMetadata, load_assets_jsonld_metadata

_FLAT_ATTEMPT_RE = re.compile(
    r"^version-(?P<version>.+?)_params-(?P<params>[^_]+)_config-(?P<config>[^_]+)(?:_.+?)?_attempt-(?P<attempt>\d+)$"
)
_NESTED_ATTEMPT_RE = re.compile(r"^params-(?P<params>[^_]+)_config-(?P<config>[^_]+)_attempt-(?P<attempt>\d+)$")
_DANDISET_ID_RE = re.compile(r"/dandisets/(?P<dandiset_id>\d+)/")
_log = logging.getLogger(__name__)
_load_assets_jsonld_metadata = load_assets_jsonld_metadata


@dataclass(frozen=True)
class JobInfo:
    dandiset_id: str
    dandi_path: str
    pipeline: str
    version: str
    params: str
    config: str
    attempt: int


def _coerce_assets_jsonld_metadata(
    assets_jsonld_metadata: AssetsJsonldMetadata | tuple[dict[str, dict[str, object]], dict[str, str]],
) -> AssetsJsonldMetadata:
    if isinstance(assets_jsonld_metadata, AssetsJsonldMetadata):
        return assets_jsonld_metadata

    content_id_to_asset, path_to_date_modified = assets_jsonld_metadata
    path_to_asset_metadata: dict[str, AssetMetadata] = {
        path: AssetMetadata(path=path, date_modified=date_modified, content_id=None)
        for path, date_modified in path_to_date_modified.items()
    }
    for content_id, asset in content_id_to_asset.items():
        path = asset.get("path")
        if isinstance(path, str):
            date_modified = path_to_date_modified.get(path)
            path_to_asset_metadata[path] = AssetMetadata(path=path, date_modified=date_modified, content_id=content_id)

    return AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset,
        path_to_asset_metadata=path_to_asset_metadata,
    )


def _parse_attempt_identity(asset_path: str) -> tuple[JobInfo, str] | None:
    asset_path_parts = pathlib.PurePosixPath(asset_path).parts
    dandiset_index = next(
        (index for index, part in enumerate(asset_path_parts) if part.startswith("dandiset-")),
        None,
    )
    if dandiset_index is None:
        return None
    pipeline_index = next(
        (
            index
            for index in range(dandiset_index + 1, len(asset_path_parts))
            if asset_path_parts[index].startswith("pipeline-")
        ),
        None,
    )
    if pipeline_index is None or pipeline_index <= dandiset_index + 1:
        return None

    pipeline_part = asset_path_parts[pipeline_index]
    pipeline = pipeline_part[len("pipeline-") :]
    if not pipeline:
        return None

    dandi_path_prefix = pathlib.PurePosixPath(*asset_path_parts[dandiset_index + 1 : pipeline_index]).as_posix()
    dandi_path = f"{dandi_path_prefix}.nwb"

    first_after_pipeline_index = pipeline_index + 1
    if first_after_pipeline_index >= len(asset_path_parts):
        return None

    version: str | None = None
    params: str | None = None
    config: str | None = None
    attempt: int | None = None
    attempt_directory_index: int | None = None

    flat_attempt_match = _FLAT_ATTEMPT_RE.fullmatch(asset_path_parts[first_after_pipeline_index])
    if flat_attempt_match is not None:
        version = flat_attempt_match.group("version")
        params = flat_attempt_match.group("params")
        config = flat_attempt_match.group("config")
        attempt = int(flat_attempt_match.group("attempt"))
        attempt_directory_index = first_after_pipeline_index
    elif asset_path_parts[first_after_pipeline_index].startswith("version-") and first_after_pipeline_index + 1 < len(
        asset_path_parts
    ):
        version = asset_path_parts[first_after_pipeline_index][len("version-") :]
        nested_attempt_match = _NESTED_ATTEMPT_RE.fullmatch(asset_path_parts[first_after_pipeline_index + 1])
        if nested_attempt_match is not None:
            params = nested_attempt_match.group("params")
            config = nested_attempt_match.group("config")
            attempt = int(nested_attempt_match.group("attempt"))
            attempt_directory_index = first_after_pipeline_index + 1

    if version is None or params is None or config is None or attempt is None or attempt_directory_index is None:
        return None

    subpath = pathlib.PurePosixPath(*asset_path_parts[attempt_directory_index + 1 :]).as_posix()
    return (
        JobInfo(
            dandiset_id=asset_path_parts[dandiset_index][len("dandiset-") :],
            dandi_path=dandi_path,
            pipeline=pipeline,
            version=version,
            params=params,
            config=config,
            attempt=attempt,
        ),
        subpath,
    )


def write_queue_state(
    *,
    queue_directory: pathlib.Path,
) -> None:
    """
    Write ``state.jsonl`` from DANDI ``assets.jsonld`` metadata.

    Each state entry represents one attempt capsule inferred from the
    ``derivatives/dandiset-*/.../pipeline-*/..._attempt-*`` path structure in
    ``assets.jsonld``. ``dandi_path`` is derived from the path segment between
    ``dandiset-*`` and ``pipeline-*`` with ``.nwb`` appended, and
    ``has_code``/``has_output``/``has_logs`` are inferred by checking whether
    any assets are present in the corresponding subdirectories under each
    attempt path.

    :param queue_directory: Path to the queue root directory.
    :type queue_directory: pathlib.Path
    """
    _load_queue_config(queue_directory=queue_directory)
    state_file = queue_directory / "state.jsonl"
    assets_jsonld_metadata = _coerce_assets_jsonld_metadata(_load_assets_jsonld_metadata())

    records_by_attempt: dict[JobInfo, dict[str, object]] = {}
    log_timestamps_by_attempt: dict[JobInfo, list[str]] = {}
    for asset_path in assets_jsonld_metadata.path_to_asset_metadata:
        parsed_attempt_identity = _parse_attempt_identity(asset_path)
        if parsed_attempt_identity is None:
            continue
        attempt_identity, subpath = parsed_attempt_identity
        record = records_by_attempt.setdefault(
            attempt_identity,
            {
                "dandiset_id": attempt_identity.dandiset_id,
                "dandi_path": attempt_identity.dandi_path,
                "pipeline": attempt_identity.pipeline,
                "version": attempt_identity.version,
                "params": attempt_identity.params,
                "config": attempt_identity.config,
                "attempt": attempt_identity.attempt,
                "has_code": False,
                "has_output": False,
                "has_logs": False,
            },
        )
        if subpath == "code" or subpath.startswith("code/"):
            record["has_code"] = True
        if subpath == "derivatives" or subpath.startswith("derivatives/"):
            record["has_output"] = True
        if subpath.startswith("logs/"):
            log_relative_path = subpath.removeprefix("logs/")
            if log_relative_path and log_relative_path != "dataset_description.json":
                record["has_logs"] = True
                log_metadata = assets_jsonld_metadata.path_to_asset_metadata.get(asset_path)
                if log_metadata is not None and isinstance(log_metadata.date_modified, str):
                    log_timestamp = log_metadata.date_modified
                    log_timestamps_by_attempt.setdefault(attempt_identity, []).append(log_timestamp)

    records: list[dict[str, object]] = []
    for attempt_identity, record in records_by_attempt.items():
        source_metadata = assets_jsonld_metadata.path_to_asset_metadata.get(attempt_identity.dandi_path)
        source_content_id = source_metadata.content_id if source_metadata is not None else None
        if not isinstance(source_content_id, str):
            _log.debug(
                "Skipping attempt for dandiset_id=%s dandi_path=%s because source content_id could not be resolved",
                attempt_identity.dandiset_id,
                attempt_identity.dandi_path,
            )
            continue
        source_asset_metadata = assets_jsonld_metadata.content_id_to_asset.get(source_content_id)
        if source_asset_metadata is None:
            _log.debug("Skipping attempt for content_id=%s because source metadata is unavailable", source_content_id)
            continue

        content_size = source_asset_metadata.get("contentSize")
        if not isinstance(content_size, int):
            _log.debug(
                "Skipping attempt for content_id=%s because contentSize is not an int (value=%r)",
                source_content_id,
                content_size,
            )
            continue

        created_at = source_asset_metadata.get("blobDateModified")
        if not isinstance(created_at, str):
            created_at = source_asset_metadata.get("dateModified")
        if not isinstance(created_at, str):
            created_at = ""

        completion_times = log_timestamps_by_attempt.get(attempt_identity, [])
        record.update(
            {
                "content_id": source_content_id,
                "asset_size_bytes": content_size,
                "created_at": created_at,
                "job_completion_time": max(completion_times) if completion_times else None,
            }
        )
        records.append(record)

    dandiset_id_match = _DANDISET_ID_RE.search(_ASSETS_JSONLD_URL)
    default_dandiset_id = dandiset_id_match.group("dandiset_id") if dandiset_id_match is not None else ""
    dandi_paths_with_attempts = {attempt_identity.dandi_path for attempt_identity in records_by_attempt}
    for content_id, source_asset_metadata in assets_jsonld_metadata.content_id_to_asset.items():
        source_dandi_path = source_asset_metadata.get("path")
        if not isinstance(source_dandi_path, str):
            continue
        if source_dandi_path.startswith("derivatives/"):
            continue
        if source_dandi_path in dandi_paths_with_attempts:
            continue
        content_size = source_asset_metadata.get("contentSize")
        if not isinstance(content_size, int):
            _log.debug(
                "Skipping asset for content_id=%s because contentSize is not an int (value=%r)",
                content_id,
                content_size,
            )
            continue
        created_at = source_asset_metadata.get("blobDateModified")
        if not isinstance(created_at, str):
            created_at = source_asset_metadata.get("dateModified")
        if not isinstance(created_at, str):
            created_at = ""
        records.append(
            {
                "dandiset_id": default_dandiset_id,
                "dandi_path": source_dandi_path,
                "content_id": content_id,
                "asset_size_bytes": content_size,
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
