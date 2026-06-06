import json
import logging
import pathlib
import re
import urllib.error
import urllib.request
from dataclasses import dataclass

from ._load_queue_config import _load_queue_config
from ..dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    _build_asset_metadata,
    load_assets_jsonld_metadata,
)

_FLAT_ATTEMPT_RE = re.compile(
    r"^version-(?P<version>.+?)"
    r"_codebase-(?P<codebase>[^_]+)"
    r"_params-(?P<params>[^_]+)"
    r"_config-(?P<config>[^_]+)"
    r"_attempt-(?P<attempt>\d+)$"
)
_NESTED_ATTEMPT_RE = re.compile(r"^params-(?P<params>[^_]+)_config-(?P<config>[^_]+)_attempt-(?P<attempt>\d+)$")
_UPSTREAM_JSONLD_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/dandisets/{dandiset_id}/draft/assets.jsonld"

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobInfo:
    dandiset_id: str
    dandi_path: str
    pipeline: str
    version: str
    params: str
    config: str
    attempt: int
    codebase: str


# --- path parsing -----------------------------------------------------------


def _find_segment_index(parts: tuple[str, ...], prefix: str, start: int = 0) -> int | None:
    """Return the index of the first part starting with ``prefix`` at or after ``start``."""
    for index in range(start, len(parts)):
        if parts[index].startswith(prefix):
            return index
    return None


def _parse_flat_attempt(parts: tuple[str, ...], index: int) -> tuple[dict[str, str], int] | None:
    """Parse a single-segment ``version-..._params-..._config-..._attempt-N`` directory."""
    match = _FLAT_ATTEMPT_RE.fullmatch(parts[index])
    if match is None:
        return None
    return match.groupdict(), index


def _parse_nested_attempt(parts: tuple[str, ...], index: int) -> tuple[dict[str, str], int] | None:
    """Parse a ``version-X / params-..._config-..._attempt-N`` directory pair."""
    if not parts[index].startswith("version-") or index + 1 >= len(parts):
        return None
    match = _NESTED_ATTEMPT_RE.fullmatch(parts[index + 1])
    if match is None:
        return None
    fields = match.groupdict()
    fields["version"] = parts[index][len("version-") :]
    return fields, index + 1


def _parse_attempt_identity(asset_path: str) -> tuple[JobInfo, str] | None:
    """
    Parse an asset path of the form
    ``derivatives/dandiset-XXX/.../pipeline-NAME/<attempt-dir>/<subpath>``
    into a :class:`JobInfo` and the subpath beneath the attempt directory.
    Returns ``None`` if the path does not match either supported layout.
    """
    parts = pathlib.PurePosixPath(asset_path).parts

    dandiset_index = _find_segment_index(parts, "dandiset-")
    if dandiset_index is None:
        return None

    pipeline_index = _find_segment_index(parts, "pipeline-", start=dandiset_index + 1)
    if pipeline_index is None or pipeline_index <= dandiset_index + 1:
        return None

    pipeline = parts[pipeline_index][len("pipeline-") :]
    if not pipeline:
        return None

    attempt_dir_index = pipeline_index + 1
    if attempt_dir_index >= len(parts):
        return None

    parsed = _parse_flat_attempt(parts, attempt_dir_index) or _parse_nested_attempt(parts, attempt_dir_index)
    if parsed is None:
        return None
    fields, attempt_directory_index = parsed

    dandi_path = "/".join(parts[dandiset_index + 1 : pipeline_index]) + ".nwb"
    subpath = "/".join(parts[attempt_directory_index + 1 :])

    job_info = JobInfo(
        dandiset_id=parts[dandiset_index][len("dandiset-") :],
        dandi_path=dandi_path,
        pipeline=pipeline,
        version=fields["version"],
        params=fields["params"],
        config=fields["config"],
        attempt=int(fields["attempt"]),
        codebase=fields["codebase"],
    )
    return job_info, subpath


def _subpath_is_under(subpath: str, directory: str) -> bool:
    """True if ``subpath`` equals ``directory`` or lives inside it."""
    return subpath == directory or subpath.startswith(f"{directory}/")


# --- upstream metadata lookup ----------------------------------------------


def _load_upstream_assets_jsonld_metadata(dandiset_id: str) -> AssetsJsonldMetadata:
    """
    Fetch and index ``assets.jsonld`` for another dandiset by id.

    Mirrors :func:`load_assets_jsonld_metadata` but parameterized by URL so
    derivatives in a meta-analysis dandiset can resolve their source assets.
    """
    url = _UPSTREAM_JSONLD_URL_TEMPLATE.format(dandiset_id=dandiset_id)
    content_id_to_asset: dict[str, dict[str, object]] = {}
    path_to_asset_metadata: dict[str, AssetMetadata] = {}

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            assets = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exception:
        _log.warning("Unable to load upstream metadata from %s: %s", url, exception)
        return AssetsJsonldMetadata(
            content_id_to_asset=content_id_to_asset,
            path_to_asset_metadata=path_to_asset_metadata,
        )

    if not isinstance(assets, list):
        _log.warning("Expected a JSON array from %s, got %s", url, type(assets).__name__)
        return AssetsJsonldMetadata(
            content_id_to_asset=content_id_to_asset,
            path_to_asset_metadata=path_to_asset_metadata,
        )

    for asset in assets:
        if not isinstance(asset, dict):
            continue
        try:
            content_id, metadata = _build_asset_metadata(asset)
        except ValueError as exception:
            _log.debug("Skipping malformed upstream asset in %s: %s", url, exception)
            continue
        content_id_to_asset[content_id] = asset
        path_to_asset_metadata[metadata.path] = metadata

    return AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset,
        path_to_asset_metadata=path_to_asset_metadata,
    )


class _UpstreamMetadataCache:
    """Per-call cache of upstream ``assets.jsonld`` lookups, keyed by dandiset id."""

    def __init__(self) -> None:
        self._cache: dict[str, AssetsJsonldMetadata] = {}

    def get(self, dandiset_id: str) -> AssetsJsonldMetadata:
        if dandiset_id not in self._cache:
            self._cache[dandiset_id] = _load_upstream_assets_jsonld_metadata(dandiset_id)
        return self._cache[dandiset_id]


# --- record construction ---------------------------------------------------


def _new_attempt_record(job: JobInfo) -> dict[str, object]:
    return {
        "dandiset_id": job.dandiset_id,
        "dandi_path": job.dandi_path,
        "pipeline": job.pipeline,
        "version": job.version,
        "params": job.params,
        "config": job.config,
        "attempt": job.attempt,
        "has_code": False,
        "has_been_submitted": False,
        "has_output": False,
        "has_logs": False,
        "dataset_description_path": {},
        "output_paths": {},
        "log_paths": {},
        "codebase": job.codebase,
    }


# --- attempt collection ----------------------------------------------------


@dataclass
class _AttemptCollection:
    """Bookkeeping accumulated while walking ``assets.jsonld`` once."""

    records_by_attempt: dict[JobInfo, dict[str, object]]
    log_timestamps_by_attempt: dict[JobInfo, list[str]]
    submit_sh_timestamps_by_attempt: dict[JobInfo, str]


def _collect_attempts(local_metadata: AssetsJsonldMetadata) -> _AttemptCollection:
    """
    Walk every asset path, group by attempt identity, record presence flags,
    and capture the ``code/submit.sh`` timestamp per attempt (used for
    ``created_at``).
    """
    records_by_attempt: dict[JobInfo, dict[str, object]] = {}
    log_timestamps_by_attempt: dict[JobInfo, list[str]] = {}
    submit_sh_timestamps_by_attempt: dict[JobInfo, str] = {}

    for asset_path, asset_metadata in local_metadata.path_to_asset_metadata.items():
        parsed = _parse_attempt_identity(asset_path)
        if parsed is None:
            continue
        job_info, subpath = parsed

        record = records_by_attempt.setdefault(job_info, _new_attempt_record(job_info))

        if _subpath_is_under(subpath, "code"):
            record["has_code"] = True
        if subpath.startswith("code/submitted"):
            record["has_been_submitted"] = True
        if subpath == "dataset_description.json":
            record["dataset_description_path"][asset_path] = asset_metadata.content_id
        elif _subpath_is_under(subpath, "derivatives"):
            record["has_output"] = True
            record["output_paths"][asset_path] = asset_metadata.content_id
        elif subpath.startswith("logs/"):
            log_relative_path = subpath.removeprefix("logs/")
            if log_relative_path and log_relative_path != "dataset_description.json":
                record["has_logs"] = True
                record["log_paths"][asset_path] = asset_metadata.content_id
                log_timestamps_by_attempt.setdefault(job_info, []).append(asset_metadata.date_modified)

        if subpath == "code/submit.sh":
            submit_sh_timestamps_by_attempt[job_info] = asset_metadata.date_modified

    return _AttemptCollection(
        records_by_attempt=records_by_attempt,
        log_timestamps_by_attempt=log_timestamps_by_attempt,
        submit_sh_timestamps_by_attempt=submit_sh_timestamps_by_attempt,
    )


def _finalize_attempt_records(
    *,
    collection: _AttemptCollection,
    upstream_cache: _UpstreamMetadataCache,
) -> list[dict[str, object]]:
    """
    Attach source-asset fields (``content_id``, ``asset_size_bytes``) from the
    upstream dandiset's ``assets.jsonld``, and ``created_at`` /
    ``job_completion_time`` from local timestamps.

    Records are emitted even if the upstream lookup fails; the source fields
    become ``None`` and a warning is logged.
    """
    finalized: list[dict[str, object]] = []
    for job_info, record in collection.records_by_attempt.items():
        upstream_metadata = upstream_cache.get(job_info.dandiset_id)
        source_metadata = upstream_metadata.path_to_asset_metadata.get(job_info.dandi_path)

        if source_metadata is None:
            _log.warning(
                "Source asset not found in upstream dandiset %s for dandi_path=%s; "
                "emitting record with null content_id/asset_size_bytes",
                job_info.dandiset_id,
                job_info.dandi_path,
            )
            content_id: str | None = None
            asset_size_bytes: int | None = None
        else:
            content_id = source_metadata.content_id
            asset_size_bytes = source_metadata.content_size

        completion_times = collection.log_timestamps_by_attempt.get(job_info, [])
        record.update(
            {
                "content_id": content_id,
                "asset_size_bytes": asset_size_bytes,
                "created_at": collection.submit_sh_timestamps_by_attempt.get(job_info),
                "job_completion_time": max(completion_times) if completion_times else None,
            }
        )
        finalized.append(record)
    return finalized


def _sort_key(record: dict[str, object]) -> tuple[str, str, str]:
    # content_id may be None for attempts whose upstream source wasn't resolvable;
    # coerce to "" so sorting stays total.
    return (
        str(record["dandiset_id"]),
        str(record["dandi_path"]),
        str(record["content_id"]) if record["content_id"] is not None else "",
    )


def write_queue_state(*, queue_directory: pathlib.Path) -> None:
    """
    Write ``state.jsonl`` from DANDI ``assets.jsonld`` metadata.

    Each state entry represents one attempt capsule inferred from the
    ``derivatives/dandiset-*/.../pipeline-*/..._attempt-*`` path structure in
    ``assets.jsonld``. ``dandi_path`` is derived from the path segment between
    ``dandiset-*`` and ``pipeline-*`` with ``.nwb`` appended.
    ``has_code``/``has_been_submitted``/``has_output``/``has_logs`` are
    inferred from the assets present under each attempt directory.
    ``dataset_description_path`` maps the root-level ``dataset_description.json``
    asset path to its blob ID when present.
    ``output_paths`` maps each output asset path to its blob ID when
    ``has_output`` is ``True``; otherwise it is an empty dict.
    ``log_paths`` maps each log asset path to its blob ID when ``has_logs`` is
    ``True``; otherwise it is an empty dict.

    For meta-analysis dandisets, the ``content_id`` and ``asset_size_bytes``
    of each attempt's source NWB are resolved by fetching the upstream
    dandiset's ``assets.jsonld``. ``created_at`` comes from the local
    ``code/submit.sh`` modification time; ``job_completion_time`` is the
    latest modification time among log files.

    :param queue_directory: Path to the queue root directory.
    :type queue_directory: pathlib.Path
    """
    _load_queue_config(queue_directory=queue_directory)
    local_metadata = load_assets_jsonld_metadata()

    collection = _collect_attempts(local_metadata)
    upstream_cache = _UpstreamMetadataCache()
    records = _finalize_attempt_records(
        collection=collection,
        upstream_cache=upstream_cache,
    )
    records.sort(key=_sort_key)

    state_file = queue_directory / "state.jsonl"
    with state_file.open(mode="w") as file_stream:
        for record in records:
            file_stream.write(json.dumps(record) + "\n")
