"""
Private helpers for the :mod:`._queue_state` OOP model.

This companion module holds the lower-level utilities the ``QueueState`` /
``JobEntry`` model depends on (assets-path parsing, attempt-record construction,
upstream-metadata lookup, queue-config validation, content-id ordering, and log
parsing). It deliberately reimplements the behavior of the sibling procedural
queue helpers so the model does not call them. The duplication is temporary,
kept until those procedural functions are removed in favor of the model.
"""

from __future__ import annotations

import collections
import json
import logging
import pathlib
import random
import re
import urllib.error
import urllib.request
from dataclasses import dataclass

import linkml_runtime.processing.referencevalidator
import linkml_runtime.utils.schemaview

from ._globals import _DURATION_PART_RE, _QUEUE_CONFIG_SCHEMA_PATH
from ._job_info import JobInfo
from ..dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    _build_asset_metadata,
)
from ..dandiset._load_content_id_to_usage_dandiset_path import _load_content_id_to_usage_dandiset_path

_log = logging.getLogger(__name__)

_UPSTREAM_JSONLD_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/dandisets/{dandiset_id}/draft/assets.jsonld"

_FLAT_ATTEMPT_RE = re.compile(
    r"^version-(?P<version>.+?)"
    r"_codebase-(?P<codebase>[^_]+)"
    r"_params-(?P<params>[^_]+)"
    r"_config-(?P<config>[^_]+)"
    r"_attempt-(?P<attempt>\d+)$"
)
_NESTED_ATTEMPT_RE = re.compile(r"^params-(?P<params>[^_]+)_config-(?P<config>[^_]+)_attempt-(?P<attempt>\d+)$")


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
    ``derivatives/dandiset-XXX/.../pipeline-NAME/<attempt-dir>/<subpath>`` into a
    :class:`JobInfo` and the subpath beneath the attempt directory. Returns
    ``None`` if the path does not match either supported layout.
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


def _load_upstream_assets_jsonld_metadata(dandiset_id: str) -> AssetsJsonldMetadata:
    """Fetch and index ``assets.jsonld`` for another dandiset by id."""
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


def _new_attempt_record(job: JobInfo) -> dict[str, object]:
    return {
        **job.to_dict(),
        "has_code": False,
        "has_been_submitted": False,
        "has_output": False,
        "has_logs": False,
        "dataset_description_path": {},
        "output_paths": {},
        "log_paths": {},
    }


@dataclass
class _AttemptCollection:
    """Bookkeeping accumulated while walking ``assets.jsonld`` once."""

    records_by_attempt: dict[JobInfo, dict[str, object]]
    log_timestamps_by_attempt: dict[JobInfo, list[str]]
    submit_sh_timestamps_by_attempt: dict[JobInfo, str]


def _collect_attempts(local_metadata: AssetsJsonldMetadata) -> _AttemptCollection:
    """
    Walk every asset path, group by attempt identity, record presence flags, and
    capture the ``code/submit.sh`` timestamp per attempt (used for ``created_at``).
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


def _validate_queue_config(*, queue_config: dict) -> None:
    """Validate the queue config (top-level ``pipelines`` mapping) against the LinkML schema."""
    validator = linkml_runtime.processing.referencevalidator.ReferenceValidator(
        linkml_runtime.utils.schemaview.SchemaView(str(_QUEUE_CONFIG_SCHEMA_PATH))
    )
    report = validator.validate(queue_config, target="PipelinesConfig")
    errors = [result for result in report.results if not (result.normalized or result.repaired)]
    if errors:
        message = (
            f"Invalid queue configuration: LinkML validation failed with {len(errors)} error(s). "
            f"First error: {errors[0]!r}"
        )
        raise ValueError(message)


def _load_queue_config(*, queue_directory: pathlib.Path) -> dict:
    """
    Read ``queue_config.json`` and validate it against the LinkML schema.

    :raises FileNotFoundError: If ``queue_config.json`` is not found in *queue_directory*.
    :raises ValueError: If the queue configuration fails LinkML validation.
    """
    queue_config_file = queue_directory / "queue_config.json"
    if not queue_config_file.exists():
        message = f"'queue_config.json' not found in '{queue_directory}'."
        raise FileNotFoundError(message)

    queue_config = json.loads(queue_config_file.read_text())
    _validate_queue_config(queue_config=queue_config)
    return queue_config


def _order_content_ids_for_uniform_dandiset_sampling(*, content_ids: list[str]) -> list[str]:
    """Return content IDs ordered by randomized round-robin across source Dandisets."""
    content_id_to_dandiset_path = _load_content_id_to_usage_dandiset_path()
    content_ids_by_dandiset: dict[str | tuple[str, str], list[str]] = {}
    for content_id in content_ids:
        content_id_mapping = content_id_to_dandiset_path.get(content_id, {})
        if len(content_id_mapping) == 1:
            dandiset_key: str | tuple[str, str] = next(iter(content_id_mapping))
        else:
            dandiset_key = ("content-id", content_id)
        content_ids_by_dandiset.setdefault(dandiset_key, []).append(content_id)

    grouped_content_ids = list(content_ids_by_dandiset.values())
    random.shuffle(grouped_content_ids)
    grouped_content_id_queues: list[collections.deque[str]] = []
    for content_ids_for_dandiset in grouped_content_ids:
        random.shuffle(content_ids_for_dandiset)
        grouped_content_id_queues.append(collections.deque(content_ids_for_dandiset))

    ordered_content_ids: list[str] = []
    while grouped_content_id_queues:
        next_grouped_content_id_queues: list[collections.deque[str]] = []
        for content_ids_for_dandiset in grouped_content_id_queues:
            ordered_content_ids.append(content_ids_for_dandiset.popleft())
            if content_ids_for_dandiset:
                next_grouped_content_id_queues.append(content_ids_for_dandiset)
        grouped_content_id_queues = next_grouped_content_id_queues
    return ordered_content_ids


def _duration_string_to_seconds(duration_string: str) -> float:
    """Parse a Nextflow duration string (for example ``1m 5s``) into seconds."""
    total_seconds = 0.0
    for match in _DURATION_PART_RE.finditer(duration_string):
        value = float(match.group("value"))
        unit = match.group("unit")
        if unit == "ms":
            total_seconds += value / 1000.0
        elif unit == "s":
            total_seconds += value
        elif unit == "m":
            total_seconds += value * 60.0
        elif unit == "h":
            total_seconds += value * 3600.0
        elif unit == "d":
            total_seconds += value * 86400.0
    return total_seconds


def _extract_nextflow_timeline_data(*, timeline_html: str) -> dict | None:
    """Extract the ``window.data`` JSON payload from a Nextflow timeline HTML report."""
    marker = "window.data ="
    marker_index = timeline_html.find(marker)
    if marker_index == -1:
        return None

    object_start = timeline_html.find("{", marker_index)
    if object_start == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    object_end = -1
    for index in range(object_start, len(timeline_html)):
        character = timeline_html[index]
        if in_string:
            if escape:
                escape = False
            elif character == "\\":
                escape = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character == "{":
            depth += 1
        elif character == "}":
            depth -= 1
            if depth == 0:
                object_end = index
                break

    if object_end == -1:
        return None

    try:
        payload = json.loads(timeline_html[object_start : object_end + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_error_lines(*, log_file: pathlib.Path) -> list[str]:
    """Return non-empty log lines containing 'error' (case-insensitive)."""
    if not log_file.is_file():
        return []

    return [
        line.strip()
        for line in log_file.read_text(errors="replace").splitlines()
        if line.strip() and "error" in line.lower()
    ]


def _list_capsule_log_directories(*, dandiset_directory: pathlib.Path) -> list[pathlib.Path]:
    """Return sorted ``logs/`` directories that belong to attempt capsules."""
    derivatives_root = dandiset_directory / "derivatives"
    if not derivatives_root.is_dir():
        return []

    return sorted(path for path in derivatives_root.rglob("logs") if path.is_dir() and "_attempt-" in path.parent.name)


def _remove_empty_parents(*, start: pathlib.Path, stop: pathlib.Path) -> None:
    """
    Remove empty directories from ``start`` up to but not including ``stop``.

    If ``stop`` is not an ancestor of ``start``, this function returns without
    modifying the filesystem. Removal stops at the first non-empty directory.
    """
    if stop not in start.parents:
        return

    current = start
    while current != stop:
        if not current.exists() or not current.is_dir():
            break
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent
