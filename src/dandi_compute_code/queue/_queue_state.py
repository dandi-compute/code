"""
QueueState — typed container for ``state.jsonl``.

``state.jsonl`` is a newline-delimited JSON file where each line is one attempt
capsule.  Previously every consumer read it into a ``list[dict]`` and accessed
fields by string key.  This module adds a thin typed layer on top:

- :class:`JobEntry` wraps an existing :class:`JobInfo` with the status fields
  (``has_code``, ``has_output``, ``has_logs``, ``content_id``, ...).
- :class:`QueueState` is the container — a list of ``JobEntry`` objects with
  convenience helpers for filtering and round-trip I/O.
"""

from __future__ import annotations

import json
import logging
import pathlib
from collections.abc import Iterator
from dataclasses import dataclass, field

import yaml

from ._write_queue_state import (
    JobInfo,
    _collect_attempts,
    _finalize_attempt_records,
    _sort_key,
    _UpstreamMetadataCache,
)
from ..dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    _build_asset_metadata,
    load_assets_jsonld_metadata,
)

_log = logging.getLogger(__name__)


@dataclass
class JobEntry:
    """
    A :class:`JobInfo` (identity) plus the status fields written by
    ``write_queue_state`` and consumed across the queue module.
    """

    job: JobInfo
    content_id: str | None
    asset_size_bytes: int | None
    has_code: bool = False
    has_been_submitted: bool = False
    has_output: bool = False
    has_logs: bool = False
    created_at: str | None = None
    job_completion_time: str | None = None
    dataset_description_path: dict[str, str] = field(default_factory=dict)
    output_paths: dict[str, str] = field(default_factory=dict)
    log_paths: dict[str, str] = field(default_factory=dict)

    @property
    def is_pending(self) -> bool:
        """Code prepared but never submitted (no logs, no output yet)."""
        return self.has_code and not self.has_logs and not self.has_output

    @property
    def is_running(self) -> bool:
        """Logs present but no output yet — likely still executing."""
        return self.has_logs and not self.has_output

    @property
    def is_successful(self) -> bool:
        """Output directory present — job completed successfully."""
        return self.has_output

    @property
    def is_failed(self) -> bool:
        """Has code and logs but no output — the job ran but did not succeed."""
        return self.has_code and self.has_logs and not self.has_output

    @property
    def identity(self) -> tuple:
        """
        Stable key for matching queue/state/last-submitted entries.

        Excludes ``codebase`` deliberately: an attempt is the same logical job
        regardless of which codebase version produced it.
        """
        return (
            self.job.dandiset_id,
            self.job.dandi_path,
            self.job.pipeline,
            self.job.version,
            self.job.params,
            self.job.config,
            self.job.attempt,
        )

    def attempt_dir_candidates(self, base_dir: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
        """
        Return ``(flat_layout_path, legacy_nested_layout_path)`` for this attempt.

        :param base_dir: Root of the local Dandiset tree to resolve paths under.
        :type base_dir: pathlib.Path
        :raises ValueError: If this entry's ``dandi_path`` is an empty string.
        """
        if self.job.dandi_path == "":
            message = f"Entry has invalid dandi_path field (empty): {self!r}"
            raise ValueError(message)
        normalized_dandi_path = self.job.dandi_path.removesuffix(".nwb")

        pipeline_dir = (
            base_dir
            / "derivatives"
            / f"dandiset-{self.job.dandiset_id}"
            / pathlib.PurePosixPath(normalized_dandi_path)
            / f"pipeline-{self.job.pipeline}"
        )
        flat_attempt_dir = pipeline_dir / (
            f"version-{self.job.version}_codebase-{self.job.codebase}"
            f"_params-{self.job.params}_config-{self.job.config}_attempt-{self.job.attempt}"
        )
        nested_attempt_dir = (
            pipeline_dir
            / f"version-{self.job.version}"
            / f"params-{self.job.params}_config-{self.job.config}_attempt-{self.job.attempt}"
        )
        return flat_attempt_dir, nested_attempt_dir

    def resolve_attempt_dir(self, base_dir: pathlib.Path) -> pathlib.Path:
        """Resolve the best on-disk attempt-directory path for this entry."""
        flat_attempt_dir, nested_attempt_dir = self.attempt_dir_candidates(base_dir)
        if flat_attempt_dir.is_dir():
            return flat_attempt_dir
        if nested_attempt_dir.is_dir():
            return nested_attempt_dir

        dandiset_root = base_dir / "derivatives" / f"dandiset-{self.job.dandiset_id}"
        if not dandiset_root.is_dir():
            return nested_attempt_dir

        pipeline_dir_name = f"pipeline-{self.job.pipeline}"
        flat_attempt_dir_name = (
            f"version-{self.job.version}_codebase-{self.job.codebase}"
            f"_params-{self.job.params}_config-{self.job.config}_attempt-{self.job.attempt}"
        )
        nested_attempt_dir_name = f"params-{self.job.params}_config-{self.job.config}_attempt-{self.job.attempt}"
        for pipeline_dir in sorted(dandiset_root.rglob(pipeline_dir_name)):
            if not pipeline_dir.is_dir():
                continue
            fallback_flat_attempt_dir = pipeline_dir / flat_attempt_dir_name
            if fallback_flat_attempt_dir.is_dir():
                return fallback_flat_attempt_dir
            fallback_nested_attempt_dir = pipeline_dir / f"version-{self.job.version}" / nested_attempt_dir_name
            if fallback_nested_attempt_dir.is_dir():
                return fallback_nested_attempt_dir

        return nested_attempt_dir

    def resolve_unsubmitted_attempt_dir(self, base_dir: pathlib.Path) -> pathlib.Path | None:
        """
        Resolve the attempt directory only if this entry is queued but unsubmitted.

        Returns ``None`` when the entry is not pending (see :attr:`is_pending`) or
        when a submitted marker (``code/submitted`` or ``code/submitted_date-*``)
        is present on disk.
        """
        if not self.is_pending:
            return None

        attempt_dir = self.resolve_attempt_dir(base_dir)
        code_dir = attempt_dir / "code"
        if (code_dir / "submitted").exists() or any(code_dir.glob("submitted_date-*")):
            return None
        return attempt_dir

    @classmethod
    def from_dict(cls, data: dict, /) -> JobEntry:
        """Construct from a raw ``state.jsonl`` entry dict."""
        job = JobInfo(
            dandiset_id=data["dandiset_id"],
            dandi_path=data["dandi_path"],
            pipeline=data["pipeline"],
            version=data["version"],
            params=data["params"],
            config=data["config"],
            attempt=int(data["attempt"]),
            codebase=data["codebase"],
        )
        return cls(
            job=job,
            content_id=data.get("content_id"),
            asset_size_bytes=data.get("asset_size_bytes"),
            has_code=bool(data.get("has_code", False)),
            has_been_submitted=bool(data.get("has_been_submitted", False)),
            has_output=bool(data.get("has_output", False)),
            has_logs=bool(data.get("has_logs", False)),
            created_at=data.get("created_at"),
            job_completion_time=data.get("job_completion_time"),
            dataset_description_path=dict(data.get("dataset_description_path") or {}),
            output_paths=dict(data.get("output_paths") or {}),
            log_paths=dict(data.get("log_paths") or {}),
        )

    def to_dict(self) -> dict:
        """Serialise back to the flat dict format written to ``state.jsonl``."""
        return {
            "dandiset_id": self.job.dandiset_id,
            "dandi_path": self.job.dandi_path,
            "pipeline": self.job.pipeline,
            "version": self.job.version,
            "params": self.job.params,
            "config": self.job.config,
            "attempt": self.job.attempt,
            "codebase": self.job.codebase,
            "content_id": self.content_id,
            "asset_size_bytes": self.asset_size_bytes,
            "has_code": self.has_code,
            "has_been_submitted": self.has_been_submitted,
            "has_output": self.has_output,
            "has_logs": self.has_logs,
            "dataset_description_path": self.dataset_description_path,
            "output_paths": self.output_paths,
            "log_paths": self.log_paths,
            "created_at": self.created_at,
            "job_completion_time": self.job_completion_time,
        }


@dataclass
class QueueState:
    """
    Container for all entries in ``state.jsonl``.

    Replaces the scattered ``list[dict]`` reads in ``_prepare_queue.py``,
    ``_aggregate_queue_statistics.py``, ``_process_queue.py``, and
    ``_clean_unsubmitted_capsules.py``.
    """

    entries: list[JobEntry]

    def __iter__(self) -> Iterator[JobEntry]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    @property
    def pending(self) -> list[JobEntry]:
        """Entries with code prepared but not yet submitted."""
        return [e for e in self.entries if e.is_pending]

    @property
    def running(self) -> list[JobEntry]:
        """Entries with logs present but no output — likely still executing."""
        return [e for e in self.entries if e.is_running]

    @property
    def successful(self) -> list[JobEntry]:
        """Entries whose output directory is present."""
        return [e for e in self.entries if e.is_successful]

    @property
    def failed(self) -> list[JobEntry]:
        """Entries with code and logs but no output."""
        return [e for e in self.entries if e.is_failed]

    @property
    def successful_asset_bytes_total(self) -> int:
        """Total source-asset bytes across successful entries with a known size."""
        return sum(
            entry.asset_size_bytes
            for entry in self.entries
            if entry.is_successful
            and isinstance(entry.asset_size_bytes, int)
            and not isinstance(entry.asset_size_bytes, bool)
        )

    def content_id_to_dandiset_ids(self) -> dict[str, set[str]]:
        """
        Map each ``content_id`` to the set of source Dandiset IDs it appears under.

        A content ID is expected to map to a single source Dandiset in normal
        operation; ambiguous mappings (more than one) are surfaced so callers can
        handle them conservatively.
        """
        mapping: dict[str, set[str]] = {}
        for entry in self.entries:
            if entry.content_id and entry.job.dandiset_id:
                mapping.setdefault(entry.content_id, set()).add(entry.job.dandiset_id)
        return mapping

    def failures_for(self, *, pipeline: str, version: str) -> list[JobEntry]:
        """Failed entries matching a given pipeline and version."""
        return [e for e in self.failed if e.job.pipeline == pipeline and e.job.version == version]

    @staticmethod
    def pending_code_dirs() -> list[str]:
        """
        Identify attempt ``code`` directories awaiting submission from DANDI assets metadata.

        Loads the DANDI ``assets.jsonld`` metadata and collects every attempt
        directory that contains a ``code/submit.sh`` asset but no adjacent
        submitted-marker asset. An entry is considered submitted when a sibling
        ``submitted`` asset exists, or when a sibling asset whose name starts with
        ``submitted_date-`` exists.

        :returns: Sorted list of ``code`` directory paths (relative to the
            Dandiset root) that are pending submission. Empty when nothing is
            awaiting submission.
        """
        metadata = load_assets_jsonld_metadata()
        paths = set(metadata.path_to_asset_metadata.keys())

        pending_entries: list[str] = []
        for asset_path in sorted(paths):
            if asset_path.endswith("/code/submit.sh"):
                code_dir_path = asset_path[: -len("/submit.sh")]
                submitted_marker_prefix = f"{code_dir_path}/submitted_date-"
                has_submitted_marker = any(
                    path == f"{code_dir_path}/submitted" or path.startswith(submitted_marker_prefix) for path in paths
                )
                if not has_submitted_marker:
                    pending_entries.append(code_dir_path)

        return pending_entries

    @classmethod
    def has_pending_jobs(cls) -> bool:
        """
        Report whether any queued jobs are awaiting submission.

        Lightweight check intended to gate queue dispatch: it inspects the DANDI
        assets metadata for attempt directories that contain a ``code/submit.sh``
        asset without an adjacent submitted marker. It does not submit anything
        and does not require SLURM access.
        """
        pending_entries = cls.pending_code_dirs()
        _log.info("Found %d pending queue entries", len(pending_entries))
        return len(pending_entries) > 0

    @classmethod
    def from_jsonl(cls, file_path: pathlib.Path, /) -> QueueState:
        """
        Load from an existing ``state.jsonl`` file.

        :param file_path: Path to the ``state.jsonl`` file to read.
        :type file_path: pathlib.Path
        :raises FileNotFoundError: If *file_path* does not exist.
        """
        if not file_path.exists():
            message = f"State file not found: {file_path}"
            raise FileNotFoundError(message)
        stripped_lines = [line.strip() for line in file_path.read_text().splitlines()]
        entries = [JobEntry.from_dict(json.loads(line)) for line in stripped_lines if line]
        return cls(entries=entries)

    @classmethod
    def from_assets_yaml(cls, file_path: pathlib.Path | None = None, /) -> QueueState:
        """
        Build from DANDI assets metadata.

        When *file_path* is ``None`` the metadata is fetched from the DANDI
        S3 bucket over the network.  When a path is provided the file is read
        locally; the file should be a YAML file whose content is a list of
        asset dicts with ``path``, ``contentSize``, ``dateModified``, and
        ``contentUrl`` fields (matching the ``assets.yaml`` layout from DANDI).

        :param file_path: Optional path to a local assets YAML file.  Pass
            ``None`` to fetch from the network.
        :type file_path: pathlib.Path, optional
        """
        if file_path is None:
            local_metadata = load_assets_jsonld_metadata()
        else:
            raw = yaml.safe_load(file_path.read_text())
            if not isinstance(raw, list):
                raise ValueError(f"Expected a YAML list in {file_path}, got {type(raw).__name__}")
            content_id_to_asset: dict[str, dict] = {}
            path_to_asset_metadata: dict[str, AssetMetadata] = {}
            for asset in raw:
                if not isinstance(asset, dict):
                    continue
                try:
                    content_id, metadata = _build_asset_metadata(asset)
                except ValueError:
                    continue
                content_id_to_asset[content_id] = asset
                path_to_asset_metadata[metadata.path] = metadata
            local_metadata = AssetsJsonldMetadata(
                content_id_to_asset=content_id_to_asset,
                path_to_asset_metadata=path_to_asset_metadata,
            )

        collection = _collect_attempts(local_metadata)
        upstream_cache = _UpstreamMetadataCache()
        records = _finalize_attempt_records(collection=collection, upstream_cache=upstream_cache)
        records.sort(key=_sort_key)
        entries = [JobEntry.from_dict(record) for record in records]
        return cls(entries=entries)

    def to_file(self, file_path: pathlib.Path, /) -> None:
        """
        Write all entries to *file_path* as newline-delimited JSON.

        :param file_path: Destination path; the file is overwritten if it
            already exists.
        :type file_path: pathlib.Path
        """
        with file_path.open(mode="w") as file_stream:
            for entry in self.entries:
                file_stream.write(json.dumps(entry.to_dict()) + "\n")
