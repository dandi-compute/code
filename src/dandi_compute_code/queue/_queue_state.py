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
