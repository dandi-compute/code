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

import collections
import datetime
import gzip
import json
import logging
import os
import pathlib
import random
import shutil
import subprocess
import tempfile
import time
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Literal

import yaml

from ._globals import _AIND_EPHYS_PARAMS_REGISTRY
from ._queue_utils import (
    JobInfo,
    _collect_attempts,
    _duration_string_to_seconds,
    _extract_error_lines,
    _extract_nextflow_timeline_data,
    _finalize_attempt_records,
    _list_capsule_log_directories,
    _load_queue_config,
    _order_content_ids_for_uniform_dandiset_sampling,
    _remove_empty_parents,
    _sort_key,
    _UpstreamMetadataCache,
)
from ..aind_ephys_pipeline import UnmappedContentIDError, prepare_aind_ephys_job
from ..dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID, _JOB_CAPSULES_DANDISET_ID
from ..dandiset._load_assets_jsonld_metadata import (
    AssetMetadata,
    AssetsJsonldMetadata,
    _build_asset_metadata,
    load_assets_jsonld_metadata,
)

_log = logging.getLogger(__name__)

#: Dandiset whose assets back the pending/submission queries (the job capsules Dandiset).
_DANDISET_ID = _JOB_CAPSULES_DANDISET_ID


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
            **self.job.to_dict(),
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

    def entry_for(self, *, dandi_path: str, attempt: int = 1) -> JobEntry:
        """
        Return the entry with the given ``dandi_path`` (and ``attempt``).

        :param dandi_path: The ``dandi_path`` recorded on the target entry.
        :param attempt: Disambiguates scenarios that use more than one attempt of
            the same asset.
        :raises KeyError: If no entry matches *dandi_path* and *attempt*.
        """
        for entry in self.entries:
            if entry.job.dandi_path == dandi_path and entry.job.attempt == attempt:
                return entry
        message = f"No entry with dandi_path={dandi_path!r} and attempt={attempt}"
        raise KeyError(message)

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
    def submit_next(
        cls,
        *,
        processing_directory: pathlib.Path,
        max_submissions: int = 2,
        test: bool = False,
    ) -> bool:
        """
        Submit the next eligible pending entries from the DANDI assets metadata.

        Identifies all attempt directories that contain a ``code/submit.sh`` asset
        but no adjacent submitted-marker asset (see :meth:`pending_code_dirs`). For
        each candidate (up to *max_submissions*), a temporary working directory is
        created inside *processing_directory*, the ``code/`` tree is downloaded via
        ``dandi download --preserve-tree``, the submission script is executed via
        ``sbatch``, a submitted marker is written adjacent to ``submit.sh``, the
        marker is pushed back to the archive via ``dandi upload --allow-any-path``,
        and the temporary directory is removed on success.

        :param processing_directory: Directory in which temporary per-job working
            trees are created.
        :type processing_directory: pathlib.Path
        :param max_submissions: Maximum number of pending jobs to submit.
        :type max_submissions: int
        :param test: When ``True``, leave temporary working directories on disk
            after successful submission for debugging.
        :type test: bool
        :returns: ``True`` if at least one job was submitted, ``False`` otherwise.
        :rtype: bool
        :raises RuntimeError: If ``dandi download``, ``sbatch``, or ``dandi upload``
            returns a non-zero exit code for any candidate.
        """
        if max_submissions < 1:
            return False

        candidates = cls.pending_code_dirs()

        if not candidates:
            _log.info("No eligible pending entries available for submission")
            return False

        for code_dir_path in candidates[:max_submissions]:
            dandi_url = f"dandi://dandi/{_DANDISET_ID}/{code_dir_path}/"
            # Temporary directory is intentionally left on disk when any step fails
            # so that it can be inspected for debugging.
            temp_dir = pathlib.Path(tempfile.mkdtemp(dir=processing_directory, prefix="submit-next-"))
            _log.info("Submitting job run for %s in %s", code_dir_path, temp_dir)

            result = subprocess.run(
                ["dandi", "download", "--preserve-tree", dandi_url],
                capture_output=True,
                text=True,
                cwd=temp_dir,
            )
            _log.info("dandi download returned code %d for %s", result.returncode, dandi_url)
            _log.debug("dandi download stdout: %s\nstderr: %s", result.stdout, result.stderr)
            if result.returncode != 0:
                _log.warning("dandi download stdout: %s\nstderr: %s", result.stdout, result.stderr)
                message = f"dandi download failed for {dandi_url}"
                raise RuntimeError(message)

            dandiset_directory = temp_dir / _DANDISET_ID
            submit_sh_path = dandiset_directory / code_dir_path / "submit.sh"
            result = subprocess.run(
                ["sbatch", str(submit_sh_path.absolute())],
                capture_output=True,
                text=True,
            )
            _log.info("sbatch returned code %d; stdout %s", result.returncode, result.stdout)
            _log.debug("sbatch stdout: %s\nstderr: %s", result.stdout, result.stderr)
            if result.returncode != 0:
                _log.warning("sbatch stdout: %s\nstderr: %s", result.stdout, result.stderr)
                message = "sbatch submission failed - please check the logs to see more details."
                raise RuntimeError(message)

            now = datetime.datetime.now()
            submitted_marker = submit_sh_path.parent / (
                f"submitted_date-{now.year:04d}+{now.month:02d}+{now.day:02d}"
                f"_time-{now.hour:02d}+{now.minute:02d}+{now.second:02d}"
            )
            submitted_marker.write_bytes(b"1")
            _log.info("Created `submitted` file at: %s", submitted_marker.absolute())

            result = subprocess.run(
                ["dandi", "upload", "--allow-any-path"],
                capture_output=True,
                text=True,
                cwd=dandiset_directory,
            )
            _log.info("dandi upload returned code %d", result.returncode)
            _log.debug("dandi upload stdout: %s\nstderr: %s", result.stdout, result.stderr)
            if result.returncode != 0:
                _log.warning("dandi upload stdout: %s\nstderr: %s", result.stdout, result.stderr)
                message = "dandi upload failed - please check the logs to see more details."
                raise RuntimeError(message)

            if test:
                _log.info("Leaving temporary directory in place for test mode: %s", temp_dir)
            else:
                shutil.rmtree(temp_dir)

        return True

    @staticmethod
    def count_running_aind_ephys_pipeline_jobs() -> int:
        """
        Count currently running AIND Ephys pipeline jobs via the SLURM scheduler.

        Calls ``squeue --me --format=%j`` and counts jobs whose name is exactly
        ``AIND-Ephys-Pipeline``.

        :raises RuntimeError: If the ``squeue`` invocation exits non-zero and writes
            to standard error.
        """
        command = ["squeue", "--me", "--format=%j"]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0 and result.stderr:
            message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
            raise RuntimeError(message)
        if result.stderr:
            _log.warning(result.stderr)
        return sum(1 for line in result.stdout.splitlines() if line.strip() == "AIND-Ephys-Pipeline")

    @staticmethod
    def load_queue_config(*, queue_directory: pathlib.Path) -> dict:
        """
        Read and validate ``queue_config.json`` under *queue_directory*.

        :raises FileNotFoundError: If ``queue_config.json`` is not found.
        :raises ValueError: If the queue configuration fails LinkML validation.
        """
        return _load_queue_config(queue_directory=queue_directory)

    @staticmethod
    def resolve_params_key_to_id(pipeline: str, params_key: str) -> str:
        """
        Resolve a human-readable parameters key to its 7-character hash ID.

        For the ``aind+ephys`` pipeline the lookup is performed against the
        registered params registry. For any other pipeline, or if the key is not
        found, *params_key* is returned unchanged so callers that already store raw
        hash IDs continue to work.
        """
        if pipeline == "aind+ephys":
            entry = _AIND_EPHYS_PARAMS_REGISTRY.get(params_key)
            if entry:
                return entry["md5"][:7]
        return params_key

    @classmethod
    def from_assets(cls, *, dandiset_id: str = _JOB_CAPSULES_DANDISET_ID) -> QueueState:
        """
        Build a queue state from a Dandiset's DANDI ``assets.jsonld`` metadata.

        Each entry represents one attempt capsule inferred from the
        ``derivatives/dandiset-*/.../pipeline-*/..._attempt-*`` path structure, with
        ``content_id`` / ``asset_size_bytes`` resolved from the upstream source
        Dandiset's ``assets.jsonld``.

        :param dandiset_id: The Dandiset whose ``assets.jsonld`` is read. Defaults to
            the job capsules Dandiset (``001697``).
        :type dandiset_id: str
        """
        local_metadata = load_assets_jsonld_metadata(dandiset_id=dandiset_id)
        collection = _collect_attempts(local_metadata)
        upstream_cache = _UpstreamMetadataCache()
        records = _finalize_attempt_records(collection=collection, upstream_cache=upstream_cache)
        records.sort(key=_sort_key)
        return cls(entries=[JobEntry.from_dict(record) for record in records])

    @classmethod
    def write_state(
        cls,
        *,
        queue_directory: pathlib.Path,
        dandiset_id: str = _JOB_CAPSULES_DANDISET_ID,
        state_file_name: str = "state.jsonl",
    ) -> None:
        """
        Write a queue state file from DANDI ``assets.jsonld`` metadata.

        Validates ``queue_config.json`` under *queue_directory*, builds the state via
        :meth:`from_assets`, and writes it to ``queue_directory/state_file_name``.

        :param queue_directory: Path to the queue root directory.
        :type queue_directory: pathlib.Path
        :param dandiset_id: The Dandiset whose ``assets.jsonld`` portrays the state.
        :type dandiset_id: str
        :param state_file_name: Name of the state file written under *queue_directory*.
        :type state_file_name: str
        :raises FileNotFoundError: If ``queue_config.json`` is not found.
        :raises ValueError: If the queue configuration fails LinkML validation.
        """
        _load_queue_config(queue_directory=queue_directory)
        state = cls.from_assets(dandiset_id=dandiset_id)
        state.to_file(queue_directory / state_file_name)

    @classmethod
    def write_archive_state(cls, *, queue_directory: pathlib.Path) -> None:
        """
        Write ``archive_state.jsonl`` from the failed runs archive ``assets.jsonld``.

        The archive counterpart to :meth:`write_state`; produces an identically
        structured state file adjacent to ``state.jsonl`` portraying the failed runs
        archive Dandiset (``001873``) rather than the job capsules Dandiset.

        :param queue_directory: Path to the queue root directory.
        :type queue_directory: pathlib.Path
        """
        cls.write_state(
            queue_directory=queue_directory,
            dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
            state_file_name="archive_state.jsonl",
        )

    def aggregate_statistics(
        self,
        *,
        queue_directory: pathlib.Path,
        dandiset_directory: pathlib.Path,
        output_file_name: str = "queue_stats.json",
    ) -> dict:
        """Write aggregate queue statistics JSON and return the written payload."""
        job_step_wall_time_seconds: collections.defaultdict[str, float] = collections.defaultdict(float)
        timeline_files_processed = 0
        for entry in self.entries:
            attempt_dir = entry.resolve_attempt_dir(dandiset_directory)
            timeline_file = attempt_dir / "logs" / "timeline.html"
            if not timeline_file.is_file():
                continue

            timeline_data = _extract_nextflow_timeline_data(timeline_html=timeline_file.read_text())
            if timeline_data is None:
                continue

            processes = timeline_data.get("processes")
            if not isinstance(processes, list):
                continue
            timeline_files_processed += 1

            for process in processes:
                if not isinstance(process, dict):
                    continue
                process_label = process.get("label")
                if not isinstance(process_label, str):
                    continue
                step_name = process_label.split(" (", 1)[0]
                times = process.get("times")
                if not isinstance(times, list):
                    continue
                for step in times:
                    if not isinstance(step, dict):
                        continue
                    duration_label = step.get("label")
                    if not isinstance(duration_label, str):
                        continue
                    duration_string = duration_label.split("/", 1)[0].strip()
                    duration_seconds = _duration_string_to_seconds(duration_string)
                    if duration_seconds > 0:
                        job_step_wall_time_seconds[step_name] += duration_seconds

        statistics = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "state_entry_count": len(self.entries),
            "successful_asset_bytes_total": self.successful_asset_bytes_total,
            "timeline_files_processed": timeline_files_processed,
            "job_step_wall_time_seconds": {
                key: value for key, value in sorted(job_step_wall_time_seconds.items(), key=lambda item: item[0])
            },
        }

        output_file = queue_directory / output_file_name
        output_file.write_text(json.dumps(statistics, indent=2, sort_keys=True) + "\n")
        return statistics

    def clean_unsubmitted_capsules(self, *, dandiset_directory: pathlib.Path) -> list[pathlib.Path]:
        """
        Remove all queued (unsubmitted) capsule directories from the dandiset tree.

        A capsule is *queued* when its attempt directory has a ``code/`` subdirectory
        but no ``logs/`` or ``derivatives/`` content and no submitted marker. Each
        matching attempt directory is deleted from the DANDI archive (via ``dandi
        delete``) and the local filesystem.

        :param dandiset_directory: Local clone of the dandiset used to resolve and
            delete matching attempt directories.
        :type dandiset_directory: pathlib.Path
        :returns: Attempt directory paths that were deleted.
        :rtype: list[pathlib.Path]
        :raises RuntimeError: If ``DANDI_API_KEY`` is not set or is blank.
        """
        if not os.environ.get("DANDI_API_KEY", "").strip():
            message = "`DANDI_API_KEY` environment variable is not set or is blank."
            raise RuntimeError(message)

        cleanable_attempt_dirs = [
            attempt_dir
            for entry in self.entries
            if (attempt_dir := entry.resolve_unsubmitted_attempt_dir(dandiset_directory)) is not None
        ]

        removed: list[pathlib.Path] = []
        for attempt_dir in cleanable_attempt_dirs:
            if attempt_dir.is_dir():
                parent_dir = attempt_dir.parent
                subprocess.run(
                    ["dandi", "delete", str(attempt_dir)],
                    input=b"y\n",
                    check=True,
                )
                shutil.rmtree(attempt_dir)
                _remove_empty_parents(start=parent_dir, stop=dandiset_directory / "derivatives")
                removed.append(attempt_dir)

        return removed

    @classmethod
    def process_queue(
        cls,
        *,
        queue_directory: pathlib.Path,
        processing_directory: pathlib.Path,
        max_concurrent_aind_jobs: int = 2,
        jitter_seconds: float = 30.0,
        test: bool = False,
    ) -> Literal["submitted", "no-pending", "slots-unavailable"]:
        """
        Submit jobs from ``state.jsonl`` up to ``max_concurrent_aind_jobs`` total
        running ``AIND-Ephys-Pipeline`` SLURM jobs.

        :param queue_directory: Path to the queue root directory.
        :param processing_directory: Directory for temporary working trees during submission.
        :param max_concurrent_aind_jobs: Maximum concurrent ``AIND-Ephys-Pipeline`` jobs.
        :param jitter_seconds: Maximum random delay (seconds) before processing; ``0`` disables.
        :param test: If ``True``, preserve temporary processing directories on success.
        :raises FileNotFoundError: If ``state.jsonl`` is not found in *queue_directory*.
        :raises ValueError: If *jitter_seconds* is negative or *max_concurrent_aind_jobs* < 1.
        """
        if jitter_seconds < 0:
            message = "jitter_seconds must be non-negative"
            raise ValueError(message)
        if jitter_seconds > 0:
            delay = random.uniform(0, jitter_seconds)
            _log.info("Sleeping %.2f seconds (jitter) before processing queue", delay)
            time.sleep(delay)

        state_file = queue_directory / "state.jsonl"
        if not state_file.exists():
            message = f"State file not found: {state_file}"
            raise FileNotFoundError(message)
        if max_concurrent_aind_jobs < 1:
            message = "max_concurrent_aind_jobs must be at least 1"
            raise ValueError(message)
        if not state_file.read_text().strip():
            _log.info(f"No entries in {state_file}")
            return "no-pending"

        running_count = cls.count_running_aind_ephys_pipeline_jobs()
        available_slots = max(0, max_concurrent_aind_jobs - running_count)
        if available_slots < 1:
            return "slots-unavailable"

        submitted_any = cls.submit_next(
            processing_directory=processing_directory,
            max_submissions=available_slots,
            test=test,
        )
        return "submitted" if submitted_any else "no-pending"

    @classmethod
    def prepare(
        cls,
        *,
        queue_directory: pathlib.Path,
        pipeline_directory: pathlib.Path | None = None,
        config_key: str = "default",
        content_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> None:
        """
        En-masse preparation of qualifying assets based on the current queue config.

        For every pipeline/version/params combination declared in
        ``queue_config.json`` this determines which content IDs to prepare and calls
        :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job` for
        each asset. The per-pipeline failure cap (``max_fail_per_dandiset``) is
        enforced by reading the existing ``state.jsonl`` under *queue_directory*.

        :param queue_directory: Path to the queue root directory.
        :param pipeline_directory: Local path to the AIND pipeline repository.
        :param config_key: Key for a registered job configuration.
        :param content_ids: Explicit content IDs to prepare; when provided, the
            qualifying list is not fetched from the network.
        :param limit: If provided, stop after preparing *limit* assets in total.
        """
        queue_config = _load_queue_config(queue_directory=queue_directory)

        if content_ids is None:
            qualifying_aind_content_ids_url = (
                "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/dist/"
                "derivatives/qualifying_aind_content_ids.jsonl.gz"
            )
            with urllib.request.urlopen(url=qualifying_aind_content_ids_url) as response:
                decompressed = gzip.decompress(response.read()).decode()
                fetched_content_ids = [json.loads(line) for line in decompressed.splitlines() if line.strip()]
            content_ids = _order_content_ids_for_uniform_dandiset_sampling(content_ids=fetched_content_ids)

        state_file = queue_directory / "state.jsonl"
        state = cls.from_jsonl(state_file) if state_file.exists() else cls(entries=[])
        content_id_to_dandiset_ids = state.content_id_to_dandiset_ids()

        prepared_count = 0
        for pipeline_name, pipeline_data in queue_config.get("pipelines", {}).items():
            if limit is not None and prepared_count >= limit:
                break
            for version in pipeline_data.get("version_priority", []):
                if limit is not None and prepared_count >= limit:
                    break
                for params in pipeline_data.get("params_priority", []):
                    if limit is not None and prepared_count >= limit:
                        break
                    pipeline_cfg = queue_config["pipelines"][pipeline_name]

                    max_fail = pipeline_cfg.get("max_fail_per_dandiset")
                    failure_count_by_dandiset: collections.defaultdict[str, int] = collections.defaultdict(int)
                    if max_fail is not None:
                        for entry in state.failures_for(pipeline=pipeline_name, version=version):
                            dandiset_id = entry.job.dandiset_id
                            if not dandiset_id:
                                continue
                            failure_count_by_dandiset[dandiset_id] += 1

                    for content_id in content_ids:
                        if limit is not None and prepared_count >= limit:
                            break
                        if max_fail is not None:
                            dandiset_ids = content_id_to_dandiset_ids.get(content_id, set())
                            if len(dandiset_ids) == 1:
                                dandiset_id = next(iter(dandiset_ids))
                                failure_count = failure_count_by_dandiset.get(dandiset_id, 0)
                                if failure_count >= max_fail:
                                    _log.info(
                                        f"Skipping preparation for {pipeline_name}/{version}/{params}/{content_id}: "
                                        f"failure count ({failure_count}) for dandiset-{dandiset_id} has reached "
                                        f"max_fail_per_dandiset ({max_fail})."
                                    )
                                    continue
                            else:
                                mapped_dandisets = ", ".join(sorted(dandiset_ids)) if dandiset_ids else "<none>"
                                _log.info(
                                    f"Preparing {content_id} without max_fail_per_dandiset enforcement for "
                                    f"{pipeline_name}/{version}/{params}: expected exactly 1 mapped dandiset but "
                                    f"found {len(dandiset_ids)} ({mapped_dandisets})."
                                )

                        _log.info(f"Preparing content ID: {content_id}")
                        try:
                            prepare_aind_ephys_job(
                                content_id=content_id,
                                parameters_key=params,
                                pipeline_version=version,
                                pipeline_directory=pipeline_directory,
                                config_key=config_key,
                                silent=True,
                            )
                        except UnmappedContentIDError as error:
                            _log.warning(
                                f"Skipping preparation for {pipeline_name}/{version}/{params}/{content_id}: {error}"
                            )
                            continue
                        prepared_count += 1

    @staticmethod
    def dump_issues(
        *,
        dandiset_directory: pathlib.Path,
        queue_directory: pathlib.Path,
        output_file_name: str = "issues_dump.json",
    ) -> list[dict]:
        """Scan nextflow/slurm logs and write per-capsule error lines under *queue_directory*."""
        records: list[dict] = []
        for logs_dir in _list_capsule_log_directories(dandiset_directory=dandiset_directory):
            nextflow_log = logs_dir / "nextflow.log"
            slurm_logs = sorted(path for path in logs_dir.glob("*slurm.log") if path.is_file())

            nextflow_errors = _extract_error_lines(log_file=nextflow_log)
            slurm_errors = {log_file.name: _extract_error_lines(log_file=log_file) for log_file in slurm_logs}
            slurm_errors = {key: value for key, value in slurm_errors.items() if value}
            if not nextflow_errors and not slurm_errors:
                continue

            records.append(
                {
                    "capsule_path": logs_dir.parent.relative_to(dandiset_directory).as_posix(),
                    "nextflow_log": (
                        nextflow_log.relative_to(dandiset_directory).as_posix() if nextflow_log.is_file() else None
                    ),
                    "nextflow_errors": nextflow_errors,
                    "slurm_errors": {
                        log_name: errors for log_name, errors in sorted(slurm_errors.items(), key=lambda item: item[0])
                    },
                }
            )

        payload = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "capsule_count": len(records),
            "records": records,
        }
        (queue_directory / output_file_name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        return records

    @staticmethod
    def summarize_issues(
        *,
        dandiset_directory: pathlib.Path,
        queue_directory: pathlib.Path,
        dump_output_file_name: str = "issues_dump.json",
        output_file_name: str = "issues_summary.json",
    ) -> dict[str, list[str]]:
        """Write descending error-frequency summary where keys are counts and values are error strings."""
        records = QueueState.dump_issues(
            dandiset_directory=dandiset_directory,
            queue_directory=queue_directory,
            output_file_name=dump_output_file_name,
        )

        counts: collections.Counter[str] = collections.Counter()
        for record in records:
            counts.update(record.get("nextflow_errors", []))
            for errors in record.get("slurm_errors", {}).values():
                counts.update(errors)

        errors_by_count: dict[str, list[str]] = collections.defaultdict(list)
        for message, count in sorted(counts.items(), key=lambda message_count: (-message_count[1], message_count[0])):
            errors_by_count[str(count)].append(message)

        summary = {
            count: messages
            for count, messages in sorted(
                errors_by_count.items(),
                key=lambda count_messages: int(count_messages[0]),
                reverse=True,
            )
        }
        output_payload = {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "summary": summary,
        }
        (queue_directory / output_file_name).write_text(json.dumps(output_payload, indent=2, sort_keys=True) + "\n")
        return summary

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
