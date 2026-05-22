import fcntl
import gzip
import json
import os
import pathlib
import re
import shutil
import subprocess
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import linkml_runtime.processing.referencevalidator
import linkml_runtime.utils.schemaview

from ..aind_ephys_pipeline import prepare_aind_ephys_job, submit_job
from ..dandiset import scan_dandiset_directory

_AIND_EPHYS_PARAMS_REGISTRY_PATH = (
    pathlib.Path(__file__).parent.parent / "aind_ephys_pipeline" / "registries" / "registered_params.json"
)
_QUEUE_CONFIG_SCHEMA_PATH = pathlib.Path(__file__).parent / "schemas" / "queue_config.linkml.yaml"
_FLAT_ATTEMPT_DIR_RE = re.compile(r"^version-(?P<version>.+?)_params-[^_]+_config-.+_attempt-\d+$")
_DURATION_PART_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ms|s|m|h|d)\b")
TEST_QUEUE_CONTENT_ID = "048d1ee9-83b7-491f-8f02-1ca615b1d455"

try:
    _AIND_EPHYS_PARAMS_REGISTRY: dict = json.loads(_AIND_EPHYS_PARAMS_REGISTRY_PATH.read_text())
except (OSError, json.JSONDecodeError):
    _AIND_EPHYS_PARAMS_REGISTRY = {}


def _validate_queue_config(*, queue_config: dict) -> None:
    """Validate queue_config payload (top-level ``pipelines`` mapping) against the LinkML schema."""
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
    """Read queue_config.json and validate it against the LinkML schema."""
    queue_config_file = queue_directory / "queue_config.json"
    if not queue_config_file.exists():
        message = f"'queue_config.json' not found in '{queue_directory}'."
        raise FileNotFoundError(message)

    queue_config = json.loads(queue_config_file.read_text())
    _validate_queue_config(queue_config=queue_config)
    return queue_config


def _resolve_params_key_to_id(pipeline: str, params_key: str) -> str:
    """
    Resolve a human-readable parameters key to its 7-character hash ID.

    ``queue_config.json`` stores parameters references as human-readable key
    names (e.g. ``"default"``), but the on-disk directory names — and therefore
    the ``params`` field recorded by :func:`scan_dandiset_directory` — use the
    first seven hex characters of the MD5 checksum of the parameters file (e.g.
    ``"98fd947"``).  This function bridges that gap by looking up the registered
    MD5 for a known key.

    For the ``aind+ephys`` pipeline the lookup is performed against
    ``registered_params.json`` inside the pipeline module.  For any other
    pipeline, or if the key is not found in the registry, the *params_key* is
    returned unchanged so that callers that already store raw hash IDs continue
    to work.

    Parameters
    ----------
    pipeline : str
        The pipeline name as recorded in the state entry (e.g. ``"aind+ephys"``).
    params_key : str
        The human-readable key from ``params_priority`` in ``queue_config.json``
        (e.g. ``"default"``), or a raw hash ID.

    Returns
    -------
    str
        The 7-character hash ID corresponding to *params_key*, or *params_key*
        itself if no mapping is found.
    """
    if pipeline == "aind+ephys":
        entry = _AIND_EPHYS_PARAMS_REGISTRY.get(params_key)
        if entry:
            return entry["md5"][:7]
    return params_key


def _version_matches(state_version: str, config_version: str) -> bool:
    """
    Return True if *state_version* matches *config_version*.

    An exact match is always accepted.  Additionally, a *state_version* that
    extends *config_version* by appending ``+<7 hex chars>`` (the first 7
    characters of the dandi-compute/code repo commit hash) is also accepted,
    providing backward-compatible matching when the queue config does not yet
    include the code-repo suffix.  The suffix is validated against the pattern
    ``[0-9a-f]{7,40}`` to accommodate any valid git short-hash length.
    """
    if state_version == config_version:
        return True
    if state_version.startswith(config_version + "+"):
        extra = state_version[len(config_version) + 1 :]
        return bool(re.fullmatch(r"[0-9a-f]{7,40}", extra))
    return False


def _build_processing_order(
    *,
    state_entries: list[dict],
    queue_config: dict,
) -> list[dict]:
    """
    Build an ordered list of pending entries using zipper-style interleaving.

    Filters *state_entries* to those that are prepared but not yet run
    (``has_code=True``, ``has_logs=False``, ``has_output=False``).  For each
    pipeline and version declared in ``queue_config``, dandiset instances
    (identified by ``dandiset_id``, ``dandi_path``, and ``config``)
    are sorted by their earliest ``created_at`` timestamp.  Within each
    instance the entries are ordered by ``params_priority``, ensuring that all
    parameterizations of a dandiset for a given version are queued before
    moving on to the next dandiset or version.

    Parameters
    ----------
    state_entries : list[dict]
        Records produced by :func:`~dandi_compute_code.dandiset.scan_dandiset_directory`
        (or loaded from a ``state.jsonl`` file).
    queue_config : dict
        Parsed contents of ``queue_config.json``.  Expected shape::

            {
                "pipelines": {
                    "<pipeline_name>": {
                        "version_priority": [...],
                        "params_priority": [...]
                    }
                }
            }

    Returns
    -------
    list[dict]
        Ordered list of pending entries ready to be submitted.
    """
    pending = [e for e in state_entries if e.get("has_code") and not e.get("has_output") and not e.get("has_logs")]

    result: list[dict] = []
    for pipeline_name, pipeline_data in queue_config.get("pipelines", {}).items():
        version_priority = pipeline_data.get("version_priority", [])
        params_priority = pipeline_data.get("params_priority", [])

        for version in version_priority:
            version_entries = [
                e
                for e in pending
                if e.get("pipeline") == pipeline_name and _version_matches(e.get("version", ""), version)
            ]

            # Group by dandiset instance: (dandiset_id, dandi_path, config)
            instance_groups: dict[tuple, list[dict]] = {}
            for entry in version_entries:
                key = (
                    entry.get("dandiset_id", ""),
                    entry.get("dandi_path", ""),
                    entry.get("config", ""),
                )
                instance_groups.setdefault(key, []).append(entry)

            # Sort instances by their earliest created_at timestamp
            sorted_instances = sorted(
                instance_groups.items(),
                key=lambda kv: min(e.get("created_at", "") for e in kv[1]),
            )

            # Zipper: for each instance add entries in params_priority order
            for _key, entries in sorted_instances:
                for params_key in params_priority:
                    params_id = _resolve_params_key_to_id(pipeline_name, params_key)
                    matching = [e for e in entries if e.get("params") == params_id]
                    result.extend(matching)

    return result


def _count_dandiset_failures(
    *,
    dandiset_directory: pathlib.Path,
    version: str,
) -> int:
    """
    Count failure attempt directories across all source dandisets for a given version.

    Scans every ``derivatives/dandiset-*`` sub-directory inside *dandiset_directory*
    (i.e. the local clone of the 001697 dandiset repository) and counts attempt
    directories that contain a ``code/`` subdirectory, a non-empty ``logs/``
    subdirectory, but **no** ``derivatives/`` subdirectory — the signature of a job that
    ran but did not produce output.  Pending entries (code present but logs empty or
    absent) are not counted.

    A directory is considered an attempt if its name ends with ``_attempt-<number>``
    and either:

    * its immediate parent is named ``version-{version}`` (legacy layout), or
    * its immediate parent is ``pipeline-*`` and the attempt directory name starts
      with ``version-{version}_`` (current flat layout).

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to the local clone of the 001697 dandiset repository.
    version : str
        The BIDS-encoded pipeline version string as stored in the directory name
        (e.g., ``'v1.0.0+fixes+47bd492'``).

    Returns
    -------
    int
        Total number of failed attempt directories across all source dandisets for
        the given *version*.
    """
    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return 0

    failure_count = 0
    attempt_re = re.compile(r"_attempt-\d+$")
    version_dir_name = f"version-{version}"

    for dandiset_path in derivatives.iterdir():
        if not dandiset_path.is_dir() or not dandiset_path.name.startswith("dandiset-"):
            continue
        for attempt_dir in dandiset_path.rglob("*_attempt-*"):
            if not attempt_dir.is_dir():
                continue
            if not attempt_re.search(attempt_dir.name):
                continue
            parent_name = attempt_dir.parent.name
            if parent_name == version_dir_name:
                is_matching_version = True
            elif parent_name.startswith("pipeline-"):
                flat_match = _FLAT_ATTEMPT_DIR_RE.fullmatch(attempt_dir.name)
                is_matching_version = bool(flat_match and flat_match.group("version") == version)
            else:
                is_matching_version = False
            if not is_matching_version:
                continue
            logs_dir = attempt_dir / "logs"
            has_logs = logs_dir.is_dir() and any(logs_dir.iterdir())
            if (attempt_dir / "code").is_dir() and has_logs and not (attempt_dir / "derivatives").is_dir():
                failure_count += 1

    return failure_count


def _count_running_aind_ephys_pipeline_jobs() -> int:
    """
    Count currently running AIND Ephys pipeline jobs via the SLURM scheduler.

    Calls ``squeue --me --format=%j`` and counts jobs whose name is exactly
    ``AIND-Ephys-Pipeline``.

    Returns
    -------
    int
        Number of running jobs with SLURM job-name ``AIND-Ephys-Pipeline``.
    """
    command = ["squeue", "--me", "--format=%j"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)
    if result.stderr:
        print(result.stderr)
    return sum(1 for line in result.stdout.splitlines() if line.strip() == "AIND-Ephys-Pipeline")


def _attempt_dir_candidates(*, base_dir: pathlib.Path, entry: dict) -> tuple[pathlib.Path, pathlib.Path]:
    """Return (flat_layout_path, legacy_nested_layout_path) for an attempt entry."""
    dandiset_id = entry["dandiset_id"]
    dandi_path = entry.get("dandi_path")
    if dandi_path is None:
        message = f"Entry has invalid dandi_path field (missing): {entry!r}"
        raise ValueError(message)
    if dandi_path == "":
        message = f"Entry has invalid dandi_path field (empty): {entry!r}"
        raise ValueError(message)
    pipeline = entry["pipeline"]
    version = entry["version"]
    params = entry["params"]
    config = entry["config"]
    attempt = entry["attempt"]

    pipeline_dir = base_dir / "derivatives" / f"dandiset-{dandiset_id}" / pathlib.PurePosixPath(dandi_path)
    pipeline_dir = pipeline_dir / f"pipeline-{pipeline}"

    flat_attempt_dir = pipeline_dir / f"version-{version}_params-{params}_config-{config}_attempt-{attempt}"
    nested_attempt_dir = pipeline_dir / f"version-{version}" / f"params-{params}_config-{config}_attempt-{attempt}"
    return flat_attempt_dir, nested_attempt_dir


def _resolve_attempt_dir(*, base_dir: pathlib.Path, entry: dict) -> pathlib.Path:
    """Resolve the best attempt-directory path for a queue entry."""
    flat_attempt_dir, nested_attempt_dir = _attempt_dir_candidates(base_dir=base_dir, entry=entry)
    if flat_attempt_dir.is_dir():
        return flat_attempt_dir
    if nested_attempt_dir.is_dir():
        return nested_attempt_dir

    dandiset_root = base_dir / "derivatives" / f"dandiset-{entry['dandiset_id']}"
    if not dandiset_root.is_dir():
        return nested_attempt_dir

    pipeline_dir_name = f"pipeline-{entry['pipeline']}"
    flat_attempt_dir_name = (
        f"version-{entry['version']}_params-{entry['params']}_config-{entry['config']}_attempt-{entry['attempt']}"
    )
    nested_attempt_dir_name = f"params-{entry['params']}_config-{entry['config']}_attempt-{entry['attempt']}"
    for pipeline_dir in sorted(dandiset_root.rglob(pipeline_dir_name)):
        if not pipeline_dir.is_dir():
            continue
        fallback_flat_attempt_dir = pipeline_dir / flat_attempt_dir_name
        if fallback_flat_attempt_dir.is_dir():
            return fallback_flat_attempt_dir
        fallback_nested_attempt_dir = pipeline_dir / f"version-{entry['version']}" / nested_attempt_dir_name
        if fallback_nested_attempt_dir.is_dir():
            return fallback_nested_attempt_dir

    return nested_attempt_dir


def _entry_identity(entry: dict) -> tuple:
    """Return a stable tuple key for matching queue/state/last-submitted entries."""
    return (
        entry.get("dandiset_id"),
        entry.get("dandi_path"),
        entry.get("pipeline"),
        entry.get("version"),
        entry.get("params"),
        entry.get("config"),
        entry.get("attempt"),
    )


def _prune_last_submitted(*, queue_directory: pathlib.Path, state_entries: list[dict]) -> None:
    """Remove last_submitted entries that now have logs or output in state."""
    last_submitted_file = queue_directory / "last_submitted.jsonl"
    if not last_submitted_file.exists():
        return

    entries_to_prune = {_entry_identity(e) for e in state_entries if e.get("has_output") or e.get("has_logs")}

    last_submitted_entries = [
        json.loads(line.strip()) for line in last_submitted_file.read_text().splitlines() if line.strip()
    ]
    filtered = [entry for entry in last_submitted_entries if _entry_identity(entry) not in entries_to_prune]
    last_submitted_file.write_text("".join(json.dumps(entry) + "\n" for entry in filtered))


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
    """Extract ``window.data`` JSON payload from a Nextflow timeline HTML report."""
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


def aggregate_queue_statistics(
    *,
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    output_file_name: str = "queue_stats.json",
) -> dict:
    """Write aggregate queue statistics JSON and return the written payload."""
    state_file = queue_directory / "state.jsonl"
    state_entries = (
        [json.loads(line.strip()) for line in state_file.read_text().splitlines() if line.strip()]
        if state_file.exists()
        else []
    )

    successful_asset_bytes_total = sum(
        entry["asset_size_bytes"]
        for entry in state_entries
        if entry.get("has_output")
        and isinstance(entry.get("asset_size_bytes"), int)
        and not isinstance(entry.get("asset_size_bytes"), bool)
    )

    job_step_wall_time_seconds: defaultdict[str, float] = defaultdict(float)
    timeline_files_processed = 0
    for entry in state_entries:
        attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)
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
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "state_entry_count": len(state_entries),
        "successful_asset_bytes_total": successful_asset_bytes_total,
        "timeline_files_processed": timeline_files_processed,
        "job_step_wall_time_seconds": {
            key: value for key, value in sorted(job_step_wall_time_seconds.items(), key=lambda item: item[0])
        },
    }

    output_file = queue_directory / output_file_name
    output_file.write_text(json.dumps(statistics, indent=2, sort_keys=True) + "\n")
    return statistics


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


def clean_unsubmitted_capsules(
    *,
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
) -> list[pathlib.Path]:
    """
    Remove all queued (unsubmitted) capsule directories from the dandiset tree.

    A capsule is considered *queued* (prepared but not yet submitted) when its
    attempt directory has a ``code/`` subdirectory but neither a non-empty
    ``logs/`` subdirectory nor a ``derivatives/`` subdirectory, **and** the
    entry is not present in ``last_submitted.jsonl`` (which tracks recently
    submitted in-flight jobs).

    The function scans *dandiset_directory* for all attempt directories using
    the local filesystem as the ground truth, filters to the queued subset,
    then deletes each matching attempt directory tree from the DANDI archive
    (via ``dandi delete``) and from the local filesystem.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository.  The function scans
        ``{dandiset_directory}/derivatives/dandiset-*/`` to locate attempt
        directories.
    queue_directory : pathlib.Path
        Path to the queue root directory.  ``last_submitted.jsonl`` is read
        from here to exclude in-flight submissions from deletion.

    Returns
    -------
    list[pathlib.Path]
        List of attempt directory paths that were deleted.
    """
    if not os.environ.get("DANDI_API_KEY", "").strip():
        message = "`DANDI_API_KEY` environment variable is not set or is blank."
        raise RuntimeError(message)

    from ..dandiset import scan_dandiset_directory

    state_entries = scan_dandiset_directory(dandiset_directory=dandiset_directory)

    # Build the set of identities that have been recently submitted (in-flight).
    last_submitted_file = queue_directory / "last_submitted.jsonl"
    submitted_identities: set[tuple] = set()
    if last_submitted_file.exists():
        submitted_identities = {
            _entry_identity(json.loads(line.strip()))
            for line in last_submitted_file.read_text().splitlines()
            if line.strip()
        }

    # Filter to queued entries: has code, no logs, no output, not submitted.
    queued_entries = [
        e
        for e in state_entries
        if e.get("has_code")
        and not e.get("has_output")
        and not e.get("has_logs")
        and _entry_identity(e) not in submitted_identities
    ]

    removed: list[pathlib.Path] = []
    for entry in queued_entries:
        attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)

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


def _submit_next(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> bool:
    """
    Submit the next pending entry from ``waiting.jsonl``.

    Reads ``waiting.jsonl`` from the queue directory — this file is written by
    :func:`refresh_queue` and contains the priority-ordered list of pending
    entries produced by :func:`order_queue`.  If the file is absent
    or empty, :func:`refresh_queue` is called once to attempt to repopulate it
    from ``state.jsonl``.  If still empty after that, returns ``False``.

    The first entry from ``waiting.jsonl`` is submitted and then removed from
    ``waiting.jsonl``; the entry is simultaneously appended to
    ``last_submitted.jsonl``. The waiting queue is produced upstream by
    :func:`refresh_queue`; this function applies no additional
    submission gating beyond requiring the submit script to exist.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to
        locate prepared submission scripts.

    Returns
    -------
    bool
        True if a job was submitted, False if there are no pending entries or
        if the submit script cannot be found.
    """
    waiting_file = queue_directory / "waiting.jsonl"

    def _read_waiting() -> list[dict]:
        if not waiting_file.exists():
            return []
        return [json.loads(line.strip()) for line in waiting_file.read_text().splitlines() if line.strip()]

    waiting_entries = _read_waiting()

    if not waiting_entries:
        # Attempt to repopulate from dandiset scan before giving up.
        refresh_queue(queue_directory=queue_directory, dandiset_directory=dandiset_directory)
        waiting_entries = _read_waiting()

    if not waiting_entries:
        print(f"No pending entries in `{waiting_file}`")
        return False

    entry = waiting_entries[0]

    attempt_dir = _resolve_attempt_dir(base_dir=dandiset_directory, entry=entry)

    script_file_path = attempt_dir / "code" / "submit.sh"
    if not script_file_path.exists():
        message = f"Submit script not found: {script_file_path}"
        raise FileNotFoundError(message)

    print(f"Submitting run capsule directory: {attempt_dir}")
    submit_job(script_file_path=script_file_path)

    # Pop the submitted entry from waiting.jsonl.
    waiting_entries.pop(0)
    waiting_file.write_text("".join(json.dumps(e) + "\n" for e in waiting_entries))

    # Append to last_submitted.jsonl.
    last_submitted_file = queue_directory / "last_submitted.jsonl"
    with last_submitted_file.open("a") as f:
        f.write(json.dumps(entry) + "\n")

    return True


def order_queue(*, state_entries: list[dict], queue_config: dict, limit: int | None = None) -> list[dict]:
    """
    Build the priority-ordered waiting list from in-memory state entries.

    Parameters
    ----------
    state_entries : list[dict]
        Records produced by :func:`~dandi_compute_code.dandiset.scan_dandiset_directory`
        (or loaded from a ``state.jsonl`` file).
    queue_config : dict
        Parsed contents of ``queue_config.json``.
    limit : int, optional
        If provided, truncate output to the first *limit* entries.

    Returns
    -------
    list[dict]
        Ordered queue entries.
    """
    ordered = _build_processing_order(state_entries=state_entries, queue_config=queue_config)
    if limit is not None:
        ordered = ordered[:limit]
    return ordered


def refresh_queue(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
    """
    Scan *dandiset_directory*, regenerate ``state.jsonl``, and write ``waiting.jsonl``.

    Entries that are prepared (``has_code=True``) but not yet run
    (``has_logs=False``, ``has_output=False``) are ordered via
    :func:`order_queue` and written to ``waiting.jsonl`` so that subsequent
    calls to :func:`process_queue` can read them directly.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local dandiset clone used to rewrite ``state.jsonl``
        before ``waiting.jsonl`` is regenerated.

    Raises
    ------
    FileNotFoundError
        If ``queue_config.json`` is not found in *queue_directory*.
    """
    assert (
        "DANDI_API_KEY" in os.environ and os.environ["DANDI_API_KEY"]
    ), "`DANDI_API_KEY` environment variable must be set before refreshing queue state."

    state_file = queue_directory / "state.jsonl"
    records = scan_dandiset_directory(dandiset_directory=dandiset_directory)
    with state_file.open(mode="w") as file_stream:
        for record in records:
            file_stream.write(json.dumps(record) + "\n")

    state_entries = [json.loads(line.strip()) for line in state_file.read_text().splitlines() if line.strip()]
    queue_config = _load_queue_config(queue_directory=queue_directory)
    ordered = order_queue(state_entries=state_entries, queue_config=queue_config)
    waiting_file = queue_directory / "waiting.jsonl"
    waiting_file.write_text("".join(json.dumps(e) + "\n" for e in ordered))
    _prune_last_submitted(queue_directory=queue_directory, state_entries=state_entries)


def process_queue(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
    """
    Submit jobs from the priority-ordered ``waiting.jsonl`` up to two total
    running ``AIND-Ephys-Pipeline`` SLURM jobs.

    If ``waiting.jsonl`` is absent or empty, :func:`refresh_queue` is called
    first to populate it from ``state.jsonl``. Then ``squeue --me`` is checked
    for currently running ``AIND-Ephys-Pipeline`` jobs, and up to the
    difference from two jobs are submitted.

    To avoid duplicate submissions from overlapping invocations, a non-blocking
    advisory file lock is acquired on ``process_queue.lock`` in
    *queue_directory*. If the lock is already held, the invocation returns
    without submitting jobs.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to
        locate prepared submission scripts and to count failure directories
        for ``max_fail_per_dandiset`` enforcement.

    Raises
    ------
    FileNotFoundError
        If ``queue_config.json`` is not found in *queue_directory* (raised by
        :func:`refresh_queue`).
    """
    lock_file = queue_directory / "process_queue.lock"
    lock_file.touch(exist_ok=True)
    with lock_file.open("r+") as lock_file_stream:
        try:
            fcntl.flock(lock_file_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"Skipping queue processing: lock already held at `{lock_file}`")
            return

        waiting_file = queue_directory / "waiting.jsonl"
        if not waiting_file.exists() or not waiting_file.read_text().strip():
            refresh_queue(queue_directory=queue_directory, dandiset_directory=dandiset_directory)

        running_count = _count_running_aind_ephys_pipeline_jobs()
        for _ in range(max(0, 2 - running_count)):
            submitted = _submit_next(queue_directory=queue_directory, dandiset_directory=dandiset_directory)
            if not submitted:
                break


def _strip_commit_hash_suffix(version: str) -> str:
    """Strip a trailing +<git-hash> suffix from a version string when present."""
    version_parts = version.split("+")
    if len(version_parts) > 1 and re.fullmatch(r"[0-9a-f]{7,40}", version_parts[-1]):
        return "+".join(version_parts[:-1])
    return version


def prepare_queue(
    *,
    queue_directory: pathlib.Path,
    pipeline_directory: pathlib.Path | None = None,
    config_key: str = "default",
    content_ids: list[str] | None = None,
    limit: int | None = None,
) -> None:
    """
    En-masse preparation of qualifying assets based on the current queue config.

    For every pipeline/version/params combination declared in ``queue_config.json``
    this function determines which content IDs to prepare and calls
    :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job` for each
    asset — generating the ``code/`` directory and its parent directories without
    submitting a job.

    The per-pipeline failure cap (``max_fail_per_dandiset`` in ``queue_config.json``)
    is enforced by reading the existing ``state.jsonl`` file inside
    *queue_directory*.  Entries with ``has_code=True``, ``has_logs=True``, and
    ``has_output=False`` are counted as failures for the relevant pipeline,
    version, and source Dandiset.  Run :func:`refresh_queue` beforehand to
    ensure ``state.jsonl`` is up to date.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    pipeline_directory : pathlib.Path, optional
        Local path to the AIND pipeline repository.  Passed directly to
        :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job`.
    config_key : str
        Key for a registered job configuration. Passed directly to
        :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job`.
    content_ids : list of str, optional
        Explicit list of content IDs to prepare.  When provided, the qualifying
        content IDs list is not fetched from the network and these IDs are used
        directly instead.  Useful for targeted runs such as testing with one or
        more known content IDs.
    limit : int, optional
        If provided, stop after preparing *limit* assets in total (across all
        pipeline/version/params combinations).  Useful for testing.
    """
    queue_config = _load_queue_config(queue_directory=queue_directory)

    if content_ids is None:
        qualifying_aind_content_ids_url = (
            "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/"
            "derivatives/qualifying_aind_content_ids.min.json.gz"
        )
        with urllib.request.urlopen(url=qualifying_aind_content_ids_url) as response:
            content_ids = json.loads(gzip.decompress(response.read()))

    state_file = queue_directory / "state.jsonl"
    state_entries = (
        [json.loads(line.strip()) for line in state_file.read_text().splitlines() if line.strip()]
        if state_file.exists()
        else []
    )

    # Build from all known state entries. A content ID is expected to map to a
    # stable source dandiset in normal operation; ambiguous mappings are handled
    # conservatively below by skipping max-failure enforcement for that asset.
    content_id_to_dandiset_ids: dict[str, set[str]] = {}
    for entry in state_entries:
        content_id = entry.get("content_id")
        dandiset_id = entry.get("dandiset_id")
        if content_id and dandiset_id:
            content_id_to_dandiset_ids.setdefault(content_id, set()).add(dandiset_id)
    failure_entries = [
        entry
        for entry in state_entries
        if entry.get("has_code") and entry.get("has_logs") and not entry.get("has_output")
    ]

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
                failure_count_by_dandiset: defaultdict[str, int] = defaultdict(int)
                if max_fail is not None:
                    for entry in failure_entries:
                        if entry.get("pipeline") != pipeline_name or not _version_matches(
                            entry.get("version", ""), version
                        ):
                            continue
                        dandiset_id = entry.get("dandiset_id")
                        if not dandiset_id:
                            continue
                        failure_count_by_dandiset[dandiset_id] += 1
                # Strip the trailing commit-hash suffix before passing to prepare_aind_ephys_job.
                submission_version = _strip_commit_hash_suffix(version)

                for content_id in sorted(content_ids):
                    if limit is not None and prepared_count >= limit:
                        break
                    if max_fail is not None:
                        dandiset_ids = content_id_to_dandiset_ids.get(content_id, set())
                        if len(dandiset_ids) == 1:
                            dandiset_id = next(iter(dandiset_ids))
                            failure_count = failure_count_by_dandiset.get(dandiset_id, 0)
                            if failure_count >= max_fail:
                                print(
                                    f"Skipping preparation for {pipeline_name}/{version}/{params}/{content_id}: "
                                    f"failure count ({failure_count}) for dandiset-{dandiset_id} has reached "
                                    f"max_fail_per_dandiset ({max_fail})."
                                )
                                continue
                        else:
                            mapped_dandisets = ", ".join(sorted(dandiset_ids)) if dandiset_ids else "<none>"
                            print(
                                f"Preparing {content_id} without max_fail_per_dandiset enforcement for "
                                f"{pipeline_name}/{version}/{params}: expected exactly 1 mapped dandiset but "
                                f"found {len(dandiset_ids)} ({mapped_dandisets})."
                            )

                    print(f"Preparing content ID: {content_id}")
                    prepare_aind_ephys_job(
                        content_id=content_id,
                        parameters_key=params,
                        pipeline_version=submission_version,
                        pipeline_directory=pipeline_directory,
                        config_key=config_key,
                        silent=True,
                    )
                    prepared_count += 1
