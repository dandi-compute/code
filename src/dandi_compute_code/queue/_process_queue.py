import gzip
import json
import os
import pathlib
import re
import shutil
import subprocess
import urllib.request

from dandi_compute_code.aind_ephys_pipeline import prepare_aind_ephys_job

_AIND_EPHYS_PARAMS_REGISTRY_PATH = (
    pathlib.Path(__file__).parent.parent / "aind_ephys_pipeline" / "registries" / "registered_params.json"
)
_TEST_QUEUE_CONTENT_ID = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
_TEST_QUEUE_PIPELINE = "aind+ephys"

try:
    _AIND_EPHYS_PARAMS_REGISTRY: dict = json.loads(_AIND_EPHYS_PARAMS_REGISTRY_PATH.read_text())
except (OSError, json.JSONDecodeError):
    _AIND_EPHYS_PARAMS_REGISTRY = {}


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
    (identified by ``dandiset_id``, ``subject``, ``session``, and ``config``)
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

            # Group by dandiset instance: (dandiset_id, subject, session, config)
            instance_groups: dict[tuple, list[dict]] = {}
            for entry in version_entries:
                key = (
                    entry.get("dandiset_id", ""),
                    entry.get("subject", ""),
                    entry.get("session") or "",
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
    and its immediate parent is named ``version-{version}``.

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
            if attempt_dir.parent.name != version_dir_name:
                continue
            logs_dir = attempt_dir / "logs"
            has_logs = logs_dir.is_dir() and any(logs_dir.iterdir())
            if (attempt_dir / "code").is_dir() and has_logs and not (attempt_dir / "derivatives").is_dir():
                failure_count += 1

    return failure_count


def _determine_running() -> bool:
    """
    Check whether any AIND jobs are currently running via the SLURM scheduler.

    Calls ``squeue --format=%j`` and looks for any job names containing 'AIND'.

    Returns
    -------
    bool
        True if at least one AIND job is currently running, False otherwise.
    """
    command = ["squeue", "--me", "--format=%j"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)
    if result.stderr:
        print(result.stderr)
    for line in result.stdout.splitlines():
        if "AIND" in line:
            return True
    return False


def _entry_identity(entry: dict) -> tuple:
    """Return a stable tuple key for matching queue/state/last-submitted entries."""
    return (
        entry.get("dandiset_id"),
        entry.get("subject"),
        entry.get("session"),
        entry.get("pipeline"),
        entry.get("version"),
        entry.get("params"),
        entry.get("config"),
        entry.get("attempt"),
    )


def _prune_last_submitted(*, cwd: pathlib.Path, state_entries: list[dict]) -> None:
    """Remove last_submitted entries that now have logs or output in state."""
    last_submitted_file = cwd / "last_submitted.jsonl"
    if not last_submitted_file.exists():
        return

    entries_to_prune = {_entry_identity(e) for e in state_entries if e.get("has_output") or e.get("has_logs")}

    last_submitted_entries = [
        json.loads(line.strip()) for line in last_submitted_file.read_text().splitlines() if line.strip()
    ]
    filtered = [entry for entry in last_submitted_entries if _entry_identity(entry) not in entries_to_prune]
    last_submitted_file.write_text("".join(json.dumps(entry) + "\n" for entry in filtered))


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
        dandiset_id = entry["dandiset_id"]
        subject = entry["subject"]
        session = entry.get("session")
        pipeline = entry["pipeline"]
        version = entry["version"]
        params = entry["params"]
        config = entry["config"]
        attempt = entry["attempt"]

        attempt_dir = dandiset_directory / "derivatives" / f"dandiset-{dandiset_id}" / f"sub-{subject}"
        if session:
            attempt_dir = attempt_dir / f"ses-{session}"
        attempt_dir = (
            attempt_dir
            / f"pipeline-{pipeline}"
            / f"version-{version}"
            / f"params-{params}_config-{config}_attempt-{attempt}"
        )

        if attempt_dir.is_dir():
            subprocess.run(
                ["dandi", "delete", str(attempt_dir)],
                input=b"y\n",
                check=True,
            )
            shutil.rmtree(attempt_dir)
            removed.append(attempt_dir)

    return removed


def _submit_next(*, cwd: pathlib.Path, dandiset_directory: pathlib.Path) -> bool:
    """
    Submit the next pending entry from ``waiting.jsonl``.

    Reads ``waiting.jsonl`` from the queue directory — this file is written by
    :func:`refresh_waiting_queue` and contains the priority-ordered list of pending
    entries produced by :func:`order_queue`.  If the file is absent
    or empty, :func:`refresh_waiting_queue` is called once to attempt to repopulate it
    from ``state.jsonl``.  If still empty after that, returns ``False``.

    The first entry from ``waiting.jsonl`` is submitted and then removed from
    ``waiting.jsonl``; the entry is simultaneously appended to
    ``last_submitted.jsonl``. The waiting queue is produced upstream by
    :func:`refresh_waiting_queue`; this function applies no additional
    submission gating beyond requiring the submit script to exist.

    Parameters
    ----------
    cwd : pathlib.Path
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
    waiting_file = cwd / "waiting.jsonl"

    def _read_waiting() -> list[dict]:
        if not waiting_file.exists():
            return []
        return [json.loads(line.strip()) for line in waiting_file.read_text().splitlines() if line.strip()]

    waiting_entries = _read_waiting()

    if not waiting_entries:
        # Attempt to repopulate from state.jsonl before giving up.
        # Use a small limit to avoid building a runaway queue.
        refresh_waiting_queue(cwd=cwd, limit=3)
        waiting_entries = _read_waiting()

    if not waiting_entries:
        print(f"No pending entries in `{waiting_file}`")
        return False

    entry = waiting_entries[0]

    dandiset_id = entry["dandiset_id"]
    subject = entry["subject"]
    session = entry.get("session")
    pipeline = entry["pipeline"]
    version = entry["version"]
    params = entry["params"]
    config = entry["config"]
    attempt = entry["attempt"]

    attempt_dir = dandiset_directory / "derivatives" / f"dandiset-{dandiset_id}" / f"sub-{subject}"
    if session:
        attempt_dir = attempt_dir / f"ses-{session}"
    attempt_dir = (
        attempt_dir
        / f"pipeline-{pipeline}"
        / f"version-{version}"
        / f"params-{params}_config-{config}_attempt-{attempt}"
    )

    script_file_path = attempt_dir / "code" / "submit.sh"
    if not script_file_path.exists():
        print(f"Submit script not found: {script_file_path}")
        return False

    print(f"Submitting: {attempt_dir.name}")
    command = ["dandicompute", "aind", "submit", "--script", str(script_file_path)]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)

    # Pop the submitted entry from waiting.jsonl.
    waiting_entries.pop(0)
    waiting_file.write_text("".join(json.dumps(e) + "\n" for e in waiting_entries))

    # Append to last_submitted.jsonl.
    last_submitted_file = cwd / "last_submitted.jsonl"
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


def refresh_waiting_queue(*, cwd: pathlib.Path, limit: int | None = None) -> None:
    """
    Build and write ``waiting.jsonl`` from ``state.jsonl`` in the queue directory.

    Reads ``state.jsonl`` (produced by ``dandicompute dandiset scan``) to find
    entries that are prepared (``has_code=True``) but not yet run
    (``has_logs=False``, ``has_output=False``).  The entries are ordered
    via :func:`order_queue` and written to ``waiting.jsonl`` so
    that subsequent calls to :func:`process_queue` can read them directly.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory.
    limit : int, optional
        If provided, truncate ``waiting.jsonl`` to the first *limit* entries.
        Useful for testing without submitting the full queue.

    Raises
    ------
    FileNotFoundError
        If ``state.jsonl`` is not found in *cwd*.
    """
    state_file = cwd / "state.jsonl"
    if not state_file.exists():
        message = (
            f"'state.jsonl' not found in '{cwd}'. "
            "Generate it with: dandicompute dandiset scan --directory <dandiset_dir> --output <queue_dir>/state.jsonl"
        )
        raise FileNotFoundError(message)

    state_entries = [json.loads(line.strip()) for line in state_file.read_text().splitlines() if line.strip()]
    queue_config = json.loads((cwd / "queue_config.json").read_text())
    ordered = order_queue(state_entries=state_entries, queue_config=queue_config, limit=limit)
    waiting_file = cwd / "waiting.jsonl"
    waiting_file.write_text("".join(json.dumps(e) + "\n" for e in ordered))
    _prune_last_submitted(cwd=cwd, state_entries=state_entries)


def process_queue(*, cwd: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
    """
    Submit the next job from the priority-ordered ``waiting.jsonl``.

    If ``waiting.jsonl`` is absent or empty, :func:`refresh_waiting_queue` is called
    first to populate it from ``state.jsonl``.  Then, if no AIND jobs are
    currently running via SLURM, the next valid entry is submitted.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to
        locate prepared submission scripts and to count failure directories
        for ``max_fail_per_dandiset`` enforcement.

    Raises
    ------
    FileNotFoundError
        If ``waiting.jsonl`` is absent or empty and ``state.jsonl`` is not
        found in *cwd* (raised by :func:`refresh_waiting_queue`).
    """
    waiting_file = cwd / "waiting.jsonl"
    if not waiting_file.exists() or not waiting_file.read_text().strip():
        refresh_waiting_queue(cwd=cwd)

    any_running = _determine_running()
    if not any_running:
        _submit_next(cwd=cwd, dandiset_directory=dandiset_directory)


def _strip_commit_hash_suffix(version: str) -> str:
    version_parts = version.split("+")
    if len(version_parts) > 1 and re.fullmatch(r"[0-9a-f]{7,40}", version_parts[-1]):
        return "+".join(version_parts[:-1])
    return version


def prepare_test_queue(
    *,
    cwd: pathlib.Path,
    pipeline_directory: pathlib.Path | None = None,
    config_key: str = "default",
) -> None:
    """
    Prepare a new test run for each configured aind+ephys version/params pair.

    Reads ``queue_config.json`` from *cwd* and, for every combination of
    ``version_priority`` and ``params_priority`` in the ``aind+ephys`` pipeline
    configuration, calls
    :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job` for
    the canonical test content ID.  The preparation helper determines the next
    available attempt number, effectively bumping the attempt counter each run.
    """
    queue_config = json.loads((cwd / "queue_config.json").read_text())
    pipeline_cfg = queue_config.get("pipelines", {}).get(_TEST_QUEUE_PIPELINE)
    if pipeline_cfg is None:
        message = f"Pipeline {_TEST_QUEUE_PIPELINE!r} not found in '{cwd / 'queue_config.json'}'."
        raise ValueError(message)

    for version in pipeline_cfg.get("version_priority", []):
        submission_version = _strip_commit_hash_suffix(version)
        for params in pipeline_cfg.get("params_priority", []):
            print(f"Preparing test queue entry for {_TEST_QUEUE_PIPELINE}/{submission_version}/{params}")
            prepare_aind_ephys_job(
                content_id=_TEST_QUEUE_CONTENT_ID,
                parameters_key=params,
                pipeline_version=submission_version,
                pipeline_directory=pipeline_directory,
                config_key=config_key,
                silent=True,
            )


def prepare_queue(
    *,
    cwd: pathlib.Path,
    dandiset_directory: pathlib.Path,
    pipeline_directory: pathlib.Path | None = None,
    config_key: str = "default",
    limit: int | None = None,
) -> None:
    """
    En-masse preparation of all qualifying assets based on the current queue config.

    For every pipeline/version/params combination declared in ``queue_config.json``
    this function fetches the qualifying AIND content IDs and calls
    :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job` for each
    asset — generating the ``code/`` directory and its parent directories without
    submitting a job.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Failure
        directories are counted across all source dandisets and entries are
        skipped when the total reaches ``max_fail_per_dandiset``.
    pipeline_directory : pathlib.Path, optional
        Local path to the AIND pipeline repository.  Passed directly to
        :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job`.
    config_key : str
        Key for a registered job configuration. Passed directly to
        :func:`~dandi_compute_code.aind_ephys_pipeline.prepare_aind_ephys_job`.
    limit : int, optional
        If provided, stop after preparing *limit* assets in total (across all
        pipeline/version/params combinations).  Useful for testing.
    """
    queue_config = json.loads((cwd / "queue_config.json").read_text())

    qualifying_aind_content_ids_url = (
        "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/"
        "derivatives/qualifying_aind_content_ids.min.json.gz"
    )
    with urllib.request.urlopen(url=qualifying_aind_content_ids_url) as response:
        qualifying_aind_content_ids = json.loads(gzip.decompress(response.read()))

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

                # Respect the per-dandiset failure cap.
                max_fail = pipeline_cfg.get("max_fail_per_dandiset")
                if max_fail is not None:
                    failure_count = _count_dandiset_failures(
                        dandiset_directory=dandiset_directory,
                        version=version,
                    )
                    if failure_count >= max_fail:
                        print(
                            f"Skipping preparation for {pipeline_name}/{version}/{params}: "
                            f"failure count ({failure_count}) has reached max_fail_per_dandiset ({max_fail})."
                        )
                        continue

                # Strip the trailing commit-hash suffix before passing to prepare_aind_ephys_job.
                submission_version = _strip_commit_hash_suffix(version)

                for content_id in sorted(qualifying_aind_content_ids):
                    if limit is not None and prepared_count >= limit:
                        break

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
