import collections
import gzip
import hashlib
import json
import pathlib
import re
import subprocess
import urllib.request

_ATTEMPT_DIR_PATTERN = re.compile(r"^(.+)_attempt-\d+$")

_AIND_EPHYS_PIPELINE_DIR = pathlib.Path(__file__).parent.parent / "aind_ephys_pipeline"


def _fetch_counts(
    *,
    file_path: pathlib.Path,
    pipeline: str,
    version: str,
    params: str,
) -> collections.Counter:
    """
    Count how many times each content_id has been submitted for a given pipeline/version/params combination.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the JSONL file (e.g., submitted.jsonl).
    pipeline : str
        Pipeline name (e.g., 'aind+ephys').
    version : str
        Version string (e.g., 'v1.0.0+fixes+47bd492').
    params : str
        Params string (e.g., 'default').

    Returns
    -------
    collections.Counter
        A Counter mapping content_id to its submission count.
    """
    content_ids = []
    for line in file_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        entry = json.loads(stripped)
        if entry.get("pipeline") != pipeline or entry.get("version") != version or entry.get("params") != params:
            continue
        content_id = entry.get("content_id", "")
        if content_id:
            content_ids.append(content_id)
    return collections.Counter(content_ids)


def _count_dandiset_failures(dandiset_dir: pathlib.Path) -> collections.Counter:
    """
    Count failure attempts in a local clone of the output Dandiset.

    A failure is defined as a directory ending in ``attempt-{N}`` that has at least
    a ``code`` subdirectory but no ``output`` subdirectory.

    Failures are counted separately per unique ``(version_dir_name, params_config_prefix)``
    combination, where ``version_dir_name`` is the name of a ``version-*`` ancestor
    directory and ``params_config_prefix`` is the part of the attempt directory name
    before ``_attempt-{N}`` (e.g. ``params-abc1234_config-def5678``).

    Parameters
    ----------
    dandiset_dir : pathlib.Path
        Path to the root of the local clone of the output Dandiset (e.g. a clone
        of https://github.com/dandi-compute/001697).

    Returns
    -------
    collections.Counter
        A Counter mapping ``(version_dir_name, params_config_prefix)`` tuples to
        the number of failures for that combination.
    """
    failure_counter: collections.Counter = collections.Counter()
    for path in dandiset_dir.rglob("*"):
        if not path.is_dir():
            continue
        match = _ATTEMPT_DIR_PATTERN.match(path.name)
        if not match:
            continue
        # Failure condition: has code/ subdirectory but no output/ subdirectory
        if not (path / "code").is_dir():
            continue
        if (path / "output").is_dir():
            continue
        # Find the nearest version-* ancestor directory
        version_dir_name = None
        for parent in path.parents:
            if parent.name.startswith("version-"):
                version_dir_name = parent.name
                break
        if version_dir_name is None:
            continue
        params_config_prefix = match.group(1)
        failure_counter[(version_dir_name, params_config_prefix)] += 1
    return failure_counter


def _fill_waiting(*, cwd: pathlib.Path, pipeline: str, version: str, params: str) -> None:
    """
    Fill the waiting queue with new entries for a given pipeline/version/params combination.

    If there are already waiting entries for this combination, this function will return early
    without adding new entries.  Otherwise, it fetches qualifying AIND content IDs from the
    remote cache and adds those that have not yet exceeded their max attempt counts.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory (must be named 'queue').
    pipeline : str
        Pipeline name as it appears in ``queue_config.json`` under ``pipelines``
        (e.g., 'aind+ephys').
    version : str
        Version string as it appears in ``version_priority``
        (e.g., 'v1.0.0+fixes+47bd492').
    params : str
        Params string as it appears in ``params_priority`` (e.g., 'default').
    """
    waiting_file = cwd / "waiting.jsonl"

    previous_waiting = [
        entry
        for line in waiting_file.read_text().splitlines()
        if line.strip()
        for entry in [json.loads(line.strip())]
        if entry.get("pipeline") == pipeline and entry.get("version") == version and entry.get("params") == params
    ]
    if previous_waiting:
        print(
            f"Queue already has entries for {pipeline}/{version}/{params}!"
            " Waiting until all entries have run before re-filling."
        )
        return

    submitted_file = cwd / "submitted.jsonl"
    done_counter = _fetch_counts(
        file_path=submitted_file,
        pipeline=pipeline,
        version=version,
        params=params,
    )

    url = (
        "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/"
        "derivatives/qualifying_aind_content_ids.min.json.gz"
    )
    with urllib.request.urlopen(url=url) as response:
        qualifying_aind_content_ids = json.loads(gzip.decompress(response.read()))

    queue_config = json.loads((cwd / "queue_config.json").read_text())
    pipeline_cfg = queue_config["pipelines"][pipeline]

    global_max_attempts = pipeline_cfg["max_attempts_per_asset"]
    asset_overrides = pipeline_cfg.get("asset_overrides") or {}

    new_waiting = set()
    for content_id in qualifying_aind_content_ids:
        if (asset_override := asset_overrides.get(content_id, global_max_attempts)) is not None and done_counter.get(
            content_id, 0
        ) >= asset_override:
            continue

        new_waiting.add(content_id)

    with waiting_file.open(mode="a") as file_stream:
        for content_id in sorted(new_waiting):
            file_stream.write(
                json.dumps(
                    {
                        "pipeline": pipeline,
                        "version": version,
                        "params": params,
                        "content_id": content_id,
                    }
                )
                + "\n"
            )


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


def _compute_params_id(params: str) -> str | None:
    """
    Compute the 7-character MD5-based params ID for a registered params key.

    Parameters
    ----------
    params : str
        The params key as it appears in ``registered_params.json`` and in the queue
        (e.g. ``'default'`` or ``'no+motion'``).

    Returns
    -------
    str or None
        The first 7 hex characters of the MD5 digest of the params file, or ``None``
        if the key is not found in the registry.
    """
    params_registry_path = _AIND_EPHYS_PIPELINE_DIR / "registries" / "registered_params.json"
    params_registry = json.loads(params_registry_path.read_text())
    if params not in params_registry:
        return None
    params_file = _AIND_EPHYS_PIPELINE_DIR / "params" / params_registry[params]["path"]
    return hashlib.md5(params_file.read_bytes()).hexdigest()[0:7]


def _compute_default_config_id() -> str:
    """
    Compute the 7-character MD5-based config ID for the default configuration file.

    Returns
    -------
    str
        The first 7 hex characters of the MD5 digest of the default config file.
    """
    default_config_path = _AIND_EPHYS_PIPELINE_DIR / "configs" / "mit_engaging.config"
    return hashlib.md5(default_config_path.read_bytes()).hexdigest()[0:7]


def _submit_next(*, cwd: pathlib.Path, dandiset_dir: pathlib.Path | None = None) -> bool:
    """
    Pop the next valid entry from ``waiting.jsonl`` and submit it.

    An entry is considered invalid and skipped if it has already reached its
    maximum allowed attempt count (as defined in ``queue_config.json``) or if
    the number of failures recorded in the output Dandiset clone for its
    ``(version, params, config)`` combination meets or exceeds
    ``max_fail_per_dandiset``.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory (must be named 'queue').
    dandiset_dir : pathlib.Path, optional
        Path to a local clone of the output Dandiset repository (e.g. a clone
        of https://github.com/dandi-compute/001697).  When provided, each
        entry is also checked against ``max_fail_per_dandiset`` in
        ``queue_config.json`` before submission.

    Returns
    -------
    bool
        True if a job was submitted, False if the waiting queue is empty.
    """
    waiting_file = cwd / "waiting.jsonl"
    submitted_file = cwd / "submitted.jsonl"

    lines = waiting_file.read_text().splitlines()
    if not lines:
        print(f"No more entries in `{waiting_file}`")
        waiting_file.write_text(data="")
        return False

    queue_config = json.loads((cwd / "queue_config.json").read_text())

    failure_counter = _count_dandiset_failures(dandiset_dir) if dandiset_dir is not None else None

    entry = None
    while lines:
        line = lines.pop(0)
        stripped = line.strip()
        if not stripped:
            continue

        entry_obj = json.loads(stripped)
        pipeline = entry_obj.get("pipeline", "")
        version = entry_obj.get("version", "")
        params = entry_obj.get("params", "")
        content_id = entry_obj.get("content_id", "")
        if not all([pipeline, version, params, content_id]):
            continue

        pipeline_cfg = queue_config["pipelines"][pipeline]
        global_max_attempts = pipeline_cfg["max_attempts_per_asset"]
        asset_overrides = pipeline_cfg.get("asset_overrides") or {}

        submitted_counter = _fetch_counts(
            file_path=submitted_file,
            pipeline=pipeline,
            version=version,
            params=params,
        )

        if (
            asset_override := asset_overrides.get(content_id, global_max_attempts)
        ) is not None and submitted_counter.get(content_id, 0) >= asset_override:
            continue

        if failure_counter is not None:
            max_fail_per_dandiset = pipeline_cfg.get("max_fail_per_dandiset")
            if max_fail_per_dandiset is not None:
                params_id = _compute_params_id(params)
                config_id = _compute_default_config_id()
                if params_id is not None and config_id is not None:
                    version_dir_name = f"version-{version}"
                    params_config_prefix = f"params-{params_id}_config-{config_id}"
                    if failure_counter.get((version_dir_name, params_config_prefix), 0) >= max_fail_per_dandiset:
                        print(
                            f"Skipping content ID {content_id}: "
                            f"{failure_counter[(version_dir_name, params_config_prefix)]} failures "
                            f"already recorded for {version_dir_name}/{params_config_prefix} "
                            f"(max_fail_per_dandiset={max_fail_per_dandiset})."
                        )
                        continue

        entry = (pipeline, version, params, content_id)
        break

    if entry is None:
        print(f"No more entries in `{waiting_file}`")
        waiting_file.write_text(data="")
        return False

    pipeline, version, params, content_id = entry

    submission_version = "+".join(version.split("+")[:-1])
    submission_params = params

    print(f"Submitting content ID: {content_id}")
    command = [
        "dandicompute",
        "aind",
        "prepare",
        "--id",
        content_id,
        "--version",
        submission_version,
        "--params",
        submission_params,
        "--submit",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)
    waiting_file.write_text(data="\n".join(lines) + ("\n" if lines else ""))
    with submitted_file.open(mode="a") as file_stream:
        file_stream.write(
            json.dumps(
                {
                    "pipeline": pipeline,
                    "version": version,
                    "params": params,
                    "content_id": content_id,
                }
            )
            + "\n"
        )
    return True


def process_queue(*, cwd: pathlib.Path, dandiset_dir: pathlib.Path | None = None) -> None:
    """
    Process the current state of the queue.

    The queue is a single flat ``waiting.jsonl`` at the root of the queue directory.
    Each line is a JSON object with fields: pipeline, version, params, and content_id.

    If there are no waiting entries for a pipeline/version/params combination, it will be
    re-filled in accordance with ``queue_config.json`` and the current state of the
    qualifying AIND cache.  The fill order follows the ``version_priority`` and
    ``params_priority`` lists defined per pipeline in ``queue_config.json``.

    If there are no currently running jobs, the next entry in ``waiting.jsonl`` will be
    popped and submitted according to the logic in ``submit_job.py``.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory.  The directory must be named ``'queue'``.
    dandiset_dir : pathlib.Path, optional
        Path to a local clone of the output Dandiset repository (e.g. a clone of
        https://github.com/dandi-compute/001697).  When provided, job submissions are
        additionally gated by ``max_fail_per_dandiset`` defined per pipeline in
        ``queue_config.json``.

    Raises
    ------
    ValueError
        If the current working directory is not named ``'queue'``.
    """
    if cwd.name != "queue":
        message = f"Current working directory must be 'queue', but is '{cwd.name}'"
        raise ValueError(message)

    waiting_file = cwd / "waiting.jsonl"
    submitted_file = cwd / "submitted.jsonl"
    if not waiting_file.exists():
        waiting_file.write_text("")
    if not submitted_file.exists():
        submitted_file.write_text("")

    queue_config = json.loads((cwd / "queue_config.json").read_text())

    for pipeline_name, pipeline_data in queue_config.get("pipelines", {}).items():
        for version in pipeline_data.get("version_priority", []):
            for params in pipeline_data.get("params_priority", []):
                _fill_waiting(
                    cwd=cwd,
                    pipeline=pipeline_name,
                    version=version,
                    params=params,
                )

    any_running = _determine_running()
    if not any_running:
        _submit_next(cwd=cwd, dandiset_dir=dandiset_dir)
