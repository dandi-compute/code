import collections
import gzip
import json
import pathlib
import re
import subprocess
import urllib.request


def _count_dandiset_failures(
    *,
    dandiset_directory: pathlib.Path,
    version: str,
) -> int:
    """
    Count failure attempt directories across all source dandisets for a given version.

    Scans every ``derivatives/dandiset-*`` sub-directory inside *dandiset_directory*
    (i.e. the local clone of the 001697 dandiset repository) and counts attempt
    directories that contain a ``code/`` subdirectory but **no** ``output/``
    subdirectory — the signature of a failed run.

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
            if (attempt_dir / "code").is_dir() and not (attempt_dir / "output").is_dir():
                failure_count += 1

    return failure_count


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


def _fill_waiting(
    *,
    cwd: pathlib.Path,
    pipeline: str,
    version: str,
    params: str,
    dandiset_directory: pathlib.Path,
) -> None:
    """
    Fill the waiting queue with new entries for a given pipeline/version/params combination.

    If there are already waiting entries for this combination, this function will return early
    without adding new entries.  Otherwise, it fetches qualifying AIND content IDs from the
    remote cache and adds those that have not yet exceeded their max attempt counts.

    If ``max_fail_per_dandiset`` is set for the pipeline and the total number of failed
    attempt directories across all source dandisets for this version has already reached
    that limit, no new entries will be added.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory (must be named 'queue').
    pipeline : str
        Pipeline name as it appears in ``queue_config.json`` under ``pipelines``.
    version : str
        Version string as it appears in ``version_priority``.
    params : str
        Params string as it appears in ``params_priority``.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to count
        failure directories for the given version.
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

    queue_config = json.loads((cwd / "queue_config.json").read_text())
    pipeline_cfg = queue_config["pipelines"][pipeline]

    # Skip filling if the total failure count across all dandisets has reached the limit.
    max_fail = pipeline_cfg.get("max_fail_per_dandiset")
    if max_fail is not None:
        failure_count = _count_dandiset_failures(
            dandiset_directory=dandiset_directory,
            version=version,
        )
        if failure_count >= max_fail:
            print(
                f"Skipping fill for {pipeline}/{version}/{params}: "
                f"failure count ({failure_count}) has reached max_fail_per_dandiset ({max_fail})."
            )
            return

    submitted_file = cwd / "submitted.jsonl"
    done_counter = _fetch_counts(
        file_path=submitted_file,
        pipeline=pipeline,
        version=version,
        params=params,
    )

    qualifying_aind_content_ids_url = (
        "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/"
        "derivatives/qualifying_aind_content_ids.min.json.gz"
    )
    with urllib.request.urlopen(url=qualifying_aind_content_ids_url) as response:
        qualifying_aind_content_ids = json.loads(gzip.decompress(response.read()))

    content_id_to_unique_dandiset_path_url = (
        "https://raw.githubusercontent.com/dandi-cache/content-id-to-unique-dandiset-path/refs/heads/min/"
        "derivatives/content_id_to_unique_dandiset_path.min.json.gz"
    )
    with urllib.request.urlopen(url=content_id_to_unique_dandiset_path_url) as response:
        content_id_to_unique_dandiset_path = json.loads(gzip.decompress(response.read()))

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
            dandiset_info = content_id_to_unique_dandiset_path.get(content_id, dict())
            dandiset_id, dandiset_path = next(iter(dandiset_info.items()), (None, None))
            file_stream.write(
                json.dumps(
                    {
                        "pipeline": pipeline,
                        "version": version,
                        "params": params,
                        "content_id": content_id,
                        "dandiset_id": dandiset_id,
                        "dandiset_path": dandiset_path,
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


def _submit_next(*, cwd: pathlib.Path, dandiset_directory: pathlib.Path) -> bool:
    """
    Pop the next valid entry from ``waiting.jsonl`` and submit it.

    An entry is considered invalid and skipped if it has already reached its
    maximum allowed attempt count (as defined in ``queue_config.json``), or if the
    total number of failures in *dandiset_directory* for the entry's version has
    reached ``max_fail_per_dandiset`` (when that field is set in the pipeline config).

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory (must be named 'queue').
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Failure
        directories are counted across all source dandisets and entries are
        skipped when the total reaches ``max_fail_per_dandiset``.

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

        # Skip if the total failure count across all dandisets has reached the limit.
        max_fail = pipeline_cfg.get("max_fail_per_dandiset")
        if max_fail is not None:
            failure_count = _count_dandiset_failures(
                dandiset_directory=dandiset_directory,
                version=version,
            )
            if failure_count >= max_fail:
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


def process_queue(*, cwd: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
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
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Failure
        directories are counted across all source dandisets and entries are
        skipped when the total reaches ``max_fail_per_dandiset``.

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
                    dandiset_directory=dandiset_directory,
                )

    any_running = _determine_running()
    if not any_running:
        _submit_next(cwd=cwd, dandiset_directory=dandiset_directory)
