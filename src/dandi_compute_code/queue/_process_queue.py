import collections
import gzip
import json
import pathlib
import subprocess
import urllib.request


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
        Pipeline name (without 'pipeline-' prefix).
    version : str
        Version string (without 'version-' prefix).
    params : str
        Params string (without 'params-' prefix).

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


def _fill_waiting(*, cwd: pathlib.Path, pipeline_name: str, pipeline_version: str, params: str) -> None:
    """
    Fill the waiting queue with new entries for a given pipeline/version/params combination.

    If there are already waiting entries for this combination, this function will return early
    without adding new entries.  Otherwise, it fetches qualifying AIND content IDs from the
    remote cache and adds those that have not yet exceeded their max attempt counts.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory (must be named 'queue').
    pipeline_name : str
        Pipeline key as it appears in ``queue_config.json`` (e.g., 'pipeline-aind+ephys').
    pipeline_version : str
        Version key as it appears in ``queue_config.json`` (e.g., 'version-v1.0.0+fixes+47bd492').
    params : str
        Params key as it appears in ``queue_config.json`` (e.g., 'params-default').
    """
    waiting_file = cwd / "waiting.jsonl"

    pipeline = pipeline_name.removeprefix("pipeline-")
    version = pipeline_version.removeprefix("version-")
    params_value = params.removeprefix("params-")

    previous_waiting = [
        entry
        for line in waiting_file.read_text().splitlines()
        if line.strip()
        for entry in [json.loads(line.strip())]
        if entry.get("pipeline") == pipeline and entry.get("version") == version and entry.get("params") == params_value
    ]
    if previous_waiting:
        print(
            f"Queue already has entries for {pipeline_name}/{pipeline_version}/{params}!"
            " Waiting until all entries have run before re-filling."
        )
        return

    submitted_file = cwd / "submitted.jsonl"
    done_counter = _fetch_counts(
        file_path=submitted_file,
        pipeline=pipeline,
        version=version,
        params=params_value,
    )

    url = (
        "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/"
        "derivatives/qualifying_aind_content_ids.min.json.gz"
    )
    with urllib.request.urlopen(url=url) as response:
        qualifying_aind_content_ids = json.loads(gzip.decompress(response.read()))

    queue_config = json.loads((cwd / "queue_config.json").read_text())
    params_cfg = queue_config[pipeline_name]["versions"][pipeline_version]["params"][params]

    global_max_attempts = params_cfg["max_attempts_per_asset"]
    asset_overrides = params_cfg["asset_overrides"]

    new_waiting = set()
    for content_id in qualifying_aind_content_ids:
        if done_counter.get(content_id, 0) >= asset_overrides.get(content_id, global_max_attempts):
            continue

        new_waiting.add(content_id)

    with waiting_file.open(mode="a") as file_stream:
        for content_id in sorted(new_waiting):
            file_stream.write(
                json.dumps(
                    {
                        "pipeline": pipeline,
                        "version": version,
                        "params": params_value,
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
    result = subprocess.run(
        ["squeue", "--format=%j"],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in result.stdout.splitlines():
        if "AIND" in line:
            return True
    return False


def _submit_next(*, cwd: pathlib.Path) -> bool:
    """
    Pop the next valid entry from ``waiting.jsonl`` and submit it.

    An entry is considered invalid and skipped if it has already reached its
    maximum allowed attempt count (as defined in ``queue_config.json``).

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory (must be named 'queue').

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

        pipeline_key = "pipeline-" + pipeline
        version_key = "version-" + version
        params_key = "params-" + params
        params_cfg = queue_config[pipeline_key]["versions"][version_key]["params"][params_key]

        global_max_attempts = params_cfg["max_attempts_per_asset"]
        asset_overrides = params_cfg["asset_overrides"]

        submitted_counter = _fetch_counts(
            file_path=submitted_file,
            pipeline=pipeline,
            version=version,
            params=params,
        )

        if submitted_counter.get(content_id, 0) >= asset_overrides.get(content_id, global_max_attempts):
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
    subprocess.run(
        [
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
        ],
        check=True,
    )
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


def process_queue(*, cwd: pathlib.Path) -> None:
    """
    Process the current state of the queue.

    The queue is a single flat ``waiting.jsonl`` at the root of the queue directory.
    Each line is a JSON object with fields: pipeline, version, params, and content_id.

    If there are no waiting entries for a pipeline/version/params combination, it will be
    re-filled in accordance with ``queue_config.json`` and the current state of the
    qualifying AIND cache.  The fill order follows the priority lists defined in
    ``queue_config.json`` at the pipeline and version levels.

    If there are no currently running jobs, the next entry in ``waiting.jsonl`` will be
    popped and submitted according to the logic in ``submit_job.py``.

    Parameters
    ----------
    cwd : pathlib.Path
        Path to the queue root directory.  The directory must be named ``'queue'``.

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

    for pipeline_name, pipeline_data in queue_config.items():
        version_priority_order = pipeline_data.get("priority", [])
        versions_data = pipeline_data.get("versions", {})

        prioritized_version_names = [v for v in version_priority_order if v in versions_data]
        remaining_version_names = [v for v in versions_data if v not in prioritized_version_names]
        version_names = prioritized_version_names + remaining_version_names

        for pipeline_version in version_names:
            version_data = versions_data[pipeline_version]
            params_priority_order = version_data.get("priority", [])
            params_data = version_data.get("params", {})

            prioritized_params_names = [p for p in params_priority_order if p in params_data]
            remaining_params_names = [p for p in params_data if p not in prioritized_params_names]
            params_names = prioritized_params_names + remaining_params_names

            for params in params_names:
                _fill_waiting(
                    cwd=cwd,
                    pipeline_name=pipeline_name,
                    pipeline_version=pipeline_version,
                    params=params,
                )

    any_running = _determine_running()
    if not any_running:
        _submit_next(cwd=cwd)
