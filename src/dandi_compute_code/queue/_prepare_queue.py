import gzip
import json
import pathlib
import urllib.request
from collections import defaultdict

from ._load_queue_config import _load_queue_config
from ._order_content_ids_for_uniform_dandiset_sampling import _order_content_ids_for_uniform_dandiset_sampling
from ._strip_commit_hash_suffix import _strip_commit_hash_suffix
from ._version_matches import _version_matches
from ..aind_ephys_pipeline import prepare_aind_ephys_job


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
        pipeline/version/params combinations).  When qualifying IDs are fetched
        automatically, they are randomized in round-robin order across source
        Dandisets before this limit is applied. Useful for testing.
    """
    queue_config = _load_queue_config(queue_directory=queue_directory)

    if content_ids is None:
        qualifying_aind_content_ids_url = (
            "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/"
            "derivatives/qualifying_aind_content_ids.min.json.gz"
        )
        with urllib.request.urlopen(url=qualifying_aind_content_ids_url) as response:
            fetched_content_ids = json.loads(gzip.decompress(response.read()))
        content_ids = _order_content_ids_for_uniform_dandiset_sampling(content_ids=fetched_content_ids)

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

                for content_id in content_ids:
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
