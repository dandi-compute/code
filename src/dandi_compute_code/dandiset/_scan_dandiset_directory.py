import os
import pathlib

from ._globals import _ATTEMPT_SUFFIX_RE, _SANDBOX_DANDISET_ID
from ._lookup_asset_size_bytes import _lookup_asset_size_bytes
from ._lookup_job_completion_time import _lookup_job_completion_time
from ._parse_attempt_dir import _parse_attempt_dir


# TODO: create formal JobState LinkML model/schema that and rename this function to relate to that
def scan_dandiset_directory(dandiset_directory: pathlib.Path) -> list[dict]:
    """
    Scan a local dandiset directory and return a flat list of attempt records.

    Walks ``{dandiset_directory}/derivatives/dandiset-*/`` and finds every
    attempt directory (matched by the ``_attempt-<number>`` suffix) regardless
    of depth.  Each attempt directory is parsed into a flat dict containing all
    BIDS-encoded entities together with boolean state flags.

    :param dandiset_directory: Path to a local clone of the dandiset repository (e.g. the 001697
        dandiset).  The function looks for a ``derivatives/`` subdirectory
        inside this path.
    :type dandiset_directory: pathlib.Path
    :returns: A list of records, one per attempt directory, sorted by
        ``(dandiset_id, dandi_path, pipeline, version, params, config,
        attempt)``.  Each record contains:

        * ``dandiset_id`` – value of the ``dandiset-`` BIDS entity
        * ``dandi_path`` – resolved source asset path from DANDI API lookup when uniquely matched;
          otherwise the scanned path segment under ``dandiset-{id}/`` preceding ``pipeline-*``
        * ``content_id``  – input content identifier parsed from ``code/submit.sh``
        * ``asset_size_bytes`` – source asset size in bytes from DANDI API lookup at the
          mapped unique asset path; ``null`` when mapping/path lookup fails or size is missing
        * ``pipeline``    – value of the ``pipeline-`` BIDS entity
        * ``version``     – value of the ``version-`` BIDS entity
        * ``params``      – params portion of the attempt directory name
        * ``config``      – config portion of the attempt directory name
        * ``attempt``     – integer attempt number
        * ``has_code``           – ``True`` if a ``code/`` subdirectory is present
        * ``has_output``         – ``True`` if a ``derivatives/`` subdirectory is present
        * ``has_logs``           – ``True`` if a ``logs/`` subdirectory is present and non-empty
        * ``created_at``         – ISO 8601 UTC timestamp derived from the attempt directory's ``st_ctime``
          stat (last metadata-change time on Unix/Linux; creation time on Windows/macOS)
        * ``job_completion_time`` – ``dateModified`` ISO 8601 string from DANDI metadata for the first
          asset in the ``logs/`` subdirectory; ``null`` when no logs are present or the lookup fails
    :rtype: list[dict]
    :raises AssertionError: If ``DANDI_API_KEY`` is not set before scanning.
    """
    assert (
        "DANDI_API_KEY" in os.environ and os.environ["DANDI_API_KEY"]
    ), "`DANDI_API_KEY` environment variable must be set before scanning dandiset directory."
    api_token = os.environ["DANDI_API_KEY"]

    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return []

    attempt_re = _ATTEMPT_SUFFIX_RE
    records: list[dict] = []
    size_lookup_cache: dict[tuple[str, str, str], tuple[int | None, str | None]] = {}

    for dandiset_path in sorted(derivatives.iterdir()):
        if not dandiset_path.is_dir() or not dandiset_path.name.startswith("dandiset-"):
            continue
        if dandiset_path.name == f"dandiset-{_SANDBOX_DANDISET_ID}":
            continue
        for attempt_dir in sorted(dandiset_path.rglob("*_attempt-*")):
            if not attempt_dir.is_dir():
                continue
            if not attempt_re.search(attempt_dir.name):
                continue
            record = _parse_attempt_dir(attempt_dir)
            if record is not None:
                size_lookup_key = (
                    record["dandiset_id"],
                    record["dandi_path"],
                    record["content_id"],
                )
                if size_lookup_key not in size_lookup_cache:
                    size_lookup_cache[size_lookup_key] = _lookup_asset_size_bytes(
                        api_token=api_token,
                        dandiset_id=record["dandiset_id"],
                        dandi_path=record["dandi_path"],
                        content_id=record["content_id"],
                    )
                record["asset_size_bytes"], resolved_dandi_path = size_lookup_cache[size_lookup_key]
                if resolved_dandi_path is not None:
                    record["dandi_path"] = resolved_dandi_path

                if record["has_logs"]:
                    logs_dir = attempt_dir / "logs"
                    # Exclude dataset_description.json as it is a BIDS metadata file, not a log asset
                    first_log_file = next(
                        iter(sorted(f for f in logs_dir.iterdir() if f.name != "dataset_description.json")),
                        None,
                    )
                    if first_log_file is not None:
                        log_asset_path = first_log_file.relative_to(dandiset_directory).as_posix()
                        record["job_completion_time"] = _lookup_job_completion_time(
                            api_token=api_token,
                            dandiset_id=record["dandiset_id"],
                            log_asset_path=log_asset_path,
                        )
                    else:
                        record["job_completion_time"] = None
                else:
                    record["job_completion_time"] = None

                records.append(record)

    records.sort(
        key=lambda r: (
            r["dandiset_id"],
            r["dandi_path"],
            r["pipeline"],
            r["version"],
            r["params"],
            r["config"],
            r["attempt"],
        )
    )
    return records
