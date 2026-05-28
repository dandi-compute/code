import pathlib

from ._globals import _ATTEMPT_SUFFIX_RE, _SANDBOX_DANDISET_ID
from ._load_assets_jsonld_metadata import load_assets_jsonld_metadata
from ._parse_attempt_dir import _parse_attempt_dir

_load_assets_jsonld_metadata = load_assets_jsonld_metadata


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
        * ``dandi_path`` – source asset path from ``assets.jsonld`` when present;
          otherwise the scanned path segment under ``dandiset-{id}/`` preceding ``pipeline-*``
        * ``content_id``  – input content identifier parsed from ``code/submit.sh``
        * ``asset_size_bytes`` – source asset size in bytes from ``assets.jsonld``;
          ``null`` when missing
        * ``pipeline``    – value of the ``pipeline-`` BIDS entity
        * ``version``     – value of the ``version-`` BIDS entity
        * ``params``      – params portion of the attempt directory name
        * ``config``      – config portion of the attempt directory name
        * ``attempt``     – integer attempt number
        * ``has_code``           – ``True`` if a ``code/`` subdirectory is present
        * ``has_output``         – ``True`` if a ``derivatives/`` subdirectory is present
        * ``has_logs``           – ``True`` if a ``logs/`` subdirectory is present and non-empty
        * ``created_at``         – source ``blobDateModified`` ISO 8601 timestamp from ``assets.jsonld``
          for the ``content_id`` when available; otherwise the attempt directory ``st_ctime`` timestamp
        * ``job_completion_time`` – ``dateModified`` ISO 8601 string from ``assets.jsonld`` for the first
          asset in the ``logs/`` subdirectory; ``null`` when no logs are present or metadata is missing
    :rtype: list[dict]
    """
    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return []

    attempt_re = _ATTEMPT_SUFFIX_RE
    records: list[dict] = []
    assets_jsonld_metadata = _load_assets_jsonld_metadata()
    if isinstance(assets_jsonld_metadata, tuple):
        content_id_to_asset, path_to_date_modified = assets_jsonld_metadata
    else:
        content_id_to_asset = assets_jsonld_metadata.content_id_to_asset
        path_to_date_modified = assets_jsonld_metadata.path_to_date_modified

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
                source_asset_metadata = content_id_to_asset.get(record["content_id"])
                if source_asset_metadata is not None:
                    content_size = source_asset_metadata.get("contentSize")
                    if isinstance(content_size, int):
                        record["asset_size_bytes"] = content_size
                    elif isinstance(content_size, str) and content_size.isdigit():
                        record["asset_size_bytes"] = int(content_size)
                    else:
                        record["asset_size_bytes"] = None
                    resolved_dandi_path = source_asset_metadata.get("path")
                    if isinstance(resolved_dandi_path, str):
                        record["dandi_path"] = resolved_dandi_path
                    blob_date_modified = source_asset_metadata.get("blobDateModified")
                    if isinstance(blob_date_modified, str):
                        record["created_at"] = blob_date_modified
                else:
                    record["asset_size_bytes"] = None

                if record["has_logs"]:
                    logs_dir = attempt_dir / "logs"
                    # Exclude dataset_description.json as it is a BIDS metadata file, not a log asset
                    first_log_file = next(
                        iter(sorted(f for f in logs_dir.iterdir() if f.name != "dataset_description.json")),
                        None,
                    )
                    if first_log_file is not None:
                        log_asset_path = first_log_file.relative_to(dandiset_directory).as_posix()
                        record["job_completion_time"] = path_to_date_modified.get(log_asset_path)
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
