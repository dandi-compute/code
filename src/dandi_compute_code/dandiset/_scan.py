import datetime
import os
import pathlib
import re
import warnings

import dandi.dandiapi

_ATTEMPT_DIR_RE = re.compile(
    r"(?:version-(?P<version_in_name>.+?)_)?params-(?P<params>[^_]+)_config-(?P<config>.+)_attempt-(?P<attempt>\d+)"
)
_ATTEMPT_SUFFIX_RE = re.compile(r"_attempt-\d+$")
_SANDBOX_DANDISET_ID = "214527"
_SANDBOX_API_URL = "https://api.sandbox.dandiarchive.org/api"


def _create_dandi_api_client(*, api_token: str, dandiset_id: str) -> dandi.dandiapi.DandiAPIClient:
    """Create a DANDI API client, routing known sandbox dandiset IDs to the sandbox API."""
    if dandiset_id == _SANDBOX_DANDISET_ID:
        return dandi.dandiapi.DandiAPIClient(api_url=_SANDBOX_API_URL)
    return dandi.dandiapi.DandiAPIClient(token=api_token)


def _parse_content_id_from_submission_script(attempt_dir: pathlib.Path) -> str:
    """Read a content ID from ``code/submit.sh``."""
    script_file = attempt_dir / "code" / "submit.sh"
    if not script_file.is_file():
        raise ValueError(f"Unable to determine content_id for {attempt_dir}: missing {script_file}")

    script_text = script_file.read_text()

    for line in script_text.splitlines():
        if line.startswith("NWB_FILE_PATH="):
            nwb_file_path = line.split("=", maxsplit=1)[1].strip().strip('"').strip("'")
            content_id = pathlib.PurePosixPath(nwb_file_path).name
            if not content_id:
                raise ValueError(
                    "Unable to determine content_id for "
                    f"{attempt_dir}: empty content_id from NWB_FILE_PATH in {script_file}"
                )
            return content_id
    raise ValueError(
        f"Unable to determine content_id for {attempt_dir}: missing/invalid NWB_FILE_PATH in {script_file}"
    )


def _lookup_asset_size_bytes(
    *,
    api_token: str,
    dandiset_id: str,
    dandi_path: str,
    content_id: str,
) -> int | None:
    """Lookup asset size from DANDI API and confirm by matching blob ID to ``content_id``."""
    client = _create_dandi_api_client(api_token=api_token, dandiset_id=dandiset_id)
    dandiset = client.get_dandiset(dandiset_id=dandiset_id)
    asset_path_prefix = dandi_path

    filtered_assets: list[tuple[str, dict]] = []
    for asset in dandiset.get_assets_with_path_prefix(path=asset_path_prefix):
        metadata = asset.get_raw_metadata()
        filtered_assets.append((asset.path, metadata))

    matching_metadata: list[dict] = []
    for _asset_path, metadata in filtered_assets:
        content_urls = metadata.get("contentUrl")
        if not isinstance(content_urls, list) or len(content_urls) < 2:
            continue
        blob_url = content_urls[1]
        if not isinstance(blob_url, str):
            continue
        if pathlib.PurePosixPath(blob_url).name == content_id:
            matching_metadata.append(metadata)

    if len(matching_metadata) != 1:
        candidate_paths = [p for p, _ in filtered_assets]
        warnings.warn(
            (
                f"Unable to resolve asset_size_bytes for {content_id}. "
                f"Expected exactly 1 asset under {asset_path_prefix} "
                f"with blob ID {content_id} but found {len(matching_metadata)}. "
                f"Matching path assets: {candidate_paths}."
            ),
            stacklevel=2,
        )
        return None

    content_size = matching_metadata[0].get("contentSize")
    if isinstance(content_size, int):
        return content_size
    if isinstance(content_size, str) and content_size.isdigit():
        return int(content_size)
    warnings.warn(
        f"Unable to resolve asset_size_bytes for {content_id}. Invalid or missing contentSize value {content_size!r}.",
        stacklevel=2,
    )
    return None


def _lookup_job_completion_time(
    *,
    api_token: str,
    dandiset_id: str,
    log_asset_path: str,
) -> str | None:
    """Look up the ``dateModified`` field for a log asset in the DANDI API."""
    client = _create_dandi_api_client(api_token=api_token, dandiset_id=dandiset_id)
    dandiset = client.get_dandiset(dandiset_id=dandiset_id)

    matching_assets = list(dandiset.get_assets_with_path_prefix(path=log_asset_path))
    if len(matching_assets) != 1:
        warnings.warn(
            (
                f"Unable to resolve job_completion_time for log asset at {log_asset_path}. "
                f"Expected exactly 1 asset but found {len(matching_assets)}."
            ),
            stacklevel=2,
        )
        return None

    asset = matching_assets[0]
    metadata = asset.get_raw_metadata()
    date_modified = metadata.get("dateModified")
    if isinstance(date_modified, str):
        return date_modified
    warnings.warn(
        (
            f"Unable to resolve job_completion_time for log asset at {log_asset_path}. "
            f"Invalid or missing dateModified value {date_modified!r}."
        ),
        stacklevel=2,
    )
    return None


def _parse_attempt_dir(attempt_dir: pathlib.Path) -> dict | None:
    """
    Parse a single attempt directory into a flat record dict.

    The expected path structure (relative to ``derivatives/dandiset-{dandiset_id}/``) is::

        <dandi-path>/pipeline-{pipeline}/
            version-{version}_params-{params}_config-{config}_attempt-{attempt}/

    Legacy layout with an additional version directory is also accepted::

        <dandi-path>/pipeline-{pipeline}/version-{version}/
            params-{params}_config-{config}_attempt-{attempt}/

    Parameters
    ----------
    attempt_dir : pathlib.Path
        The attempt directory (name must match ``params-*_config-*_attempt-*``).

    Returns
    -------
    dict or None
        A flat dict with all entities and state flags, or ``None`` if the path
        does not match the expected structure.
    """
    attempt_name = attempt_dir.name
    attempt_match = _ATTEMPT_DIR_RE.fullmatch(attempt_name)
    if not attempt_match:
        return None

    version_from_name = attempt_match.group("version_in_name")
    version_or_pipeline_dir = attempt_dir.parent

    if version_or_pipeline_dir.name.startswith("version-"):
        version = version_or_pipeline_dir.name[len("version-") :]
        pipeline_dir = version_or_pipeline_dir.parent
    elif version_or_pipeline_dir.name.startswith("pipeline-"):
        if not version_from_name:
            return None
        version = version_from_name
        pipeline_dir = version_or_pipeline_dir
    else:
        return None

    if not pipeline_dir.name.startswith("pipeline-"):
        return None
    pipeline = pipeline_dir.name[len("pipeline-") :]

    dandiset_dir = next(
        (parent for parent in pipeline_dir.parents if parent.name.startswith("dandiset-")),
        None,
    )
    if dandiset_dir is None:
        return None
    dandiset_id = dandiset_dir.name[len("dandiset-") :]
    dandi_path_parts = pipeline_dir.relative_to(dandiset_dir).parts[:-1]
    if not dandi_path_parts:
        return None
    dandi_path = pathlib.PurePosixPath(*dandi_path_parts).as_posix()

    has_code = (attempt_dir / "code").is_dir()
    has_output = (attempt_dir / "derivatives").is_dir()
    logs_dir = attempt_dir / "logs"
    has_logs = logs_dir.is_dir() and any(f for f in logs_dir.iterdir() if f.name != "dataset_description.json")
    created_at = datetime.datetime.fromtimestamp(attempt_dir.stat().st_ctime, tz=datetime.timezone.utc).isoformat()
    content_id = _parse_content_id_from_submission_script(attempt_dir)

    record = {
        "dandiset_id": dandiset_id,
        "content_id": content_id,
        "dandi_path": dandi_path,
        "pipeline": pipeline,
        "version": version,
        "params": attempt_match.group("params"),
        "config": attempt_match.group("config"),
        "attempt": int(attempt_match.group("attempt")),
        "has_code": has_code,
        "has_output": has_output,
        "has_logs": has_logs,
        "created_at": created_at,
    }
    return record


def scan_dandiset_directory(dandiset_directory: pathlib.Path) -> list[dict]:
    """
    Scan a local dandiset directory and return a flat list of attempt records.

    Walks ``{dandiset_directory}/derivatives/dandiset-*/`` and finds every
    attempt directory (matched by the ``_attempt-<number>`` suffix) regardless
    of depth.  Each attempt directory is parsed into a flat dict containing all
    BIDS-encoded entities together with boolean state flags.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository (e.g. the 001697
        dandiset).  The function looks for a ``derivatives/`` subdirectory
        inside this path.

    Returns
    -------
    list[dict]
        A list of records, one per attempt directory, sorted by
        ``(dandiset_id, dandi_path, pipeline, version, params, config,
        attempt)``.  Each record contains:

        * ``dandiset_id`` – value of the ``dandiset-`` BIDS entity
        * ``dandi_path`` – full source path segment (under ``dandiset-{id}/``) preceding ``pipeline-*``
        * ``content_id``  – input content identifier parsed from ``code/submit.sh``
        * ``asset_size_bytes`` – source asset size in bytes from DANDI API lookup;
          ``null`` when no unique match is found, blob ID mismatches, or size is missing
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

    Raises
    ------
    AssertionError
        If ``DANDI_API_KEY`` is not set before scanning.
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
    size_lookup_cache: dict[tuple[str, str, str], int | None] = {}

    for dandiset_path in sorted(derivatives.iterdir()):
        if not dandiset_path.is_dir() or not dandiset_path.name.startswith("dandiset-"):
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
                record["asset_size_bytes"] = size_lookup_cache[size_lookup_key]

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
