import datetime
import json
import pathlib
import re

from ..queue import refresh_waiting_queue

_ATTEMPT_DIR_RE = re.compile(
    r"(?:version-(?P<version_in_name>.+?)_)?params-(?P<params>[^_]+)_config-(?P<config>.+)_attempt-(?P<attempt>\d+)"
)
_ATTEMPT_SUFFIX_RE = re.compile(r"_attempt-\d+$")


def _parse_attempt_dir(attempt_dir: pathlib.Path) -> dict | None:
    """
    Parse a single attempt directory into a flat record dict.

    The expected path structure (relative to ``derivatives/dandiset-{dandiset_id}/``) is::

        sub-{subject}/[ses-{session}/]pipeline-{pipeline}/
            version-{version}_params-{params}_config-{config}_attempt-{attempt}/

    Legacy layout with an additional version directory is also accepted::

        sub-{subject}/[ses-{session}/]pipeline-{pipeline}/version-{version}/
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

    # The directory above pipeline-* is either ses-* or sub-*
    above_pipeline = pipeline_dir.parent
    if above_pipeline.name.startswith("ses-"):
        session: str | None = above_pipeline.name[len("ses-") :]
        subject_dir = above_pipeline.parent
    else:
        session = None
        subject_dir = above_pipeline

    if not subject_dir.name.startswith("sub-"):
        return None
    subject = subject_dir.name[len("sub-") :]

    dandiset_dir = subject_dir.parent
    if not dandiset_dir.name.startswith("dandiset-"):
        return None
    dandiset_id = dandiset_dir.name[len("dandiset-") :]

    has_code = (attempt_dir / "code").is_dir()
    has_output = (attempt_dir / "derivatives").is_dir()
    logs_dir = attempt_dir / "logs"
    has_logs = logs_dir.is_dir() and any(f for f in logs_dir.iterdir() if f.name != "dataset_description.json")
    created_at = datetime.datetime.fromtimestamp(attempt_dir.stat().st_ctime, tz=datetime.timezone.utc).isoformat()

    return {
        "dandiset_id": dandiset_id,
        "subject": subject,
        "session": session,
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
        ``(dandiset_id, subject, session, pipeline, version, params, config,
        attempt)``.  Each record contains:

        * ``dandiset_id`` – value of the ``dandiset-`` BIDS entity
        * ``subject``     – value of the ``sub-`` BIDS entity
        * ``session``     – value of the ``ses-`` BIDS entity, or ``null``
        * ``pipeline``    – value of the ``pipeline-`` BIDS entity
        * ``version``     – value of the ``version-`` BIDS entity
        * ``params``      – params portion of the attempt directory name
        * ``config``      – config portion of the attempt directory name
        * ``attempt``     – integer attempt number
        * ``has_code``    – ``True`` if a ``code/`` subdirectory is present
        * ``has_output``  – ``True`` if a ``derivatives/`` subdirectory is present
        * ``has_logs``    – ``True`` if a ``logs/`` subdirectory is present and non-empty
        * ``created_at``  – ISO 8601 UTC timestamp derived from the attempt directory's ``st_ctime``
          stat (last metadata-change time on Unix/Linux; creation time on Windows/macOS)
    """
    derivatives = dandiset_directory / "derivatives"
    if not derivatives.is_dir():
        return []

    attempt_re = _ATTEMPT_SUFFIX_RE
    records: list[dict] = []

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
                records.append(record)

    records.sort(
        key=lambda r: (
            r["dandiset_id"],
            r["subject"],
            r["session"] or "",
            r["pipeline"],
            r["version"],
            r["params"],
            r["config"],
            r["attempt"],
        )
    )
    return records


def write_state_and_waiting_jsonl(dandiset_directory: pathlib.Path, queue_directory: pathlib.Path) -> None:
    """
    Scan *dandiset_directory* and write queue ``state.jsonl`` and ``waiting.jsonl``.

    Parameters
    ----------
    dandiset_directory : pathlib.Path
        Path to a local clone of the dandiset repository.
    queue_directory : pathlib.Path
        Path to the queue root directory containing ``queue_config.json``.

    Raises
    ------
    FileNotFoundError
        If ``queue_config.json`` is not found in *queue_directory*.
    """
    queue_config_file = queue_directory / "queue_config.json"
    if not queue_config_file.exists():
        message = f"'queue_config.json' not found in '{queue_directory}'."
        raise FileNotFoundError(message)

    records = scan_dandiset_directory(dandiset_directory=dandiset_directory)
    state_file = queue_directory / "state.jsonl"
    with state_file.open(mode="w") as file_stream:
        for record in records:
            file_stream.write(json.dumps(record) + "\n")

    refresh_waiting_queue(cwd=queue_directory)
