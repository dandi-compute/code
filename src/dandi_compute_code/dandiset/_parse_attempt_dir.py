import datetime
import pathlib

from ._globals import _ATTEMPT_DIR_RE
from ._parse_content_id_from_submission_script import _parse_content_id_from_submission_script


# TODO: rename to _parse_job_capsule_id
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
