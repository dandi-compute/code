import datetime
import pathlib

from ._globals import _JOB_CAPSULE_DIR_RE
from ._parse_content_id_from_submission_script import _parse_content_id_from_submission_script


def _parse_job_capsule_dir(capsule_dir: pathlib.Path, /) -> dict | None:
    """
    Parse a single job capsule directory into a flat record dict.

    The expected path structure (relative to
    ``derivatives/dandisets-{first 3 digits}/dandiset-{dandiset_id}/``) is::

        <dandi-path>/pipeline-{pipeline}/
            version-{version}_codebase-{codebase}_params-{params}_config-{config}/

    Legacy layout with an additional version directory is also accepted::

        <dandi-path>/pipeline-{pipeline}/version-{version}/
            params-{params}_config-{config}/

    Both layouts also accept a trailing ``_attempt-{n}``, which capsules written before
    attempts were retired still carry.

    :param capsule_dir: The job capsule directory (name must match ``params-*_config-*``).
    :type capsule_dir: pathlib.Path
    :returns: A flat dict with all entities and state flags, or ``None`` if the path
        does not match the expected structure.
    :rtype: dict or None
    """
    capsule_match = _JOB_CAPSULE_DIR_RE.fullmatch(capsule_dir.name)
    if not capsule_match:
        return None

    version_from_name = capsule_match.group("version_in_name")
    version_or_pipeline_dir = capsule_dir.parent

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

    has_code = (capsule_dir / "code").is_dir()
    has_output = (capsule_dir / "derivatives").is_dir()
    logs_dir = capsule_dir / "logs"
    has_logs = logs_dir.is_dir() and any(f for f in logs_dir.iterdir() if f.name != "dataset_description.json")
    created_at = datetime.datetime.fromtimestamp(capsule_dir.stat().st_ctime, tz=datetime.timezone.utc).isoformat()
    content_id = _parse_content_id_from_submission_script(capsule_dir)

    record = {
        "dandiset_id": dandiset_id,
        "content_id": content_id,
        "dandi_path": dandi_path,
        "pipeline": pipeline,
        "version": version,
        "params": capsule_match.group("params"),
        "config": capsule_match.group("config"),
        "has_code": has_code,
        "has_output": has_output,
        "has_logs": has_logs,
        "created_at": created_at,
    }
    return record
