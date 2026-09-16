import re

from ._job_id import _JOB_ID_RE

#: Job capsule directory names predating the job ID, kept readable for the archived capsules
#: that still carry them.
_LEGACY_JOB_CAPSULE_DIR_RE = re.compile(
    r"(?:version-(?P<version_in_name>.+?)_codebase-(?P<codebase>[^_]+)_)?"
    # The config segment is absent on pipelines that have no config, such as ``lfp``.
    r"params-(?P<params>[^_]+)(?:_config-(?P<config>[^_]+))?"
    # Capsules formed before the attempt notion was retired carry a trailing attempt number.
    r"(?:_attempt-(?P<attempt>\d+))?"
)

#: Any job capsule directory name, current (``job-{YYMMDD}+{hash}``) or legacy.
_JOB_CAPSULE_DIR_RE = re.compile(rf"(?:{_JOB_ID_RE.pattern})|(?:{_LEGACY_JOB_CAPSULE_DIR_RE.pattern})")
_SANDBOX_DANDISET_ID = "214527"
_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"
_SANDBOX_API_URL = "https://api.sandbox.dandiarchive.org/api"
_CONTENT_ID_TO_USAGE_DANDISET_PATH_URL = (
    "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/derivatives/"
    "derivatives/content_id_to_usage_dandiset_path.jsonl"
)
_ASSETS_JSONLD_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/dandisets/{dandiset_id}/draft/assets.jsonld"
_ASSETS_JSONLD_URL = _ASSETS_JSONLD_URL_TEMPLATE.format(dandiset_id=_JOB_CAPSULES_DANDISET_ID)


def _dandiset_derivatives_relative_dir(dandiset_id: str) -> str:
    """
    Return the ``dandisets-{first three digits}/dandiset-{dandiset_id}`` path segment
    used to nest a Dandiset's derivatives tree, e.g. ``"dandisets-001/dandiset-001234"``
    for dandiset_id ``"001234"``.
    """
    return f"dandisets-{dandiset_id[:3]}/dandiset-{dandiset_id}"
