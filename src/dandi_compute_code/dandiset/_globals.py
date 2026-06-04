import re

# TODO: rename _ATTEMPT_DIR_RE to JOB_CAPSULE_ID_PATTERN
_ATTEMPT_DIR_RE = re.compile(
    r"(?:version-(?P<version_in_name>.+?)(?:_codebase-(?P<codebase>[^_]+))?_)?params-(?P<params>[^_]+)_config-(?P<config>[^_]+)_attempt-(?P<attempt>\d+)"
)
_ATTEMPT_SUFFIX_RE = re.compile(r"_attempt-\d+$")
_SANDBOX_DANDISET_ID = "214527"
_SANDBOX_API_URL = "https://api.sandbox.dandiarchive.org/api"
_CONTENT_ID_TO_UNIQUE_DANDISET_PATH_URL = (
    "https://raw.githubusercontent.com/dandi-cache/content-id-to-unique-dandiset-path/refs/heads/min/"
    "derivatives/content_id_to_unique_dandiset_path.min.json.gz"
)
_ASSETS_JSONLD_URL = "https://dandiarchive.s3.amazonaws.com/dandisets/001697/draft/assets.jsonld"
