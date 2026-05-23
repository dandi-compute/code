import re


# TODO: might be able to remove this in favor of semantic versions
def _strip_commit_hash_suffix(version: str) -> str:
    """Strip a trailing +<git-hash> suffix from a version string when present."""
    version_parts = version.split("+")
    if len(version_parts) > 1 and re.fullmatch(r"[0-9a-f]{7,40}", version_parts[-1]):
        return "+".join(version_parts[:-1])
    return version
