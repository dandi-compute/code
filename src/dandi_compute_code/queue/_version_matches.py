import re


# TODO: might be able to get rid of this in favor of the semantic versions
def _version_matches(state_version: str, config_version: str) -> bool:
    """
    Return True if *state_version* matches *config_version*.

    An exact match is always accepted.  Additionally, a *state_version* that
    extends *config_version* by appending ``+<7 hex chars>`` (the first 7
    characters of the dandi-compute/code repo commit hash) is also accepted,
    providing backward-compatible matching when the queue config does not yet
    include the code-repo suffix.  The suffix is validated against the pattern
    ``[0-9a-f]{7,40}`` to accommodate any valid git short-hash length.
    """
    if state_version == config_version:
        return True
    if state_version.startswith(config_version + "+"):
        extra = state_version[len(config_version) + 1 :]
        return bool(re.fullmatch(r"[0-9a-f]{7,40}", extra))
    return False
