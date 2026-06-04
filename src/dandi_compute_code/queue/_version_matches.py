def _version_matches(state_version: str, config_version: str) -> bool:
    """Return True if *state_version* matches *config_version*."""
    return state_version == config_version
