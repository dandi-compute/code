from ._globals import _DURATION_PART_RE


def _duration_string_to_seconds(duration_string: str) -> float:
    """Parse a Nextflow duration string (for example ``1m 5s``) into seconds."""
    total_seconds = 0.0
    for match in _DURATION_PART_RE.finditer(duration_string):
        value = float(match.group("value"))
        unit = match.group("unit")
        if unit == "ms":
            total_seconds += value / 1000.0
        elif unit == "s":
            total_seconds += value
        elif unit == "m":
            total_seconds += value * 60.0
        elif unit == "h":
            total_seconds += value * 3600.0
        elif unit == "d":
            total_seconds += value * 86400.0
    return total_seconds
