def _entry_identity(entry: dict) -> tuple:
    """Return a stable tuple key for matching queue/state/last-submitted entries."""
    return (
        entry.get("dandiset_id"),
        entry.get("dandi_path"),
        entry.get("pipeline"),
        entry.get("version"),
        entry.get("params"),
        entry.get("config"),
        entry.get("attempt"),
    )
