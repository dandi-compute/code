from ._build_processing_order import _build_processing_order


def order_queue(*, state_entries: list[dict], queue_config: dict, limit: int | None = None) -> list[dict]:
    """
    Build the priority-ordered waiting list from in-memory state entries.

    Parameters
    ----------
    state_entries : list[dict]
        Records produced by :func:`~dandi_compute_code.dandiset.scan_dandiset_directory`
        (or loaded from a ``state.jsonl`` file).
    queue_config : dict
        Parsed contents of ``queue_config.json``.
    limit : int, optional
        If provided, truncate output to the first *limit* entries.

    Returns
    -------
    list[dict]
        Ordered queue entries.
    """
    ordered = _build_processing_order(state_entries=state_entries, queue_config=queue_config)
    if limit is not None:
        ordered = ordered[:limit]
    return ordered
