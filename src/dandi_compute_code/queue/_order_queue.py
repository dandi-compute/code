from ._build_processing_order import _build_processing_order


# TODO: determine if we still need this
def order_queue(*, state_entries: list[dict], queue_config: dict, limit: int | None = None) -> list[dict]:
    """
    Build the priority-ordered waiting list from in-memory state entries.

    :param state_entries: Records loaded from ``state.jsonl``.
    :type state_entries: list[dict]
    :param queue_config: Parsed contents of ``queue_config.json``.
    :type queue_config: dict
    :param limit: If provided, truncate output to the first *limit* entries.
    :type limit: int, optional
    :returns: Ordered queue entries.
    :rtype: list[dict]
    """
    ordered = _build_processing_order(state_entries=state_entries, queue_config=queue_config)
    if limit is not None:
        ordered = ordered[:limit]
    return ordered
