from ._resolve_params_key_to_id import _resolve_params_key_to_id
from ._version_matches import _version_matches


# TODO: evaluate if this can be removed
def _build_processing_order(
    *,
    state_entries: list[dict],
    queue_config: dict,
) -> list[dict]:
    """
    Build an ordered list of pending entries using zipper-style interleaving.

    Filters *state_entries* to those that are prepared but not yet run
    (``has_code=True``, ``has_logs=False``, ``has_output=False``).  For each
    pipeline and version declared in ``queue_config``, dandiset instances
    (identified by ``dandiset_id``, ``dandi_path``, and ``config``)
    are sorted by their earliest ``created_at`` timestamp.  Within each
    instance the entries are ordered by ``params_priority``, ensuring that all
    parameterizations of a dandiset for a given version are queued before
    moving on to the next dandiset or version.

    Parameters
    ----------
    state_entries : list[dict]
        Records produced by :func:`~dandi_compute_code.dandiset.scan_dandiset_directory`
        (or loaded from a ``state.jsonl`` file).
    queue_config : dict
        Parsed contents of ``queue_config.json``.  Expected shape::

            {
                "pipelines": {
                    "<pipeline_name>": {
                        "version_priority": [...],
                        "params_priority": [...]
                    }
                }
            }

    Returns
    -------
    list[dict]
        Ordered list of pending entries ready to be submitted.
    """
    pending = [e for e in state_entries if e.get("has_code") and not e.get("has_output") and not e.get("has_logs")]

    result: list[dict] = []
    for pipeline_name, pipeline_data in queue_config.get("pipelines", {}).items():
        version_priority = pipeline_data.get("version_priority", [])
        params_priority = pipeline_data.get("params_priority", [])

        for version in version_priority:
            version_entries = [
                e
                for e in pending
                if e.get("pipeline") == pipeline_name and _version_matches(e.get("version", ""), version)
            ]

            # Group by dandiset instance: (dandiset_id, dandi_path, config)
            instance_groups: dict[tuple, list[dict]] = {}
            for entry in version_entries:
                key = (
                    entry.get("dandiset_id", ""),
                    entry.get("dandi_path", ""),
                    entry.get("config", ""),
                )
                instance_groups.setdefault(key, []).append(entry)

            # Sort instances by their earliest created_at timestamp
            sorted_instances = sorted(
                instance_groups.items(),
                key=lambda kv: min(e.get("created_at", "") for e in kv[1]),
            )

            # Zipper: for each instance add entries in params_priority order
            for _key, entries in sorted_instances:
                for params_key in params_priority:
                    params_id = _resolve_params_key_to_id(pipeline_name, params_key)
                    matching = [e for e in entries if e.get("params") == params_id]
                    result.extend(matching)

    return result
