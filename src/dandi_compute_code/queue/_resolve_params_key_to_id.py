from ._globals import _AIND_EPHYS_PARAMS_REGISTRY


def _resolve_params_key_to_id(pipeline: str, params_key: str) -> str:
    """
    Resolve a human-readable parameters key to its 7-character hash ID.

    ``queue_config.json`` stores parameters references as human-readable key
    names (e.g. ``"default"``), but the on-disk directory names — and therefore
    the ``params`` field recorded in queue state entries — use the
    first seven hex characters of the MD5 checksum of the parameters file (e.g.
    ``"98fd947"``).  This function bridges that gap by looking up the registered
    MD5 for a known key.

    For the ``aind+ephys`` pipeline the lookup is performed against
    ``registered_params.json`` inside the pipeline module.  For any other
    pipeline, or if the key is not found in the registry, the *params_key* is
    returned unchanged so that callers that already store raw hash IDs continue
    to work.

    :param pipeline: The pipeline name as recorded in the state entry (e.g. ``"aind+ephys"``).
    :type pipeline: str
    :param params_key: The human-readable key from ``params_priority`` in ``queue_config.json``
        (e.g. ``"default"``), or a raw hash ID.
    :type params_key: str
    :returns: The 7-character hash ID corresponding to *params_key*, or *params_key*
        itself if no mapping is found.
    :rtype: str
    """
    if pipeline == "aind+ephys":
        entry = _AIND_EPHYS_PARAMS_REGISTRY.get(params_key)
        if entry:
            return entry["md5"][:7]
    return params_key
