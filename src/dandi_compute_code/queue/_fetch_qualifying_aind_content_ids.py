import gzip
import json
import urllib.request

from ._globals import _QUALIFYING_AIND_CONTENT_IDS_URL


def _fetch_qualifying_aind_content_ids() -> list[str]:
    """
    Fetch the list of content IDs that currently qualify for the AIND ephys pipeline.

    The remote cache is a gzip-compressed JSON Lines file where each line is a
    single-entry ``{content_id: qualifies}`` object (``qualifies`` is a ``bool``)
    covering every content ID that qualifies for the (looser) ``qualifying-lfp-content-ids``
    cache; the lines are merged into one mapping and only the content IDs whose value is
    ``True`` are returned.

    Raises
    ------
    RuntimeError
        If the mapping cannot be downloaded or decoded from the remote URL.
        The original exception is chained via ``raise ... from``.
    """
    try:
        with urllib.request.urlopen(url=_QUALIFYING_AIND_CONTENT_IDS_URL) as response:
            decompressed = gzip.decompress(response.read()).decode()
        content_id_to_qualifies: dict[str, bool] = {}
        for line in decompressed.splitlines():
            if line.strip():
                content_id_to_qualifies.update(json.loads(line))
        return [content_id for content_id, qualifies in content_id_to_qualifies.items() if qualifies]
    except Exception as exception:
        message = f"Unable to load qualifying AIND content IDs from {_QUALIFYING_AIND_CONTENT_IDS_URL}"
        raise RuntimeError(message) from exception
