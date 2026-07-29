import gzip
import json
import urllib.request

# Each pipeline draws its qualifying content IDs from its own source. The AIND
# list is gzip-compressed; the LFP list is a plain JSON Lines file.
_DEFAULT_QUALIFYING_SOURCE = (
    "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/dist/"
    "derivatives/qualifying_aind_content_ids.jsonl.gz",
    True,
)
_QUALIFYING_SOURCE_BY_PIPELINE = {
    "lfp": (
        "https://raw.githubusercontent.com/dandi-cache/qualifying-lfp-content-ids/derivatives/"
        "derivatives/qualifying_lfp_content_ids.jsonl",
        False,
    ),
}


def _fetch_qualifying_content_ids(pipeline_name: str, /) -> list[str]:
    """
    Fetch the qualifying content IDs for a pipeline from its source.

    Ordering is left to the caller so that each preparation path keeps using its
    own ``_order_content_ids_for_uniform_dandiset_sampling`` binding.

    :param pipeline_name: The pipeline key from ``queue_config.json``.
    :return: The qualifying content IDs as fetched, in source order.
    :rtype: list of str
    """
    url, compressed = _QUALIFYING_SOURCE_BY_PIPELINE.get(pipeline_name, _DEFAULT_QUALIFYING_SOURCE)
    with urllib.request.urlopen(url=url) as response:
        raw = response.read()
    decoded = gzip.decompress(raw).decode() if compressed else raw.decode()
    fetched_content_ids = [json.loads(line) for line in decoded.splitlines() if line.strip()]
    return fetched_content_ids
