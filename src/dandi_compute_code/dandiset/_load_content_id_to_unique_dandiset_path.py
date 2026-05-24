import functools
import gzip
import json
import urllib.request

from ._globals import _CONTENT_ID_TO_UNIQUE_DANDISET_PATH_URL


@functools.lru_cache(maxsize=1)
def _load_content_id_to_unique_dandiset_path() -> dict[str, dict[str, str]]:
    """
    Load the content ID to unique Dandiset path mapping.

    Raises
    ------
    RuntimeError
        If the mapping cannot be downloaded or decoded from the remote URL.
        The original exception is chained via ``raise ... from``.
    """
    try:
        with urllib.request.urlopen(url=_CONTENT_ID_TO_UNIQUE_DANDISET_PATH_URL) as response:
            return json.loads(gzip.decompress(response.read()))
    except Exception as exception:
        message = (
            "Unable to load content-id-to-unique-dandiset-path mapping from "
            f"{_CONTENT_ID_TO_UNIQUE_DANDISET_PATH_URL}"
        )
        raise RuntimeError(message) from exception
