import functools
import json
import logging
import urllib.request

from ._globals import _ASSETS_JSONLD_URL

_log = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _load_assets_jsonld_metadata() -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    """
    Load content-id and path metadata from the DANDI 001697 draft ``assets.jsonld`` stream.

    Returns
    -------
    tuple[dict[str, dict[str, object]], dict[str, str]]
        ``(content_id_to_asset, path_to_date_modified)`` dictionaries.
    """
    content_id_to_asset: dict[str, dict[str, object]] = {}
    path_to_date_modified: dict[str, str] = {}
    try:
        with urllib.request.urlopen(url=_ASSETS_JSONLD_URL) as response:
            for raw_line in response:
                line = raw_line.decode("utf-8").strip()
                if not line:
                    continue
                try:
                    asset = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(asset, dict):
                    continue
                path = asset.get("path")
                date_modified = asset.get("dateModified")
                if isinstance(path, str) and isinstance(date_modified, str):
                    path_to_date_modified[path] = date_modified
                content_urls = asset.get("contentUrl")
                if not isinstance(content_urls, list):
                    continue
                content_id = next(
                    (
                        content_url.rsplit("/", 1)[-1]
                        for content_url in content_urls
                        if isinstance(content_url, str) and "/blobs/" in content_url
                    ),
                    None,
                )
                if isinstance(content_id, str) and content_id:
                    content_id_to_asset[content_id] = asset
    except Exception as exception:
        _log.warning("Unable to load metadata from %s: %s", _ASSETS_JSONLD_URL, exception)
    return content_id_to_asset, path_to_date_modified
