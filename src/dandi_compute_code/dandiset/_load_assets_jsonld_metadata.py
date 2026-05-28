import functools
import json
import logging
import urllib.request
from dataclasses import dataclass

from ._globals import _ASSETS_JSONLD_URL

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AssetMetadata:
    """Minimal indexed metadata for one asset path."""

    path: str
    date_modified: str | None
    content_id: str | None


@dataclass(frozen=True)
class AssetsJsonldMetadata:
    """Indexed metadata loaded from DANDI ``assets.jsonld``."""

    content_id_to_asset: dict[str, dict[str, object]]
    path_to_asset_metadata: dict[str, AssetMetadata]
    all_paths: frozenset[str]

    @property
    def path_to_date_modified(self) -> dict[str, str]:
        return {
            path: asset_metadata.date_modified
            for path, asset_metadata in self.path_to_asset_metadata.items()
            if isinstance(asset_metadata.date_modified, str)
        }

    @property
    def path_to_content_id(self) -> dict[str, str]:
        return {
            path: asset_metadata.content_id
            for path, asset_metadata in self.path_to_asset_metadata.items()
            if isinstance(asset_metadata.content_id, str)
        }


@functools.lru_cache(maxsize=1)
def load_assets_jsonld_metadata() -> AssetsJsonldMetadata:
    """
    Load content-id and path metadata from the DANDI 001697 draft ``assets.jsonld`` stream.

    :returns:
        Indexed assets metadata.
    :rtype: AssetsJsonldMetadata
    """
    content_id_to_asset: dict[str, dict[str, object]] = {}
    path_to_asset_metadata: dict[str, AssetMetadata] = {}
    all_paths: set[str] = set()
    try:
        with urllib.request.urlopen(url=_ASSETS_JSONLD_URL, timeout=30) as response:
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
                content_size = asset.get("contentSize")
                if isinstance(content_size, str) and content_size.isdigit():
                    asset["contentSize"] = int(content_size)
                path = asset.get("path")
                date_modified = asset.get("dateModified")
                if isinstance(path, str):
                    all_paths.add(path)
                content_urls = asset.get("contentUrl")
                content_id: str | None = None
                if not isinstance(content_urls, list):
                    content_urls = []
                content_id = next(
                    (
                        content_url.rsplit("/", 1)[-1].split("?", 1)[0]
                        for content_url in content_urls
                        if isinstance(content_url, str) and "/blobs/" in content_url
                    ),
                    None,
                )
                if isinstance(content_id, str) and content_id:
                    content_id_to_asset[content_id] = asset
                if isinstance(path, str):
                    existing_path_metadata = path_to_asset_metadata.get(path)
                    path_to_asset_metadata[path] = AssetMetadata(
                        path=path,
                        date_modified=(
                            date_modified
                            if isinstance(date_modified, str)
                            else (existing_path_metadata.date_modified if existing_path_metadata is not None else None)
                        ),
                        content_id=(
                            content_id
                            if isinstance(content_id, str) and content_id
                            else (existing_path_metadata.content_id if existing_path_metadata is not None else None)
                        ),
                    )
    except Exception as exception:
        _log.warning("Unable to load metadata from %s: %s", _ASSETS_JSONLD_URL, exception)
    return AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset,
        path_to_asset_metadata=path_to_asset_metadata,
        all_paths=frozenset(all_paths),
    )


def _load_assets_jsonld_metadata() -> AssetsJsonldMetadata:
    """Backward-compatible alias for :func:`load_assets_jsonld_metadata`."""
    return load_assets_jsonld_metadata()
