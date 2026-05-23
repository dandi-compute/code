import pathlib


def _normalize_asset_path(asset_path: str) -> str:
    """Return a normalized POSIX asset path."""
    return pathlib.PurePosixPath(asset_path).as_posix()
