import pathlib


# TODO: get rid of this; doesn't need to be it's own function
def _normalize_asset_path(asset_path: str) -> str:
    """Return a normalized POSIX asset path."""
    return pathlib.PurePosixPath(asset_path).as_posix()
