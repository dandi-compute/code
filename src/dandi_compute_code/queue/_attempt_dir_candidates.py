import pathlib


def _attempt_dir_candidates(*, base_dir: pathlib.Path, entry: dict) -> tuple[pathlib.Path, pathlib.Path]:
    """
    Return (flat_layout_path, legacy_nested_layout_path) for an attempt entry.

    Raises
    ------
    ValueError
        If *entry* has no ``dandi_path`` key, or if its ``dandi_path`` value is
        an empty string.
    """
    dandiset_id = entry["dandiset_id"]
    dandi_path = entry.get("dandi_path")
    if dandi_path is None:
        message = f"Entry has invalid dandi_path field (missing): {entry!r}"
        raise ValueError(message)
    if dandi_path == "":
        message = f"Entry has invalid dandi_path field (empty): {entry!r}"
        raise ValueError(message)
    normalized_dandi_path = dandi_path.removesuffix(".nwb")
    pipeline = entry["pipeline"]
    version = entry["version"]
    params = entry["params"]
    config = entry["config"]
    attempt = entry["attempt"]

    pipeline_dir = base_dir / "derivatives" / f"dandiset-{dandiset_id}" / pathlib.PurePosixPath(normalized_dandi_path)
    pipeline_dir = pipeline_dir / f"pipeline-{pipeline}"

    codebase = entry.get("codebase")
    codebase_segment = f"_codebase-{codebase}" if codebase is not None else ""
    flat_attempt_dir = (
        pipeline_dir / f"version-{version}_params-{params}_config-{config}{codebase_segment}_attempt-{attempt}"
    )
    nested_attempt_dir = pipeline_dir / f"version-{version}" / f"params-{params}_config-{config}_attempt-{attempt}"
    return flat_attempt_dir, nested_attempt_dir
