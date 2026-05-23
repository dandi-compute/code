import pathlib

from ._attempt_dir_candidates import _attempt_dir_candidates


def _resolve_attempt_dir(*, base_dir: pathlib.Path, entry: dict) -> pathlib.Path:
    """Resolve the best attempt-directory path for a queue entry."""
    flat_attempt_dir, nested_attempt_dir = _attempt_dir_candidates(base_dir=base_dir, entry=entry)
    if flat_attempt_dir.is_dir():
        return flat_attempt_dir
    if nested_attempt_dir.is_dir():
        return nested_attempt_dir

    dandiset_root = base_dir / "derivatives" / f"dandiset-{entry['dandiset_id']}"
    if not dandiset_root.is_dir():
        return nested_attempt_dir

    pipeline_dir_name = f"pipeline-{entry['pipeline']}"
    flat_attempt_dir_name = (
        f"version-{entry['version']}_params-{entry['params']}_config-{entry['config']}_attempt-{entry['attempt']}"
    )
    nested_attempt_dir_name = f"params-{entry['params']}_config-{entry['config']}_attempt-{entry['attempt']}"
    for pipeline_dir in sorted(dandiset_root.rglob(pipeline_dir_name)):
        if not pipeline_dir.is_dir():
            continue
        fallback_flat_attempt_dir = pipeline_dir / flat_attempt_dir_name
        if fallback_flat_attempt_dir.is_dir():
            return fallback_flat_attempt_dir
        fallback_nested_attempt_dir = pipeline_dir / f"version-{entry['version']}" / nested_attempt_dir_name
        if fallback_nested_attempt_dir.is_dir():
            return fallback_nested_attempt_dir

    return nested_attempt_dir
