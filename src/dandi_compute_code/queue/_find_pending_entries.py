import logging

from ..dandiset._load_assets_jsonld_metadata import load_assets_jsonld_metadata

_log = logging.getLogger(__name__)


def _find_pending_entries() -> list[str]:
    """
    Identify attempt directories awaiting submission from the DANDI assets metadata.

    Loads the DANDI ``assets.jsonld`` metadata and collects every attempt
    directory that contains a ``code/submit.sh`` asset but no adjacent
    submitted-marker asset. An entry is considered submitted when a sibling
    ``submitted`` asset exists, or when a sibling asset whose name starts with
    ``submitted_date-`` exists.

    Returns
    -------
    list[str]
        Sorted list of ``code`` directory paths (relative to the Dandiset root)
        that are pending submission. Empty when nothing is awaiting submission.
    """
    metadata = load_assets_jsonld_metadata()
    paths = set(metadata.path_to_asset_metadata.keys())

    pending_entries: list[str] = []
    for asset_path in sorted(paths):
        if asset_path.endswith("/code/submit.sh"):
            code_dir_path = asset_path[: -len("/submit.sh")]
            submitted_marker_prefix = f"{code_dir_path}/submitted_date-"
            has_submitted_marker = any(
                path == f"{code_dir_path}/submitted" or path.startswith(submitted_marker_prefix) for path in paths
            )
            if not has_submitted_marker:
                pending_entries.append(code_dir_path)

    return pending_entries
