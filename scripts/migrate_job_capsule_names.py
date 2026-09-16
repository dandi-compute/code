"""
One-off migration of job capsule directories to the ``job-{YYMMDD}+{hash}`` naming.

Renames every legacy capsule directory in the job capsules Dandiset (``001697``) and the
failed runs archive Dandiset (``001873``) to its job ID, and writes the ``DandiCompute``
provenance block into each capsule's ``dataset_description.json`` so the pipeline version,
codebase version, parameters and config the old name spelled out are preserved.

All three legacy layouts are handled, with or without a trailing ``_attempt-N``::

    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}/params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}

The ``YYMMDD`` of each migrated capsule is taken from the modification date of its
``code/submit.sh``, so a capsule keeps the date it was originally prepared.

Runs as a dry run by default, printing every planned rename and changing nothing. Pass
``--apply`` to perform the migration. Requires ``DANDI_API_KEY``.

Usage::

    python scripts/migrate_job_capsule_names.py
    python scripts/migrate_job_capsule_names.py --apply
    python scripts/migrate_job_capsule_names.py --apply --dandiset 001697
"""

import argparse
import collections
import datetime
import json
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

from dandi_compute_code.dandiset import load_assets_jsonld_metadata
from dandi_compute_code.dandiset._globals import (
    _FAILED_RUNS_ARCHIVE_DANDISET_ID,
    _JOB_CAPSULES_DANDISET_ID,
    _LEGACY_JOB_CAPSULE_DIR_RE,
)
from dandi_compute_code.dandiset._job_id import _PROVENANCE_KEY, _compute_job_hash, _format_job_id
from dandi_compute_code.queue._queue_utils import _load_upstream_assets_jsonld_metadata

_log = logging.getLogger("migrate_job_capsule_names")


def find_legacy_capsules(*, dandiset_id: str) -> dict[str, dict]:
    """
    Find every legacy job capsule directory in *dandiset_id*.

    Walks the Dandiset's ``assets.jsonld`` and groups asset paths by the capsule directory
    they sit under, reading the identity straight out of each legacy directory name.

    :return: Capsule path (relative to the Dandiset root) mapped to its parsed identity.
    :rtype: dict[str, dict]
    """
    metadata = load_assets_jsonld_metadata(dandiset_id=dandiset_id)
    capsules: dict[str, dict] = {}
    submit_dates: dict[str, str] = collections.defaultdict(str)

    for asset_path, asset_metadata in metadata.path_to_asset_metadata.items():
        parsed = _parse_legacy_capsule(asset_path)
        if parsed is None:
            continue
        capsule_path, identity = parsed
        capsules.setdefault(capsule_path, identity)
        if asset_path == f"{capsule_path}/code/submit.sh":
            submit_dates[capsule_path] = asset_metadata.date_modified

    for capsule_path, identity in capsules.items():
        identity["submit_date"] = submit_dates.get(capsule_path) or ""
    return capsules


def _parse_legacy_capsule(asset_path: str, /) -> tuple[str, dict] | None:
    """Parse one asset path into its legacy capsule path and identity, or ``None``."""
    parts = pathlib.PurePosixPath(asset_path).parts

    dandiset_index = next(
        (index for index, part in enumerate(parts) if part.startswith("dandiset-")),
        None,
    )
    if dandiset_index is None:
        return None

    pipeline_index = next(
        (index for index in range(dandiset_index + 1, len(parts)) if parts[index].startswith("pipeline-")),
        None,
    )
    if pipeline_index is None or pipeline_index <= dandiset_index + 1 or pipeline_index + 1 >= len(parts):
        return None

    capsule_index = pipeline_index + 1
    match = _LEGACY_JOB_CAPSULE_DIR_RE.fullmatch(parts[capsule_index])
    version_from_parent = ""
    if match is None:
        # The nested layout puts the version in its own directory above the capsule.
        if not parts[capsule_index].startswith("version-") or capsule_index + 1 >= len(parts):
            return None
        version_from_parent = parts[capsule_index][len("version-") :]
        capsule_index += 1
        match = _LEGACY_JOB_CAPSULE_DIR_RE.fullmatch(parts[capsule_index])
        if match is None:
            return None

    version = match.group("version_in_name") or version_from_parent
    if not version:
        return None

    identity = {
        "dandiset_id": parts[dandiset_index][len("dandiset-") :],
        "dandi_path": "/".join(parts[dandiset_index + 1 : pipeline_index]) + ".nwb",
        "pipeline": parts[pipeline_index][len("pipeline-") :],
        "version": version,
        "codebase": match.group("codebase") or "",
        "params": match.group("params"),
        # Pipelines with no config, such as ``lfp``, carry no config segment.
        "config": match.group("config") or "",
        "pipeline_path": "/".join(parts[: pipeline_index + 1]),
    }
    capsule_path = "/".join(parts[: capsule_index + 1])
    return capsule_path, identity


def build_job_id(*, identity: dict, content_id: str) -> str:
    """Build the job ID a legacy capsule migrates to, dated by its ``code/submit.sh``."""
    job_hash = _compute_job_hash(
        dandiset_id=identity["dandiset_id"],
        dandi_path=identity["dandi_path"],
        pipeline=identity["pipeline"],
        version=identity["version"],
        params=identity["params"],
        config=identity["config"],
        content_id=content_id,
    )
    submit_date = identity["submit_date"]
    date = (
        datetime.datetime.fromisoformat(submit_date.replace("Z", "+00:00")).date()
        if submit_date
        else datetime.datetime.now(tz=datetime.timezone.utc).date()
    )
    job_id = _format_job_id(job_hash=job_hash, date=date)
    return job_id


def resolve_content_id(*, identity: dict) -> str:
    """Resolve a capsule's source content ID from its upstream Dandiset's ``assets.jsonld``."""
    upstream = _load_upstream_assets_jsonld_metadata(identity["dandiset_id"])
    source = upstream.path_to_asset_metadata.get(identity["dandi_path"])
    content_id = source.content_id if source is not None else ""
    if not content_id:
        _log.warning(
            "No upstream asset for dandiset-%s %s; hashing with an empty content ID",
            identity["dandiset_id"],
            identity["dandi_path"],
        )
    return content_id


def plan_migration(*, dandiset_id: str) -> list[tuple[str, str, dict]]:
    """
    Build the list of renames for *dandiset_id*.

    Capsules that map onto the same job ID are left out. That happens when two legacy
    capsules describe the same logical job and differ only in the codebase version, which the
    job hash deliberately ignores. Migrating both would merge them into one directory, so they
    are reported and skipped for a human to resolve.

    :return: ``(old_capsule_path, new_capsule_path, identity)`` triples, sorted by old path.
    :rtype: list[tuple[str, str, dict]]
    """
    capsules = find_legacy_capsules(dandiset_id=dandiset_id)
    candidates: list[tuple[str, str, dict]] = []
    for capsule_path, identity in sorted(capsules.items()):
        content_id = resolve_content_id(identity=identity)
        identity["content_id"] = content_id
        job_id = build_job_id(identity=identity, content_id=content_id)
        identity["job_id"] = job_id
        candidates.append((capsule_path, f"{identity['pipeline_path']}/{job_id}", identity))

    counts = collections.Counter(new_path for _, new_path, _ in candidates)
    colliding = {new_path for new_path, count in counts.items() if count > 1}
    for new_path in sorted(colliding):
        _log.warning(
            "Skipping %d capsules that all map to %s. They are the same logical job and differ only in "
            "codebase version. Archive or delete all but one, then re-run.",
            counts[new_path],
            new_path,
        )
        for old_path, candidate_path, _ in candidates:
            if candidate_path == new_path:
                _log.warning("  colliding capsule: %s", old_path)

    plan = [entry for entry in candidates if entry[1] not in colliding]
    return plan


def _run(command: list[str], *, cwd: pathlib.Path | None = None, input_text: str | None = None) -> None:
    """Run a subprocess, raising with its output when it fails."""
    result = subprocess.run(command, capture_output=True, text=True, cwd=cwd, input=input_text)
    if result.returncode != 0:
        message = f"command failed: {' '.join(command)}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)


def migrate_capsule(*, dandiset_id: str, old_path: str, new_path: str, identity: dict) -> None:
    """
    Rename one capsule in place on the archive.

    The capsule subtree is downloaded, copied to its new path with the provenance block
    written into ``dataset_description.json``, uploaded, and only then deleted from its old
    path, so a failed upload never destroys the original.
    """
    working_root = pathlib.Path(tempfile.mkdtemp(prefix="migrate-capsule-"))
    try:
        _run(["dandi", "download", "--preserve-tree", f"dandi://dandi/{dandiset_id}/{old_path}/"], cwd=working_root)
        _run(["dandi", "download", "--download", "dandiset.yaml", f"dandi://dandi/{dandiset_id}/"], cwd=working_root)

        dandiset_root = working_root / dandiset_id
        source_dir = dandiset_root / old_path
        target_dir = dandiset_root / new_path
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_dir, target_dir)
        _write_provenance(capsule_dir=target_dir, identity=identity)

        _run(["dandi", "upload", "--allow-any-path", new_path], cwd=dandiset_root)
        _run(["dandi", "delete", str(source_dir)], input_text="y\n")
    finally:
        shutil.rmtree(working_root, ignore_errors=True)


def _write_provenance(*, capsule_dir: pathlib.Path, identity: dict) -> None:
    """Add the ``DandiCompute`` provenance block to a capsule's ``dataset_description.json``."""
    dataset_description_file = capsule_dir / "dataset_description.json"
    dataset_description = {}
    if dataset_description_file.is_file():
        try:
            dataset_description = json.loads(dataset_description_file.read_text())
        except json.JSONDecodeError:
            _log.warning("Unreadable dataset_description.json in %s; writing a fresh one", capsule_dir)

    dataset_description[_PROVENANCE_KEY] = {
        "job_id": identity["job_id"],
        "dandiset_id": identity["dandiset_id"],
        "dandi_path": identity["dandi_path"],
        "content_id": identity["content_id"],
        "pipeline": identity["pipeline"],
        "version": identity["version"],
        "codebase": identity["codebase"],
        "params": identity["params"],
        "config": identity["config"],
    }
    dataset_description_file.write_text(json.dumps(dataset_description, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--dandiset",
        dest="dandiset_ids",
        action="append",
        default=None,
        help="Dandiset to migrate. Repeatable. Defaults to both 001697 and 001873.",
    )
    parser.add_argument("--apply", action="store_true", help="Perform the migration instead of a dry run.")
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    dandiset_ids = arguments.dandiset_ids or [_JOB_CAPSULES_DANDISET_ID, _FAILED_RUNS_ARCHIVE_DANDISET_ID]

    if arguments.apply and not os.environ.get("DANDI_API_KEY", "").strip():
        _log.error("`DANDI_API_KEY` environment variable is not set or is blank.")
        return 1

    migrated_total = 0
    for dandiset_id in dandiset_ids:
        plan = plan_migration(dandiset_id=dandiset_id)
        print(f"\ndandiset-{dandiset_id}: {len(plan)} legacy job capsule(s)")
        for old_path, new_path, _ in plan:
            print(f"  {old_path}\n    -> {new_path}")

        if not arguments.apply:
            continue
        for old_path, new_path, identity in plan:
            _log.info("Migrating %s -> %s", old_path, new_path)
            migrate_capsule(dandiset_id=dandiset_id, old_path=old_path, new_path=new_path, identity=identity)
            migrated_total += 1

    if arguments.apply:
        print(f"\nMigrated {migrated_total} job capsule(s).")
        print("Refresh the state tables with `dandicompute queue refresh` once this finishes.")
    else:
        print("\nDry run; nothing was changed. Re-run with --apply to perform the migration.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
