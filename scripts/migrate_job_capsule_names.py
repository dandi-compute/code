"""
One-off migration of job capsule directories to the ``job-{YYMMDD}{hash}`` naming.

Renames every legacy capsule directory in the job capsules Dandiset (``001697``) and the
failed runs archive Dandiset (``001873``) to its job ID, and writes the ``DandiCompute``
provenance block into each capsule's ``dataset_description.json`` so the pipeline version,
codebase version, parameters and config the old name spelled out are preserved.

All legacy layouts are handled, with or without a trailing ``_attempt-N``::

    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}/params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}

The ``YYMMDD`` of each migrated capsule is taken from the modification date of its
``code/submit.sh``, so a capsule keeps the date it was originally prepared. Its hash is what
preparation computes for the same job, so a migrated job is never formed a second time.

This script is deliberately standalone. It imports nothing from ``dandi_compute_code``, so it
runs against whatever version of the package is (or is not) installed. It needs only the
standard library, plus the ``dandi`` command line client on PATH when applying.

Runs as a dry run by default, printing every planned rename and changing nothing. Pass
``--apply`` to perform the migration. Requires ``DANDI_API_KEY`` when applying.

Usage::

    python migrate_job_capsule_names.py
    python migrate_job_capsule_names.py --apply
    python migrate_job_capsule_names.py --apply --dandiset 001697
"""

import argparse
import collections
import datetime
import hashlib
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

_log = logging.getLogger("migrate_job_capsule_names")

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"
_ASSETS_JSONLD_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/dandisets/{dandiset_id}/draft/assets.jsonld"

#: Key under which job provenance is written into a capsule's ``dataset_description.json``.
_PROVENANCE_KEY = "DandiCompute"

#: A capsule directory that already carries a job ID.
_JOB_ID_RE = re.compile(r"job-(?P<job_date>\d{6})(?P<job_hash>[0-9a-f]{6})")

#: A legacy capsule directory name. The config segment is absent on pipelines that have no
#: config, such as ``lfp``, and the attempt number is present on capsules formed before that
#: notion was retired.
_LEGACY_CAPSULE_DIR_RE = re.compile(
    r"(?:version-(?P<version_in_name>.+?)_codebase-(?P<codebase>[^_]+)_)?"
    r"params-(?P<params>[^_]+)(?:_config-(?P<config>[^_]+))?"
    r"(?:_attempt-(?P<attempt>\d+))?"
)


def compute_job_hash(
    *,
    dandiset_id: str,
    dandi_path: str,
    pipeline: str,
    version: str,
    params: str,
    config: str,
    content_id: str,
) -> str:
    """
    Six-character MD5 prefix over the fields that identify one job capsule.

    Kept byte-for-byte identical to ``dandi_compute_code.dandiset._job_id._compute_job_hash``.
    The codebase version is deliberately left out: a job is the same logical job no matter
    which release of the package formed it.
    """
    payload = "|".join([dandiset_id, dandi_path, pipeline, version, params, config, content_id])
    job_hash = hashlib.md5(payload.encode("utf-8")).hexdigest()[:6]
    return job_hash


def format_job_id(*, job_hash: str, date: datetime.date) -> str:
    """Build the ``job-{YYMMDD}{hash}`` directory name."""
    job_id = f"job-{date:%y%m%d}{job_hash}"
    return job_id


def fetch_assets(dandiset_id: str, /) -> list[dict]:
    """
    Fetch a Dandiset's draft ``assets.jsonld``.

    This is the only place the script touches the network for metadata, so tests can replace
    it wholesale.

    :return: The raw asset dicts, or an empty list when the metadata cannot be read.
    :rtype: list[dict]
    """
    url = _ASSETS_JSONLD_URL_TEMPLATE.format(dandiset_id=dandiset_id)
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            assets = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exception:
        _log.warning("Unable to load metadata from %s: %s", url, exception)
        return []

    if not isinstance(assets, list):
        _log.warning("Expected a JSON array from %s, got %s", url, type(assets).__name__)
        return []
    return [asset for asset in assets if isinstance(asset, dict)]


def _content_id_of(asset: dict, /) -> str:
    """Pull the blob or zarr content ID out of an asset's content URLs."""
    content_urls = asset.get("contentUrl")
    for url in content_urls if isinstance(content_urls, list) else []:
        if isinstance(url, str) and ("/blobs/" in url or "/zarr/" in url):
            return url.rstrip("/").rsplit("/", 1)[-1].split("?", 1)[0]
    return ""


class _SourceContentIds:
    """Per-run cache of source content IDs, keyed by upstream Dandiset."""

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, str]] = {}

    def get(self, *, dandiset_id: str, dandi_path: str) -> str:
        if dandiset_id not in self._cache:
            self._cache[dandiset_id] = {
                asset["path"]: _content_id_of(asset)
                for asset in fetch_assets(dandiset_id)
                if isinstance(asset.get("path"), str)
            }
        content_id = self._cache[dandiset_id].get(dandi_path, "")
        if not content_id:
            _log.warning(
                "No upstream asset for dandiset-%s %s; hashing with an empty content ID",
                dandiset_id,
                dandi_path,
            )
        return content_id


def parse_legacy_capsule(asset_path: str, /) -> tuple[str, dict] | None:
    """
    Parse one asset path into its legacy capsule path and the identity its name spells out.

    :return: ``(capsule_path, identity)``, or ``None`` when the path is not inside a legacy
        job capsule directory.
    :rtype: tuple[str, dict] or None
    """
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
    if _JOB_ID_RE.fullmatch(parts[capsule_index]) is not None:
        return None

    match = _LEGACY_CAPSULE_DIR_RE.fullmatch(parts[capsule_index])
    version_from_parent = ""
    if match is None:
        # The nested layout puts the version in its own directory above the capsule.
        if not parts[capsule_index].startswith("version-") or capsule_index + 1 >= len(parts):
            return None
        version_from_parent = parts[capsule_index][len("version-") :]
        capsule_index += 1
        match = _LEGACY_CAPSULE_DIR_RE.fullmatch(parts[capsule_index])
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


def find_legacy_capsules(*, dandiset_id: str) -> dict[str, dict]:
    """
    Find every legacy job capsule directory in *dandiset_id*.

    :return: Capsule path (relative to the Dandiset root) mapped to its parsed identity, each
        carrying the ``code/submit.sh`` timestamp as ``submit_date``.
    :rtype: dict[str, dict]
    """
    capsules: dict[str, dict] = {}
    submit_dates: dict[str, str] = {}

    for asset in fetch_assets(dandiset_id):
        asset_path = asset.get("path")
        if not isinstance(asset_path, str):
            continue
        parsed = parse_legacy_capsule(asset_path)
        if parsed is None:
            continue
        capsule_path, identity = parsed
        capsules.setdefault(capsule_path, identity)
        if asset_path == f"{capsule_path}/code/submit.sh":
            date_modified = asset.get("dateModified")
            if isinstance(date_modified, str):
                submit_dates[capsule_path] = date_modified

    for capsule_path, identity in capsules.items():
        identity["submit_date"] = submit_dates.get(capsule_path, "")
    return capsules


def build_job_id(*, identity: dict, content_id: str) -> str:
    """Build the job ID a legacy capsule migrates to, dated by its ``code/submit.sh``."""
    job_hash = compute_job_hash(
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
    job_id = format_job_id(job_hash=job_hash, date=date)
    return job_id


def plan_migration(*, dandiset_id: str) -> list[tuple[str, str, dict]]:
    """
    Build the list of renames for *dandiset_id*.

    Capsules that map onto the same job ID are left out. That happens when two legacy capsules
    describe the same logical job and differ only in the codebase version, which the job hash
    deliberately ignores. Migrating both would merge them into one directory, so they are
    reported and skipped for a human to resolve.

    :return: ``(old_capsule_path, new_capsule_path, identity)`` triples, sorted by old path.
    :rtype: list[tuple[str, str, dict]]
    """
    capsules = find_legacy_capsules(dandiset_id=dandiset_id)
    source_content_ids = _SourceContentIds()

    candidates: list[tuple[str, str, dict]] = []
    for capsule_path, identity in sorted(capsules.items()):
        content_id = source_content_ids.get(
            dandiset_id=identity["dandiset_id"],
            dandi_path=identity["dandi_path"],
        )
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


def write_provenance(*, capsule_dir: pathlib.Path, identity: dict) -> None:
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


def migrate_capsule(*, dandiset_id: str, old_path: str, new_path: str, identity: dict) -> None:
    """
    Rename one capsule in place on the archive.

    The capsule subtree is downloaded, copied to its new path with the provenance block written
    into ``dataset_description.json``, uploaded, and only then deleted from its old path, so a
    failed upload never destroys the original.
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
        write_provenance(capsule_dir=target_dir, identity=identity)

        _run(["dandi", "upload", "--allow-any-path", new_path], cwd=dandiset_root)
        _run(["dandi", "delete", str(source_dir)], input_text="y\n")
    finally:
        shutil.rmtree(working_root, ignore_errors=True)


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
