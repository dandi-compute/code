"""
One-off migration of job capsule directories to the ``job-{YYMMDD}{hash}`` naming.

Works against local Dandiset clones sitting next to each other. The archive has no notion of
renaming a path, so a migration is a copy, an upload of the copies, and a delete of the
originals once you have confirmed the result. Four phases, run in order::

    python migrate_job_capsule_names.py plan     # what would be copied; changes nothing
    python migrate_job_capsule_names.py copy     # copy on disk, write a manifest
    python migrate_job_capsule_names.py upload   # batch-upload the new paths
    python migrate_job_capsule_names.py clean    # delete the legacy paths

``copy`` is purely local. It copies each legacy capsule directory to its job ID name and writes
the ``DandiCompute`` provenance block into the copy's ``dataset_description.json``, so the
pipeline version, codebase version, parameters and config the old name spelled out are
preserved. The legacy directory is left exactly as it was, so the clone stays a complete mirror
of the archive until ``clean``. It records every copy in a manifest next to the clones, which
the later phases read, so you can inspect or edit it before anything reaches the archive.

Because the capsules exist twice on disk between ``copy`` and ``clean``, the clone needs room
for a second copy of every capsule being migrated.

``upload`` pushes only the new paths, batched per Dandiset. Nothing is deleted at this point,
so the archive briefly carries both names. Check that the new capsules look right, then run
``clean`` to remove the legacy paths from the archive and prune the emptied local parents.

``copy`` can be run repeatedly. Each run adds what it copied to the manifest rather than
replacing it, and a capsule whose copy an earlier run already made is recorded again rather
than skipped, so a lost or truncated manifest is rebuilt by re-running the phase.

This is a one-off migration and stands entirely alone. It shells out to ``dandi`` for the two
archive-facing phases and reads nothing but the clones themselves; it never invokes
``dandicompute`` and never reads or writes a ``state.tsv``.

All legacy layouts are handled, with or without a trailing ``_attempt-N``::

    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}/params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}

The ``YYMMDD`` of each migrated capsule comes from the modification time of its
``code/submit.sh``, so a capsule keeps the date it was originally prepared. Its hash is what
preparation computes for the same job, so a migrated job is never formed a second time.

This script is deliberately standalone. It imports nothing from ``dandi_compute_code``, so it
runs against whatever version of the package is (or is not) installed. It needs only the
standard library, plus the ``dandi`` command line client on PATH for ``upload`` and ``clean``.

The clones are expected to sit under ``--root`` (the working directory by default), one
directory per Dandiset, as ``dandi download`` lays them out::

    ./001697/derivatives/...
    ./001873/derivatives/...
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
import time

_log = logging.getLogger("migrate_job_capsule_names")

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"

#: Where ``copy`` records what it did, and what ``upload`` and ``clean`` read back.
_MANIFEST_NAME = "job-capsule-migration.json"

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

#: How many paths to hand a single ``dandi`` invocation.
_BATCH_SIZE = 100


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


def read_content_id(capsule_dir: pathlib.Path, /) -> str:
    """
    Read the source asset's content ID out of a capsule's ``code/submit.sh``.

    The submission script points at the source blob by path, whose file name is the content
    ID. Reading it here keeps the whole plan offline.
    """
    submission_script = capsule_dir / "code" / "submit.sh"
    if not submission_script.is_file():
        return ""
    for line in submission_script.read_text(errors="replace").splitlines():
        if line.startswith("NWB_FILE_PATH="):
            nwb_file_path = line.split("=", maxsplit=1)[1].strip().strip('"').strip("'")
            return pathlib.PurePosixPath(nwb_file_path).name
    return ""


def read_prepared_date(capsule_dir: pathlib.Path, /) -> datetime.date:
    """
    Read the date a capsule was prepared from its ``code/submit.sh`` modification time.

    ``dandi download`` preserves the archive's modification times, so a freshly downloaded
    clone carries the original preparation date. Falls back to today when the script is
    missing.
    """
    submission_script = capsule_dir / "code" / "submit.sh"
    if not submission_script.is_file():
        _log.warning("No code/submit.sh in %s; dating its job ID today", capsule_dir)
        return datetime.datetime.now(tz=datetime.timezone.utc).date()
    timestamp = submission_script.stat().st_mtime
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).date()


def parse_legacy_capsule(*, capsule_dir: pathlib.Path, dandiset_root: pathlib.Path) -> dict | None:
    """
    Parse a legacy capsule directory into the identity its name spells out.

    :param capsule_dir: The capsule directory inside the local clone.
    :param dandiset_root: The clone's root, i.e. the directory named after the Dandiset.
    :return: The identity, or ``None`` when *capsule_dir* is not a legacy job capsule.
    :rtype: dict or None
    """
    parts = capsule_dir.relative_to(dandiset_root).parts

    dandiset_index = next((index for index, part in enumerate(parts) if part.startswith("dandiset-")), None)
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
    if capsule_index + 1 != len(parts):
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
    return identity


class _Progress:
    """
    A one-line progress report on stderr, so a long scan is never a silent wait.

    Writes an updating line when stderr is a terminal, and periodic log lines otherwise, so
    piping the output to a file stays readable. Always reports what it is working on, which is
    what makes a stall diagnosable.
    """

    def __init__(self, label: str, /, *, interval: float = 0.4) -> None:
        self._label = label
        self._interval = interval
        self._count = 0
        self._started = time.monotonic()
        self._last_report = 0.0
        self._is_terminal = sys.stderr.isatty()

    def advance(self, detail: str = "", /) -> None:
        self._count += 1
        now = time.monotonic()
        if now - self._last_report < self._interval:
            return
        self._last_report = now
        self._write(detail)

    def _write(self, detail: str, /) -> None:
        elapsed = time.monotonic() - self._started
        message = f"{self._label}: {self._count} ({elapsed:.0f}s)"
        if detail:
            message = f"{message}  {detail}"
        if self._is_terminal:
            sys.stderr.write(f"\r\033[K{message[:160]}")
            sys.stderr.flush()
        else:
            _log.info("%s", message)

    def close(self, detail: str = "", /) -> int:
        """Finish the line and return the count reached."""
        self._last_report = 0.0
        self._write(detail)
        if self._is_terminal:
            sys.stderr.write("\n")
            sys.stderr.flush()
        return self._count


def find_pipeline_directories(dandiset_root: pathlib.Path, /) -> list[pathlib.Path]:
    """
    Find every ``pipeline-*`` directory in a local clone.

    The walk is pruned at each ``pipeline-*`` directory, so it never descends into the
    capsules themselves. That matters: a capsule's ``derivatives`` and ``logs`` trees hold the
    bulk of the clone, and walking them would dominate the scan.

    :return: Pipeline directories, sorted.
    :rtype: list[pathlib.Path]
    """
    derivatives_root = dandiset_root / "derivatives"
    if not derivatives_root.is_dir():
        _log.warning("No derivatives directory under %s", dandiset_root)
        return []

    progress = _Progress(f"Scanning {dandiset_root.name} for pipeline directories")
    pipeline_dirs: list[pathlib.Path] = []
    for current, subdirectory_names, _ in os.walk(derivatives_root):
        current_dir = pathlib.Path(current)
        progress.advance(current_dir.name)
        if current_dir.name.startswith("pipeline-"):
            pipeline_dirs.append(current_dir)
            # Everything below is capsule contents; there is nothing to find down there.
            subdirectory_names.clear()

    progress.close(f"found {len(pipeline_dirs)} pipeline directories")
    return sorted(pipeline_dirs)


def _iter_capsule_candidates(pipeline_dir: pathlib.Path, /):
    """
    Yield the directories directly under *pipeline_dir* that could be job capsules.

    The nested layout puts capsules one level further down, under a bare ``version-``
    directory, so that one level is descended into and nothing else is.
    """
    for child in sorted(pipeline_dir.iterdir()):
        if not child.is_dir():
            continue
        is_capsule_name = (
            _JOB_ID_RE.fullmatch(child.name) is not None or _LEGACY_CAPSULE_DIR_RE.fullmatch(child.name) is not None
        )
        if not is_capsule_name and child.name.startswith("version-"):
            yield from (grandchild for grandchild in sorted(child.iterdir()) if grandchild.is_dir())
        else:
            yield child


def find_legacy_capsules(dandiset_root: pathlib.Path, /) -> list[pathlib.Path]:
    """
    Find every legacy job capsule directory in a local clone.

    :return: Capsule directories, sorted.
    :rtype: list[pathlib.Path]
    """
    capsule_dirs = []
    for pipeline_dir in find_pipeline_directories(dandiset_root):
        capsule_dirs.extend(_iter_capsule_candidates(pipeline_dir))
    return sorted(capsule_dirs)


def plan_migration(*, dandiset_root: pathlib.Path) -> list[dict]:
    """
    Build the list of copies for one local clone.

    Capsules that map onto the same job ID are left out. That happens when two legacy capsules
    describe the same logical job and differ only in the codebase version, which the job hash
    deliberately ignores. Renaming both onto one directory would merge them, so they are
    reported and skipped for a human to resolve.

    :return: One record per planned copy, each with ``old_path``, ``new_path``, ``job_id``
        and the parsed ``identity``. Paths are relative to *dandiset_root*.
    :rtype: list[dict]
    """
    capsule_dirs = find_legacy_capsules(dandiset_root)
    progress = _Progress(f"Examining {dandiset_root.name} capsules")
    already_migrated = 0
    unrecognised: list[pathlib.Path] = []

    candidates: list[dict] = []
    for capsule_dir in capsule_dirs:
        progress.advance(capsule_dir.name)
        if _JOB_ID_RE.fullmatch(capsule_dir.name) is not None:
            already_migrated += 1
            continue

        identity = parse_legacy_capsule(capsule_dir=capsule_dir, dandiset_root=dandiset_root)
        if identity is None:
            unrecognised.append(capsule_dir)
            continue

        identity["content_id"] = read_content_id(capsule_dir)
        if not identity["content_id"]:
            _log.warning("No content ID readable from %s; hashing with an empty content ID", capsule_dir)

        job_hash = compute_job_hash(
            dandiset_id=identity["dandiset_id"],
            dandi_path=identity["dandi_path"],
            pipeline=identity["pipeline"],
            version=identity["version"],
            params=identity["params"],
            config=identity["config"],
            content_id=identity["content_id"],
        )
        job_id = format_job_id(job_hash=job_hash, date=read_prepared_date(capsule_dir))
        identity["job_id"] = job_id
        candidates.append(
            {
                "old_path": capsule_dir.relative_to(dandiset_root).as_posix(),
                "new_path": f"{identity['pipeline_path']}/{job_id}",
                "job_id": job_id,
                "identity": identity,
            }
        )

    progress.close(
        f"{len(candidates)} to migrate, {already_migrated} already migrated, {len(unrecognised)} unrecognised"
    )
    for capsule_dir in unrecognised:
        _log.warning("Not a recognised job capsule, leaving alone: %s", capsule_dir)

    counts = collections.Counter(record["new_path"] for record in candidates)
    colliding = {new_path for new_path, count in counts.items() if count > 1}
    for new_path in sorted(colliding):
        _log.warning(
            "Skipping %d capsules that all map to %s. They are the same logical job and differ only in "
            "codebase version. Archive or delete all but one, then re-run.",
            counts[new_path],
            new_path,
        )
        for record in candidates:
            if record["new_path"] == new_path:
                _log.warning("  colliding capsule: %s", record["old_path"])

    plan = [record for record in candidates if record["new_path"] not in colliding]
    return plan


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


def copy_capsules(*, dandiset_root: pathlib.Path, plan: list[dict]) -> list[dict]:
    """
    Copy each planned capsule to its job ID name and write the copy's provenance block.

    A copy, not a move. The archive has no notion of renaming a path: the new capsule is
    uploaded as new assets and the legacy assets are deleted afterwards, so the legacy paths
    have to stay on disk and on the archive until that deletion is confirmed. Keeping them
    also leaves the clone a complete mirror of the archive while the copies are checked.

    Purely local. Nothing is uploaded or deleted here.

    :return: The records that were copied.
    :rtype: list[dict]
    """
    copied: list[dict] = []
    for record in plan:
        source_dir = dandiset_root / record["old_path"]
        target_dir = dandiset_root / record["new_path"]
        if not source_dir.is_dir():
            _log.warning("Skipping %s: no longer on disk", record["old_path"])
            continue
        if target_dir.exists():
            # An earlier run already made this copy. Record it again rather than skipping, so a
            # manifest that was lost or truncated picks the capsule back up.
            if (target_dir / "code" / "submit.sh").is_file():
                _log.info("Already copied, re-recording %s -> %s", record["old_path"], record["new_path"])
                copied.append(record)
            else:
                _log.warning(
                    "Skipping %s: %s exists but does not look like a capsule; remove it and re-run",
                    record["old_path"],
                    record["new_path"],
                )
            continue

        target_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_dir, target_dir)
        write_provenance(capsule_dir=target_dir, identity=record["identity"])
        _log.info("Copied %s -> %s", record["old_path"], record["new_path"])
        copied.append(record)

    return copied


def _remove_empty_parents(*, start: pathlib.Path, stop: pathlib.Path) -> None:
    """Remove empty directories from *start* upwards, stopping below *stop*."""
    if stop not in start.parents:
        return
    current = start
    while current != stop:
        if not current.is_dir():
            break
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _batched(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _run(command: list[str], *, cwd: pathlib.Path | None = None, input_text: str | None = None) -> None:
    """Run a subprocess, raising with its output when it fails."""
    result = subprocess.run(command, capture_output=True, text=True, cwd=cwd, input=input_text)
    if result.returncode != 0:
        message = f"command failed: {' '.join(command)}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)


def upload_new_paths(*, dandiset_root: pathlib.Path, new_paths: list[str]) -> None:
    """Upload the copied capsules, in batches, from the clone's root."""
    for batch in _batched(new_paths, _BATCH_SIZE):
        _log.info("Uploading %d path(s) from %s", len(batch), dandiset_root)
        _run(["dandi", "upload", "--allow-any-path", *batch], cwd=dandiset_root)


def delete_legacy_paths(*, dandiset_id: str, old_paths: list[str]) -> None:
    """Delete the legacy capsule paths from the archive, in batches."""
    for batch in _batched(old_paths, _BATCH_SIZE):
        urls = [f"dandi://dandi/{dandiset_id}/{old_path}/" for old_path in batch]
        _log.info("Deleting %d legacy path(s) from dandiset-%s", len(batch), dandiset_id)
        _run(["dandi", "delete", *urls], input_text="y\n")


def manifest_path(root: pathlib.Path, /) -> pathlib.Path:
    return root / _MANIFEST_NAME


def load_manifest(root: pathlib.Path, /) -> dict:
    """Read the manifest written by ``copy``, or raise when it is missing."""
    path = manifest_path(root)
    if not path.is_file():
        message = f"No migration manifest at {path}. Run the `copy` phase first."
        raise RuntimeError(message)
    return json.loads(path.read_text())


def _read_manifest_for_update(root: pathlib.Path, /) -> dict:
    """
    Read an existing manifest so a re-run can add to it, or start a fresh one.

    ``copy`` is routinely run more than once. Writing only the current run's records would drop
    the earlier ones, leaving capsules copied on disk that ``upload`` and ``clean`` no longer
    know about. An unreadable manifest is moved aside rather than overwritten in place.
    """
    path = manifest_path(root)
    if not path.is_file():
        return {"dandisets": {}}

    try:
        manifest = json.loads(path.read_text())
    except json.JSONDecodeError:
        backup_path = path.with_suffix(f".{int(time.time())}.corrupt.json")
        path.rename(backup_path)
        _log.warning("Unreadable manifest at %s; moved to %s and starting a new one", path, backup_path)
        return {"dandisets": {}}

    manifest.setdefault("dandisets", {})
    return manifest


def _merged_records(*, existing: list[dict], added: list[dict]) -> list[dict]:
    """Append *added* to *existing*, dropping any record whose capsule is already recorded."""
    known_new_paths = {record["new_path"] for record in existing}
    return existing + [record for record in added if record["new_path"] not in known_new_paths]


def _resolve_dandiset_root(*, root: pathlib.Path, dandiset_id: str) -> pathlib.Path | None:
    dandiset_root = root / dandiset_id
    if not dandiset_root.is_dir():
        _log.warning("No local clone at %s; skipping dandiset-%s", dandiset_root, dandiset_id)
        return None
    return dandiset_root


def _print_plan(*, dandiset_id: str, plan: list[dict]) -> None:
    print(f"\ndandiset-{dandiset_id}: {len(plan)} legacy job capsule(s)")
    for record in plan:
        print(f"  {record['old_path']}\n    -> {record['new_path']}")


def _phase_plan(*, root: pathlib.Path, dandiset_ids: list[str]) -> int:
    for dandiset_id in dandiset_ids:
        dandiset_root = _resolve_dandiset_root(root=root, dandiset_id=dandiset_id)
        if dandiset_root is None:
            continue
        _print_plan(dandiset_id=dandiset_id, plan=plan_migration(dandiset_root=dandiset_root))
    print("\nNothing was changed. Run the `copy` phase to apply this on disk.")
    return 0


def _phase_copy(*, root: pathlib.Path, dandiset_ids: list[str]) -> int:
    manifest = _read_manifest_for_update(root)
    manifest["generated_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    manifest["root"] = str(root)

    copied_now = 0
    for dandiset_id in dandiset_ids:
        dandiset_root = _resolve_dandiset_root(root=root, dandiset_id=dandiset_id)
        if dandiset_root is None:
            continue
        plan = plan_migration(dandiset_root=dandiset_root)
        copied = copy_capsules(dandiset_root=dandiset_root, plan=plan)
        copied_now += len(copied)
        manifest["dandisets"][dandiset_id] = _merged_records(
            existing=manifest["dandisets"].get(dandiset_id, []), added=copied
        )
        _print_plan(dandiset_id=dandiset_id, plan=copied)

    manifest_path(root).write_text(json.dumps(manifest, indent=2) + "\n")
    total = sum(len(records) for records in manifest["dandisets"].values())
    print(f"\nCopied {copied_now} job capsule(s) on disk ({total} recorded in total).")
    print(f"Manifest: {manifest_path(root)}")
    print("The legacy paths are untouched. Review the copies, then run the `upload` phase.")
    return 0


def _phase_upload(*, root: pathlib.Path, dandiset_ids: list[str]) -> int:
    manifest = load_manifest(root)
    uploaded_total = 0
    for dandiset_id in dandiset_ids:
        records = manifest["dandisets"].get(dandiset_id, [])
        if not records:
            continue
        dandiset_root = _resolve_dandiset_root(root=root, dandiset_id=dandiset_id)
        if dandiset_root is None:
            continue
        upload_new_paths(dandiset_root=dandiset_root, new_paths=[record["new_path"] for record in records])
        uploaded_total += len(records)

    if uploaded_total == 0:
        print("\nNothing to upload: the manifest records no copied capsules.")
        return 0

    print(f"\nUploaded {uploaded_total} job capsule(s).")
    print("The archive now carries both the new and the legacy paths. Check the new capsules,")
    print("then run the `clean` phase to remove the legacy structure.")
    return 0


def _phase_clean(*, root: pathlib.Path, dandiset_ids: list[str]) -> int:
    manifest = load_manifest(root)
    deleted_total = 0
    for dandiset_id in dandiset_ids:
        records = manifest["dandisets"].get(dandiset_id, [])
        if not records:
            continue
        delete_legacy_paths(dandiset_id=dandiset_id, old_paths=[record["old_path"] for record in records])
        deleted_total += len(records)

        dandiset_root = _resolve_dandiset_root(root=root, dandiset_id=dandiset_id)
        if dandiset_root is None:
            continue
        for record in records:
            legacy_dir = dandiset_root / record["old_path"]
            if legacy_dir.is_dir():
                shutil.rmtree(legacy_dir)
            _remove_empty_parents(start=legacy_dir.parent, stop=dandiset_root / "derivatives")

    if deleted_total == 0:
        print("\nNothing to delete: the manifest records no copied capsules.")
        return 0

    print(f"\nDeleted {deleted_total} legacy job capsule path(s). The migration is complete.")
    return 0


_PHASES = {
    "plan": _phase_plan,
    "copy": _phase_copy,
    "upload": _phase_upload,
    "clean": _phase_clean,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("phase", choices=list(_PHASES), help="Which phase to run. See the module docstring.")
    parser.add_argument(
        "--root",
        type=pathlib.Path,
        default=pathlib.Path.cwd(),
        help="Directory holding the local Dandiset clones. Defaults to the working directory.",
    )
    parser.add_argument(
        "--dandiset",
        dest="dandiset_ids",
        action="append",
        default=None,
        help="Dandiset to migrate. Repeatable. Defaults to both 001697 and 001873.",
    )
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    root = arguments.root.expanduser().resolve()
    dandiset_ids = arguments.dandiset_ids or [_JOB_CAPSULES_DANDISET_ID, _FAILED_RUNS_ARCHIVE_DANDISET_ID]

    if arguments.phase in ("upload", "clean") and not os.environ.get("DANDI_API_KEY", "").strip():
        _log.error("`DANDI_API_KEY` environment variable is not set or is blank.")
        return 1

    try:
        return _PHASES[arguments.phase](root=root, dandiset_ids=dandiset_ids)
    except RuntimeError as exception:
        _log.error("%s", exception)
        return 1


if __name__ == "__main__":
    sys.exit(main())
