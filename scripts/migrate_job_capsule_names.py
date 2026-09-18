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

Every phase can be re-run. ``plan`` changes nothing; ``copy`` makes no second copy and records
again what an earlier run already copied, so a lost or truncated manifest is rebuilt by
re-running it; ``upload`` re-uploads paths the archive already has, which ``dandi`` treats as
unchanged; and ``clean`` skips legacy paths the archive no longer holds, so a second run
deletes nothing rather than failing on a path that is already gone.

There is a fifth phase, ``reconcile``, for repairing a migration that was carried out by an
earlier version of this script that moved the local directory instead of copying it::

    python migrate_job_capsule_names.py reconcile   # find legacy paths only the archive still has

Those capsules were uploaded under their job ID but their legacy paths were never deleted, and
the local legacy directory is gone, so ``plan`` cannot see them. ``reconcile`` reads the
archive's asset list and pairs each legacy capsule it still holds with a migrated capsule in
the same pipeline directory whose local provenance block describes the same job. Only a paired
legacy path is recorded, so ``clean`` never deletes one whose replacement is not up. It changes
nothing itself.

``clean`` does this same reconciliation before deleting, so those improperly named folders are
removed along with the ones this run copied, whether or not ``reconcile`` was run first. Pass
``--no-reconcile`` to delete only what the manifest records.

A sixth phase, ``refile``, puts back capsule files that were moved out of the clones while the
migration was under way::

    python migrate_job_capsule_names.py refile --from ../stray-outputs           # report only
    python migrate_job_capsule_names.py refile --from ../stray-outputs --apply   # move them

A file taken out of a clone keeps the capsule path it sat under, so the manifest says which
migrated copy it belongs in and where inside that copy. The tree holding the files can be
rooted anywhere: capsules are matched from the ``dandiset-`` segment onwards. A file whose
capsule the manifest does not record, or whose capsule path both Dandisets record and so cannot
be told apart, is left where it is and reported, and a file already present at its target is
never overwritten.

This is a one-off migration and stands entirely alone. It shells out to ``dandi`` for the two
archive-facing phases and reads nothing but the clones themselves; it never invokes
``dandicompute`` and never reads or writes a ``state.tsv``.

All legacy layouts are handled, with or without a trailing ``_attempt-N``::

    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}/params-{params}_config-{config}
    pipeline-{pipeline}/version-{version}_codebase-{codebase}_params-{params}

The ``YYMMDD`` of each migrated capsule is the date its job was submitted, read from the name
of the ``submitted_date-YYYY+MM+DD_time-...`` marker submission writes into the capsule. The
date being in the file's name is what makes it usable: it survives the capsule being
re-uploaded, where a modification time does not. A capsule that was never submitted falls back
to the modification time of its ``code/submit.sh``, which records when its files last landed
rather than when it was prepared, and ``plan`` reports how many capsules fell back.

Only the hash identifies the job, in any case: it is what preparation computes for the same
job, so a migrated job is never formed a second time.

Two capsules can still land on one name, when they are the same logical job prepared on the
same day: re-attempts, or runs differing only in codebase version, which the hash deliberately
ignores. The second and later are suffixed with a ``-2``, ``-3`` counter rather than skipped,
so every capsule migrates.

The plan assigns those counters by position, ordered by legacy path, so a clone plans the same
names every time. The copy does not trust that position, though: it recognises a capsule's own
copy by the ``migrated_from`` its provenance records, or by the manifest for a copy made before
that field existed, and takes the first counter not already on disk otherwise. A group that has
shrunk since the last run therefore cannot hand a capsule the directory holding a different
capsule's copy. A copy the manifest identifies has the missing field written into it, so the
pairing stops depending on the manifest surviving.

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
import itertools
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
import urllib.request

_log = logging.getLogger("migrate_job_capsule_names")

_JOB_CAPSULES_DANDISET_ID = "001697"
_FAILED_RUNS_ARCHIVE_DANDISET_ID = "001873"

#: Where the archive lists every asset path in a Dandiset's draft version.
_ASSETS_JSONLD_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/dandisets/{dandiset_id}/draft/assets.jsonld"

#: Where ``copy`` records what it did, and what ``upload`` and ``clean`` read back.
_MANIFEST_NAME = "job-capsule-migration.json"

#: Key under which job provenance is written into a capsule's ``dataset_description.json``.
_PROVENANCE_KEY = "DandiCompute"

#: A capsule directory that already carries a job ID, with the counter that distinguishes
#: capsules of one job prepared on one day. Kept in step with the package's own pattern.
_JOB_ID_RE = re.compile(r"job-(?P<job_date>\d{6})(?P<job_hash>[0-9a-f]{6})(?:-(?P<job_index>[2-9]|\d{2,}))?")

#: A legacy capsule directory name. The config segment is absent on pipelines that have no
#: config, such as ``lfp``, and the attempt number is present on capsules formed before that
#: notion was retired.
_LEGACY_CAPSULE_DIR_RE = re.compile(
    r"(?:version-(?P<version_in_name>.+?)_codebase-(?P<codebase>[^_]+)_)?"
    r"params-(?P<params>[^_]+)(?:_config-(?P<config>[^_]+))?"
    r"(?:_attempt-(?P<attempt>\d+))?"
)

#: The marker submission writes into a capsule's ``code/`` directory, carrying the submission
#: date in its name, e.g. ``submitted_date-2025+06+07_time-14+32+09``.
_SUBMITTED_MARKER_RE = re.compile(r"submitted_date-(?P<year>\d{4})\+(?P<month>\d{2})\+(?P<day>\d{2})")

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


def format_job_id(*, job_hash: str, date: datetime.date, index: int = 1) -> str:
    """
    Build the ``job-{YYMMDD}{hash}`` directory name.

    Kept in step with ``dandi_compute_code.dandiset._job_id._format_job_id``.

    :param index: Which capsule this is among those sharing the name. The first carries no
        counter, so the common case reads as ``job-260916a1b2c3``; later ones are suffixed
        ``-2``, ``-3`` and so on.
    """
    counter = "" if index <= 1 else f"-{index}"
    job_id = f"job-{date:%y%m%d}{job_hash}{counter}"
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
    Read the date a capsule's job was submitted, which is the date it was prepared.

    Submission writes a marker into the capsule named ``submitted_date-YYYY+MM+DD_time-...``,
    so the date is carried in the file's *name*. That is what makes it usable here: a name
    survives being re-uploaded, where a modification time does not. The earliest marker is
    taken, so a capsule re-submitted later keeps the date of its first run.

    Falls back to the modification time of ``code/submit.sh`` for a capsule that was never
    submitted, and to today when there is no submission script either. Both fallbacks record
    when the capsule's files last landed rather than when it was prepared, which for a capsule
    that has since been archived is the date it was archived.
    """
    code_dir = capsule_dir / "code"
    submitted_dates = []
    if code_dir.is_dir():
        for marker in code_dir.glob("submitted_date-*"):
            match = _SUBMITTED_MARKER_RE.match(marker.name)
            if match is not None:
                submitted_dates.append(datetime.date(int(match["year"]), int(match["month"]), int(match["day"])))
    if submitted_dates:
        return min(submitted_dates)

    submission_script = code_dir / "submit.sh"
    if not submission_script.is_file():
        _log.warning("No submission marker or code/submit.sh in %s; dating its job ID today", capsule_dir)
        return datetime.datetime.now(tz=datetime.timezone.utc).date()
    _log.debug("No submission marker in %s; dating its job ID from code/submit.sh", capsule_dir)
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
    return parse_legacy_capsule_parts(capsule_dir.relative_to(dandiset_root).parts)


def parse_legacy_capsule_parts(parts: tuple[str, ...], /) -> dict | None:
    """
    Parse the path segments of a legacy capsule into the identity its name spells out.

    Takes segments rather than a path so the same parser serves both a directory in the local
    clone and an asset path listed by the archive.

    :param parts: Path segments relative to the Dandiset root, ending at the capsule directory.
    :return: The identity, or ``None`` when *parts* do not describe a legacy job capsule.
    :rtype: dict or None
    """
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
    undated: list[pathlib.Path] = []
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
        if not any((capsule_dir / "code").glob("submitted_date-*")):
            undated.append(capsule_dir)
        prepared_date = read_prepared_date(capsule_dir)
        job_id = format_job_id(job_hash=job_hash, date=prepared_date)
        identity["job_id"] = job_id
        identity["old_path"] = capsule_dir.relative_to(dandiset_root).as_posix()
        candidates.append(
            {
                "old_path": capsule_dir.relative_to(dandiset_root).as_posix(),
                "new_path": f"{identity['pipeline_path']}/{job_id}",
                "job_id": job_id,
                "identity": identity,
                # Kept so a shared job ID can be re-formed with a counter once the whole clone
                # has been examined and the sharing is visible. Text, because the record is
                # written to the manifest as JSON.
                "job_hash": job_hash,
                "prepared_date": prepared_date.isoformat(),
            }
        )

    progress.close(
        f"{len(candidates)} to migrate, {already_migrated} already migrated, {len(unrecognised)} unrecognised"
    )
    if undated:
        _log.warning(
            "%d of %d capsules carry no submission marker; their job IDs are dated from when their files "
            "last landed rather than when they were submitted",
            len(undated),
            len(candidates),
        )
    for capsule_dir in unrecognised:
        _log.warning("Not a recognised job capsule, leaving alone: %s", capsule_dir)

    return _index_shared_job_ids(candidates)


def _index_shared_job_ids(candidates: list[dict], /) -> list[dict]:
    """
    Give each capsule sharing a job ID its own name by appending a counter.

    Capsules collide when they are the same logical job prepared on the same day: re-attempts,
    or runs that differ only in codebase version, which the hash deliberately ignores. Copying
    them onto one directory would merge them, so the second and later get a ``-2``, ``-3``
    suffix. Assignment is by legacy path, so the same clone always produces the same names.

    :return: The records, with ``job_id`` and ``new_path`` settled.
    :rtype: list[dict]
    """
    by_new_path = collections.defaultdict(list)
    for record in candidates:
        by_new_path[record["new_path"]].append(record)

    for new_path, records in sorted(by_new_path.items()):
        if len(records) == 1:
            continue
        _log.warning(
            "%d capsules map to %s. They are the same logical job prepared on the same day, so the "
            "second and later are suffixed with a counter.",
            len(records),
            new_path,
        )
        for index, record in enumerate(sorted(records, key=lambda record: record["old_path"]), start=1):
            job_id = format_job_id(
                job_hash=record["job_hash"],
                date=datetime.date.fromisoformat(record["prepared_date"]),
                index=index,
            )
            record["job_id"] = job_id
            record["identity"]["job_id"] = job_id
            record["new_path"] = f"{record['identity']['pipeline_path']}/{job_id}"
            _log.warning("  %s -> %s", record["old_path"], record["new_path"])

    return candidates


def write_provenance(*, capsule_dir: pathlib.Path, identity: dict) -> None:
    """
    Add the ``DandiCompute`` provenance block to a capsule's ``dataset_description.json``.

    The block records ``migrated_from``, the legacy path this copy was made from, which is what
    lets a later run recognise the copy as this capsule's rather than guessing from its name.
    """
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
        "migrated_from": identity.get("old_path", ""),
    }
    dataset_description_file.write_text(json.dumps(dataset_description, indent=2) + "\n")


def copy_capsules(
    *, dandiset_root: pathlib.Path, plan: list[dict], recorded_targets: dict[str, str] | None = None
) -> list[dict]:
    """
    Copy each planned capsule to its job ID name and write the copy's provenance block.

    A copy, not a move. The archive has no notion of renaming a path: the new capsule is
    uploaded as new assets and the legacy assets are deleted afterwards, so the legacy paths
    have to stay on disk and on the archive until that deletion is confirmed. Keeping them
    also leaves the clone a complete mirror of the archive while the copies are checked.

    Purely local. Nothing is uploaded or deleted here.

    :param recorded_targets: Legacy path to job ID path, as the manifest already records them.
        A copy made before the provenance carried ``migrated_from`` cannot be attributed from
        its own contents, so the manifest is what identifies it, and the copy is healed by
        having the field written into it.
    :return: The records that were copied.
    :rtype: list[dict]
    """
    recorded_targets = recorded_targets if recorded_targets is not None else {}
    copied: list[dict] = []
    for record in plan:
        source_dir = dandiset_root / record["old_path"]
        if not source_dir.is_dir():
            _log.warning("Skipping %s: no longer on disk", record["old_path"])
            continue

        job_id = _settle_job_id(dandiset_root=dandiset_root, record=record, recorded_targets=recorded_targets)
        if job_id is None:
            continue
        _apply_job_id(record, job_id)

        target_dir = dandiset_root / record["new_path"]
        if target_dir.exists():
            # An earlier run already made this copy. Record it again rather than skipping, so a
            # manifest that was lost or truncated picks the capsule back up.
            provenance = read_provenance(target_dir)
            if provenance is None or "migrated_from" not in provenance:
                _log.info("Recording where %s came from", record["new_path"])
                write_provenance(capsule_dir=target_dir, identity=record["identity"])
            _log.info("Already copied, re-recording %s -> %s", record["old_path"], record["new_path"])
            copied.append(record)
            continue

        target_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_dir, target_dir)
        write_provenance(capsule_dir=target_dir, identity=record["identity"])
        _log.info("Copied %s -> %s", record["old_path"], record["new_path"])
        copied.append(record)

    return copied


def _apply_job_id(record: dict, job_id: str, /) -> None:
    """Settle a record's job ID and the path that follows from it."""
    record["job_id"] = job_id
    record["identity"]["job_id"] = job_id
    record["new_path"] = f"{record['identity']['pipeline_path']}/{job_id}"


def _settle_job_id(*, dandiset_root: pathlib.Path, record: dict, recorded_targets: dict[str, str]) -> str | None:
    """
    Decide which job ID this capsule's copy takes, against what is already on disk.

    The counter that separates capsules sharing a job ID is assigned by position when the plan
    is built, so it depends on which legacy capsules the scan found. Trusting that position on
    a re-run is unsafe: if the set has changed since the copy was made, a capsule can be handed
    the name holding a *different* capsule's copy, be recorded as migrated, and have its legacy
    path deleted though its contents were never uploaded.

    So the copy settles the name itself. A capsule that already has a copy is recognised by the
    ``migrated_from`` its provenance records, whatever counter that copy ended up with, and
    anything else takes the first counter not already on disk.

    :return: The job ID to use, or ``None`` when a directory is in the way that cannot be told
        apart from a capsule.
    :rtype: str or None
    """
    pipeline_dir = dandiset_root / record["identity"]["pipeline_path"]

    # A copy the manifest already pairs with this capsule, made before the provenance recorded
    # where a copy came from. The manifest is the only thing that can attribute it.
    recorded_path = recorded_targets.get(record["old_path"])
    if recorded_path is not None and (dandiset_root / recorded_path).is_dir():
        return pathlib.PurePosixPath(recorded_path).name

    base_job_id = format_job_id(job_hash=record["job_hash"], date=datetime.date.fromisoformat(record["prepared_date"]))

    for index in itertools.count(start=1):
        job_id = format_job_id(
            job_hash=record["job_hash"], date=datetime.date.fromisoformat(record["prepared_date"]), index=index
        )
        candidate_dir = pipeline_dir / job_id
        if not candidate_dir.exists():
            return job_id

        provenance = read_provenance(candidate_dir)
        if provenance is not None and provenance.get("migrated_from") == record["old_path"]:
            return job_id
        if provenance is None and not (candidate_dir / "code" / "submit.sh").is_file():
            _log.warning(
                "Skipping %s: %s exists but does not look like a capsule; remove it and re-run",
                record["old_path"],
                f"{record['identity']['pipeline_path']}/{job_id}",
            )
            return None
        if provenance is not None and "migrated_from" not in provenance and job_id == base_job_id:
            # Copied by a version that did not record where a capsule came from. Only the
            # uncounted name can be attributed, since that is the one a single capsule takes.
            return job_id


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


def fetch_archive_capsule_names(dandiset_id: str, /) -> dict[str, set[str]]:
    """
    Read the capsule directory names the archive currently holds, per pipeline directory.

    The draft ``assets.jsonld`` lists every asset path in the Dandiset, which is enough to see
    which capsule directories exist without downloading anything.

    :return: Capsule directory names, keyed by the pipeline directory that holds them. Both are
        paths relative to the Dandiset root.
    :rtype: dict[str, set[str]]
    """
    url = _ASSETS_JSONLD_URL_TEMPLATE.format(dandiset_id=dandiset_id)
    _log.info("Reading the archive's asset list for dandiset-%s", dandiset_id)
    with urllib.request.urlopen(url) as response:
        assets = json.loads(response.read().decode("utf-8"))

    # The document is a JSON-LD object listing its assets under ``hasPart``, but accept a bare
    # list of assets too rather than depending on that framing.
    asset_entries = assets.get("hasPart", []) if isinstance(assets, dict) else assets

    capsule_names: dict[str, set[str]] = collections.defaultdict(set)
    for asset in asset_entries:
        if not isinstance(asset, dict):
            continue
        asset_path = asset.get("path", "")
        parts = pathlib.PurePosixPath(asset_path).parts
        pipeline_index = next((index for index, part in enumerate(parts) if part.startswith("pipeline-")), None)
        if pipeline_index is None or pipeline_index + 1 >= len(parts):
            continue
        pipeline_path = "/".join(parts[: pipeline_index + 1])
        capsule_names[pipeline_path].add(parts[pipeline_index + 1])

    return dict(capsule_names)


def read_provenance(capsule_dir: pathlib.Path, /) -> dict | None:
    """Read a capsule's ``DandiCompute`` provenance block, or ``None`` when it has none."""
    dataset_description_file = capsule_dir / "dataset_description.json"
    if not dataset_description_file.is_file():
        return None
    try:
        dataset_description = json.loads(dataset_description_file.read_text())
    except json.JSONDecodeError:
        return None
    provenance = dataset_description.get(_PROVENANCE_KEY)
    return provenance if isinstance(provenance, dict) else None


def _identity_key(identity: dict, /) -> tuple[str, str, str, str]:
    """The fields that decide whether a legacy name and a migrated capsule are the same job."""
    return (
        identity.get("version", ""),
        identity.get("codebase", ""),
        identity.get("params", ""),
        identity.get("config", ""),
    )


def archive_capsule_paths(archive_capsule_names: dict[str, set[str]], /) -> set[str]:
    """Flatten a per-pipeline listing into the set of capsule paths the archive holds."""
    return {
        f"{pipeline_path}/{capsule_name}"
        for pipeline_path, capsule_names in archive_capsule_names.items()
        for capsule_name in capsule_names
    }


def find_orphaned_legacy_paths(
    *,
    dandiset_root: pathlib.Path,
    dandiset_id: str,
    known_old_paths: set[str] | None = None,
    archive_capsule_names: dict[str, set[str]] | None = None,
) -> list[dict]:
    """
    Find legacy capsule paths the archive still holds whose migrated capsule is already up.

    This is the repair path for capsules migrated by an earlier run that moved the local
    directory instead of copying it. The legacy directory is gone locally, so ``plan`` cannot
    see it, but the archive still carries it alongside the uploaded job ID capsule.

    A legacy path is only reported when the archive also holds a job ID capsule in the same
    pipeline directory whose provenance block describes the same job, read from the local
    clone. That pairing is what makes deleting the legacy path safe, so a capsule whose
    migrated counterpart is missing or unreadable is left alone and reported.

    :param known_old_paths: Legacy paths already accounted for elsewhere, such as by the
        manifest. They are passed over silently, since the archive's asset list can lag a just
        finished upload and would otherwise report them as unpaired.
    :param archive_capsule_names: An already fetched listing, to save fetching it again.
    :return: One record per orphan, shaped like the records ``copy`` writes, so ``clean`` can
        delete them without knowing where they came from.
    :rtype: list[dict]
    """
    known_old_paths = known_old_paths if known_old_paths is not None else set()
    if archive_capsule_names is None:
        archive_capsule_names = fetch_archive_capsule_names(dandiset_id)
    progress = _Progress(f"Reconciling {dandiset_root.name} against the archive")
    orphans: list[dict] = []
    unpaired: list[str] = []

    for pipeline_path, capsule_names in sorted(archive_capsule_names.items()):
        progress.advance(pipeline_path.rsplit("/", maxsplit=1)[-1])
        legacy_names = [name for name in capsule_names if _JOB_ID_RE.fullmatch(name) is None]
        job_id_names = [name for name in capsule_names if _JOB_ID_RE.fullmatch(name) is not None]
        if not legacy_names or not job_id_names:
            continue

        provenance_by_identity = {}
        for job_id_name in job_id_names:
            provenance = read_provenance(dandiset_root / pipeline_path / job_id_name)
            if provenance is not None:
                provenance_by_identity[_identity_key(provenance)] = job_id_name

        for legacy_name in sorted(legacy_names):
            legacy_path = f"{pipeline_path}/{legacy_name}"
            if legacy_path in known_old_paths:
                continue
            identity = parse_legacy_capsule_parts(pathlib.PurePosixPath(legacy_path).parts)
            if identity is None:
                continue
            job_id_name = provenance_by_identity.get(_identity_key(identity))
            if job_id_name is None:
                unpaired.append(legacy_path)
                continue
            identity["job_id"] = job_id_name
            orphans.append(
                {
                    "old_path": legacy_path,
                    "new_path": f"{pipeline_path}/{job_id_name}",
                    "job_id": job_id_name,
                    "identity": identity,
                }
            )

    progress.close(f"{len(orphans)} paired with a migrated capsule, {len(unpaired)} left alone")
    for legacy_path in unpaired:
        _log.warning("No migrated capsule found for %s; leaving it on the archive", legacy_path)
    return orphans


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
    """
    Append *added* to *existing*, dropping any record for a legacy path already recorded.

    Keyed on the legacy path rather than the job ID, because several legacy paths can share one
    job ID: re-attempts of the same job differ only by a suffix the identity ignores, so each
    one is its own path to delete even though they all pair with the same migrated capsule.
    """
    known_old_paths = {record["old_path"] for record in existing}
    return existing + [record for record in added if record["old_path"] not in known_old_paths]


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
        copied = copy_capsules(
            dandiset_root=dandiset_root,
            plan=plan,
            recorded_targets={
                record["old_path"]: record["new_path"] for record in manifest["dandisets"].get(dandiset_id, [])
            },
        )
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


def _phase_clean(*, root: pathlib.Path, dandiset_ids: list[str], reconcile: bool = True) -> int:
    manifest = load_manifest(root)
    deleted_total = 0
    for dandiset_id in dandiset_ids:
        records = manifest["dandisets"].get(dandiset_id, [])
        dandiset_root = _resolve_dandiset_root(root=root, dandiset_id=dandiset_id)

        if reconcile and dandiset_root is not None:
            archive_capsule_names = fetch_archive_capsule_names(dandiset_id)

            # Legacy paths an earlier run left on the archive when it moved the local directory
            # instead of copying it. They are not in the manifest, because nothing local records
            # them, but they are exactly the improperly named folders this phase is here to
            # remove. Only ones paired with a migrated capsule already on the archive come back.
            orphans = find_orphaned_legacy_paths(
                dandiset_root=dandiset_root,
                dandiset_id=dandiset_id,
                known_old_paths={record["old_path"] for record in records},
                archive_capsule_names=archive_capsule_names,
            )
            records = _merged_records(existing=records, added=orphans)
            manifest["dandisets"][dandiset_id] = records

            # A path the archive no longer holds was deleted by an earlier run, so asking the
            # archive to delete it again would fail the phase. Re-running clean is then a no-op
            # rather than an error, and the manifest stays as the record of what was migrated.
            held_by_archive = archive_capsule_paths(archive_capsule_names)
            already_deleted = [record for record in records if record["old_path"] not in held_by_archive]
            if already_deleted:
                _log.info("%d legacy path(s) are already gone from the archive; skipping them", len(already_deleted))
            records = [record for record in records if record["old_path"] in held_by_archive]

        if not records:
            continue
        delete_legacy_paths(dandiset_id=dandiset_id, old_paths=[record["old_path"] for record in records])
        deleted_total += len(records)

        if dandiset_root is None:
            continue
        for record in records:
            legacy_dir = dandiset_root / record["old_path"]
            if legacy_dir.is_dir():
                shutil.rmtree(legacy_dir)
            _remove_empty_parents(start=legacy_dir.parent, stop=dandiset_root / "derivatives")

    manifest_path(root).write_text(json.dumps(manifest, indent=2) + "\n")
    if deleted_total == 0:
        print("\nNothing to delete: no legacy paths are recorded, and none were found on the archive.")
        return 0

    print(f"\nDeleted {deleted_total} legacy job capsule path(s). The migration is complete.")
    return 0


def _phase_reconcile(*, root: pathlib.Path, dandiset_ids: list[str]) -> int:
    manifest = _read_manifest_for_update(root)
    manifest["generated_at"] = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    manifest["root"] = str(root)

    found_now = 0
    for dandiset_id in dandiset_ids:
        dandiset_root = _resolve_dandiset_root(root=root, dandiset_id=dandiset_id)
        if dandiset_root is None:
            continue
        orphans = find_orphaned_legacy_paths(dandiset_root=dandiset_root, dandiset_id=dandiset_id)
        found_now += len(orphans)
        manifest["dandisets"][dandiset_id] = _merged_records(
            existing=manifest["dandisets"].get(dandiset_id, []), added=orphans
        )
        _print_plan(dandiset_id=dandiset_id, plan=orphans)

    manifest_path(root).write_text(json.dumps(manifest, indent=2) + "\n")
    total = sum(len(records) for records in manifest["dandisets"].values())
    print(f"\nFound {found_now} orphaned legacy path(s) ({total} recorded in total).")
    print(f"Manifest: {manifest_path(root)}")
    print("Nothing was uploaded or deleted. Review the manifest, then run the `clean` phase.")
    return 0


def _legacy_tail(path: str, /) -> str | None:
    """
    The part of a capsule path that identifies it regardless of where the tree is rooted.

    Everything from the ``dandiset-`` segment onwards, which is what a capsule path and a copy
    of one taken out of the clone still have in common.
    """
    parts = pathlib.PurePosixPath(path).parts
    dandiset_index = next((index for index, part in enumerate(parts) if part.startswith("dandiset-")), None)
    if dandiset_index is None:
        return None
    return "/".join(parts[dandiset_index:])


def index_manifest_by_legacy_tail(manifest: dict, /) -> dict[str, list[tuple[str, str]]]:
    """
    Index the manifest so a capsule can be found from a path rooted anywhere.

    :return: Legacy tail to the ``(dandiset_id, new_path)`` pairs recording it. A tail can be
        recorded by both Dandisets, since the failed runs archive holds capsules under the
        paths they had in the job capsules Dandiset.
    :rtype: dict[str, list[tuple[str, str]]]
    """
    index: dict[str, list[tuple[str, str]]] = collections.defaultdict(list)
    for dandiset_id, records in manifest.get("dandisets", {}).items():
        for record in records:
            tail = _legacy_tail(record["old_path"])
            if tail is not None:
                index[tail].append((dandiset_id, record["new_path"]))
    return dict(index)


def _split_at_capsule_contents(parts: tuple[str, ...], /) -> tuple[str, str] | None:
    """
    Split a path into the capsule that held a file and the file's path inside that capsule.

    The capsule directory is what follows the ``pipeline-`` segment, and its contents start at
    the ``derivatives``, ``logs``, ``code`` or ``intermediate`` directory beneath it.
    """
    pipeline_index = next((index for index, part in enumerate(parts) if part.startswith("pipeline-")), None)
    if pipeline_index is None:
        return None
    contents_index = next(
        (
            index
            for index in range(pipeline_index + 1, len(parts))
            if parts[index] in ("derivatives", "logs", "code", "intermediate")
        ),
        None,
    )
    if contents_index is None or contents_index == pipeline_index + 1:
        return None
    return "/".join(parts[:contents_index]), "/".join(parts[contents_index:])


def plan_refiling(*, source_dir: pathlib.Path, manifest: dict) -> tuple[list[dict], list[pathlib.Path]]:
    """
    Work out where files taken out of legacy capsules belong under the job ID naming.

    Files moved out of a clone keep the capsule path they sat under, so the manifest can say
    which copy each one belongs in. A file whose capsule the manifest does not record, or whose
    capsule path is recorded by both Dandisets and so cannot be told apart, is left alone.

    :return: The planned moves, each with ``source``, ``dandiset_id`` and ``target`` relative to
        that Dandiset's clone, and the files nothing could be decided for.
    :rtype: tuple[list[dict], list[pathlib.Path]]
    """
    index = index_manifest_by_legacy_tail(manifest)
    progress = _Progress(f"Scanning {source_dir.name}")
    moves: list[dict] = []
    undecided: list[pathlib.Path] = []

    for current, _, file_names in os.walk(source_dir):
        current_dir = pathlib.Path(current)
        progress.advance(current_dir.name)
        for file_name in sorted(file_names):
            source_file = current_dir / file_name
            split = _split_at_capsule_contents(source_file.relative_to(source_dir).parts)
            if split is None:
                undecided.append(source_file)
                continue
            capsule_path, inside_capsule = split

            tail = _legacy_tail(capsule_path)
            recorded = index.get(tail) if tail is not None else None
            if recorded is None:
                undecided.append(source_file)
                continue
            if len({new_path for _, new_path in recorded}) > 1 or len(recorded) > 1:
                _log.warning("%s belongs to a capsule recorded by more than one Dandiset; leaving it", source_file)
                undecided.append(source_file)
                continue

            dandiset_id, new_path = recorded[0]
            moves.append(
                {
                    "source": source_file,
                    "dandiset_id": dandiset_id,
                    "target": f"{new_path}/{inside_capsule}",
                }
            )

    progress.close(f"{len(moves)} file(s) placed, {len(undecided)} undecided")
    return moves, undecided


def refile_outputs(*, root: pathlib.Path, moves: list[dict]) -> int:
    """
    Move each planned file into the capsule copy it belongs to.

    A file already present at the target is left where it is rather than overwritten, since the
    copy is the migrated capsule and what is in it was put there deliberately.

    :return: How many files were moved.
    :rtype: int
    """
    moved = 0
    for move in moves:
        target_file = root / move["dandiset_id"] / move["target"]
        if target_file.exists():
            _log.warning("Leaving %s: %s already exists", move["source"], target_file)
            continue
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(move["source"]), str(target_file))
        _log.info("Moved %s -> %s", move["source"], target_file)
        moved += 1
    return moved


def _phase_refile(*, root: pathlib.Path, dandiset_ids: list[str], source_dir: pathlib.Path | None, apply: bool) -> int:
    if source_dir is None:
        message = "The `refile` phase needs `--from`, the directory holding the files to put back."
        raise RuntimeError(message)
    if not source_dir.is_dir():
        message = f"No directory at {source_dir}."
        raise RuntimeError(message)

    manifest = load_manifest(root)
    moves, undecided = plan_refiling(source_dir=source_dir, manifest=manifest)
    moves = [move for move in moves if move["dandiset_id"] in dandiset_ids]

    for move in moves:
        print(f"  {move['source']}\n    -> {root / move['dandiset_id'] / move['target']}")
    for source_file in undecided:
        _log.warning("Nothing in the manifest places %s", source_file)

    if not apply:
        print(f"\n{len(moves)} file(s) would be moved, {len(undecided)} left alone. Nothing was changed.")
        print("Re-run with `--apply` to move them.")
        return 0

    moved = refile_outputs(root=root, moves=moves)
    print(f"\nMoved {moved} file(s) into their migrated capsules, {len(undecided)} left alone.")
    if moved != len(moves):
        print("Some were left because a file already sat at the target; they are logged above.")
    return 0


_PHASES = {
    "plan": _phase_plan,
    "copy": _phase_copy,
    "reconcile": _phase_reconcile,
    "upload": _phase_upload,
    "clean": _phase_clean,
    "refile": _phase_refile,
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
        "--no-reconcile",
        dest="reconcile",
        action="store_false",
        help=(
            "For the `clean` phase: do not also look on the archive for improperly named legacy "
            "paths left behind by an earlier migration. Deletes only what the manifest records."
        ),
    )
    parser.add_argument(
        "--from",
        dest="source_dir",
        type=pathlib.Path,
        default=None,
        help=(
            "For the `refile` phase: the directory holding capsule files that were moved out of "
            "the clones, to be put back into the capsules they belong to."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="For the `refile` phase: move the files. Without it the phase only reports what it would do.",
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

    phase_arguments = {"root": root, "dandiset_ids": dandiset_ids}
    if arguments.phase == "clean":
        phase_arguments["reconcile"] = arguments.reconcile
    if arguments.phase == "refile":
        source_dir = arguments.source_dir
        phase_arguments["source_dir"] = source_dir.expanduser().resolve() if source_dir is not None else None
        phase_arguments["apply"] = arguments.apply

    try:
        return _PHASES[arguments.phase](**phase_arguments)
    except RuntimeError as exception:
        _log.error("%s", exception)
        return 1


if __name__ == "__main__":
    sys.exit(main())
