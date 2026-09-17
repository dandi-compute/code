"""
The job ID that names a job capsule directory.

A job capsule directory is named ``job-{YYMMDD}{hash}``, where ``hash`` is the first six
characters of the MD5 checksum of the fields that identify the job. The date makes the name
readable at a glance and separates re-attempts of the same job across days, while the hash
keeps capsules for different parameters, configs or assets apart within a single day.

Both halves are fixed width, six characters each, so the two are unambiguous without a
separator between them.

Two capsules can still land on one name, when they are the same logical job submitted on the
same day: re-attempts differ only by details the hash deliberately ignores. A ``-2``, ``-3``
counter is appended to tell those apart. Preparation never produces one, because it does not
form a capsule for a job that already has one, but a migration of older capsules does.

Everything the name used to spell out (pipeline version, codebase version, parameters and
config) is recorded in the capsule's ``dataset_description.json`` provenance and in the
``state.tsv`` summary table.
"""

import datetime
import hashlib
import re

#: Job capsule directory name, e.g. ``job-260916a1b2c3``, or ``job-260916a1b2c3-2`` for the
#: second capsule of one job on one day.
_JOB_ID_RE = re.compile(r"job-(?P<job_date>\d{6})(?P<job_hash>[0-9a-f]{6})(?:-(?P<job_index>[2-9]|\d{2,}))?")

#: Key under which job provenance is written into a capsule's ``dataset_description.json``.
_PROVENANCE_KEY = "DandiCompute"


def _compute_job_hash(
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

    The codebase version is deliberately left out. A job is the same logical job no matter
    which release of this package formed it, which matches how the queue decides whether a
    capsule already exists.
    """
    payload = "|".join([dandiset_id, dandi_path, pipeline, version, params, config, content_id])
    job_hash = hashlib.md5(payload.encode("utf-8")).hexdigest()[:6]
    return job_hash


def _format_job_id(*, job_hash: str, date: datetime.date | None = None, index: int = 1) -> str:
    """
    Build the ``job-{YYMMDD}{hash}`` directory name, defaulting to today's date.

    :param index: Which capsule this is among those sharing the name. The first carries no
        counter, so the common case reads as ``job-260916a1b2c3``; later ones are suffixed
        ``-2``, ``-3`` and so on.
    """
    date = date if date is not None else datetime.datetime.now(tz=datetime.timezone.utc).date()
    counter = "" if index <= 1 else f"-{index}"
    job_id = f"job-{date:%y%m%d}{job_hash}{counter}"
    return job_id


def _parse_job_hash(job_id: str, /) -> str | None:
    """
    Return the hash portion of a job ID, or ``None`` when *job_id* is not a job ID.

    Any counter suffix is ignored, so capsules sharing a job read back as the same job.
    """
    match = _JOB_ID_RE.fullmatch(job_id)
    job_hash = match.group("job_hash") if match is not None else None
    return job_hash
