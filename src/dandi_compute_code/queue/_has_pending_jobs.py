import logging

from ._find_pending_entries import _find_pending_entries

_log = logging.getLogger(__name__)


def has_pending_jobs() -> bool:
    """
    Report whether any queued jobs are awaiting submission.

    This is a lightweight check intended to gate queue dispatch. It inspects the
    DANDI assets metadata for attempt directories that contain a ``code/submit.sh``
    asset without an adjacent submitted marker. It does not submit anything and
    does not require SLURM access.

    Returns
    -------
    bool
        ``True`` when at least one job is awaiting submission, ``False`` otherwise.
    """
    pending_entries = _find_pending_entries()
    pending = len(pending_entries) > 0
    _log.info("Found %d pending queue entries", len(pending_entries))
    return pending
