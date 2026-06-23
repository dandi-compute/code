from ._queue_state import QueueState


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
    return QueueState.has_pending_jobs()
