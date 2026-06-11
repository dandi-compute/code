import logging
import pathlib
import random
import time
from typing import Literal

from ._count_running_aind_ephys_pipeline_jobs import _count_running_aind_ephys_pipeline_jobs
from ._submit_next import _submit_next

_log = logging.getLogger(__name__)


def process_queue(
    *,
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    max_concurrent_aind_jobs: int = 2,
    jitter_seconds: float = 30.0,
    test: bool = False,
) -> Literal["submitted", "no-pending", "slots-unavailable"]:
    """
    Submit jobs from ``state.jsonl`` up to ``max_concurrent_aind_jobs`` total
    running ``AIND-Ephys-Pipeline`` SLURM jobs.

    If ``state.jsonl`` is absent, a :class:`FileNotFoundError` is raised.
    If ``state.jsonl`` exists but is empty, a warning is emitted and the
    invocation returns without submitting jobs. Otherwise ``squeue --me`` is
    checked for currently running ``AIND-Ephys-Pipeline`` jobs, and up to the
    difference from ``max_concurrent_aind_jobs`` jobs are submitted.

    A random delay of up to *jitter_seconds* is applied before any work is
    done to spread out concurrent invocations and avoid thundering-herd
    submission bursts.

    :param queue_directory: Path to the queue root directory.
    :type queue_directory: pathlib.Path
    :param processing_directory: Path to the directory used for temporary working trees
        during job submission.
    :type processing_directory: pathlib.Path
    :param max_concurrent_aind_jobs:
        Maximum number of ``AIND-Ephys-Pipeline`` jobs allowed to be running
        concurrently before new submissions are skipped.
    :type max_concurrent_aind_jobs: int
    :param jitter_seconds:
        Maximum number of seconds to sleep before proceeding. A uniformly
        random duration between ``0`` and *jitter_seconds* is chosen each
        invocation. Set to ``0`` to disable jitter entirely.
    :type jitter_seconds: float
    :param test: If ``True``, preserve temporary processing directories on success.
    :type test: bool
    :returns:
        ``"submitted"`` when one or more jobs were submitted,
        ``"no-pending"`` when no pending jobs were available to submit, and
        ``"slots-unavailable"`` when submission was skipped because no queue
        slots were available.
    :rtype: Literal["submitted", "no-pending", "slots-unavailable"]
    :raises FileNotFoundError: If ``state.jsonl`` is not found in *queue_directory*.
    :raises ValueError: If *jitter_seconds* is negative.
    """
    if jitter_seconds < 0:
        message = "jitter_seconds must be non-negative"
        raise ValueError(message)
    if jitter_seconds > 0:
        delay = random.uniform(0, jitter_seconds)
        _log.info("Sleeping %.2f seconds (jitter) before processing queue", delay)
        time.sleep(delay)

    state_file = queue_directory / "state.jsonl"
    if not state_file.exists():
        message = f"State file not found: {state_file}"
        raise FileNotFoundError(message)
    if max_concurrent_aind_jobs < 1:
        message = "max_concurrent_aind_jobs must be at least 1"
        raise ValueError(message)
    if not state_file.read_text().strip():
        _log.info(f"No entries in {state_file}")
        return "no-pending"

    running_count = _count_running_aind_ephys_pipeline_jobs()
    available_slots = max(0, max_concurrent_aind_jobs - running_count)
    if available_slots < 1:
        return "slots-unavailable"

    submitted_any = _submit_next(
        processing_directory=processing_directory,
        max_submissions=available_slots,
        test=test,
    )
    return "submitted" if submitted_any else "no-pending"
