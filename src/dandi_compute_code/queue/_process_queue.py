import logging
import pathlib
from typing import Literal

from ._count_running_aind_ephys_pipeline_jobs import _count_running_aind_ephys_pipeline_jobs
from ._submit_next import _submit_next

_log = logging.getLogger(__name__)


def process_queue(
    *,
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    test: bool = False,
) -> Literal["submitted", "no-pending", "slots-unavailable"]:
    """
    Submit jobs from ``state.jsonl`` up to two total
    running ``AIND-Ephys-Pipeline`` SLURM jobs.

    If ``state.jsonl`` is absent, a :class:`FileNotFoundError` is raised.
    If ``state.jsonl`` exists but is empty, a warning is emitted and the
    invocation returns without submitting jobs. Otherwise ``squeue --me`` is
    checked for currently running ``AIND-Ephys-Pipeline`` jobs, and up to the
    difference from two jobs are submitted.

    :param queue_directory: Path to the queue root directory.
    :type queue_directory: pathlib.Path
    :param processing_directory: Path to the directory used for temporary working trees
        during job submission.
    :type processing_directory: pathlib.Path
    :param test: If ``True``, preserve temporary processing directories on success.
    :type test: bool
    :returns:
        ``"submitted"`` when one or more jobs were submitted,
        ``"no-pending"`` when no pending jobs were available to submit, and
        ``"slots-unavailable"`` when submission was skipped because no queue
        slots were available.
    :rtype: Literal["submitted", "no-pending", "slots-unavailable"]
    :raises FileNotFoundError: If ``state.jsonl`` is not found in *queue_directory*.
    """
    state_file = queue_directory / "state.jsonl"
    if not state_file.exists():
        message = f"State file not found: {state_file}"
        raise FileNotFoundError(message)
    if not state_file.read_text().strip():
        _log.info(f"No entries in {state_file}")
        return "no-pending"

    running_count = _count_running_aind_ephys_pipeline_jobs()
    available_slots = max(0, 2 - running_count)
    if available_slots < 1:
        return "slots-unavailable"

    submitted_any = _submit_next(
        processing_directory=processing_directory,
        max_submissions=available_slots,
        test=test,
    )
    return "submitted" if submitted_any else "no-pending"
