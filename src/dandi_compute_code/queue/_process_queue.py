import fcntl
import pathlib

from ._count_running_aind_ephys_pipeline_jobs import _count_running_aind_ephys_pipeline_jobs
from ._refresh_queue import refresh_queue_state
from ._submit_next import _submit_next


def process_queue(*, queue_directory: pathlib.Path, dandiset_directory: pathlib.Path) -> None:
    """
    Submit jobs from ``state.jsonl`` up to two total
    running ``AIND-Ephys-Pipeline`` SLURM jobs.

    If ``state.jsonl`` is absent or empty, :func:`refresh_queue_state` is called
    first to populate it. Then ``squeue --me`` is checked for currently running
    ``AIND-Ephys-Pipeline`` jobs, and up to the difference from two jobs are
    submitted.

    To avoid duplicate submissions from overlapping invocations, a non-blocking
    advisory file lock is acquired on ``process_queue.lock`` in
    *queue_directory*. If the lock is already held, the invocation returns
    without submitting jobs.

    Parameters
    ----------
    queue_directory : pathlib.Path
        Path to the queue root directory.
    dandiset_directory : pathlib.Path
        Path to a local clone of the 001697 dandiset repository.  Used to
        locate prepared submission scripts and to count failure directories
        for ``max_fail_per_dandiset`` enforcement.

    Raises
    ------
    FileNotFoundError
        If ``queue_config.json`` is not found in *queue_directory* (raised by
        :func:`refresh_queue_state`).
    """
    lock_file = queue_directory / "process_queue.lock"
    lock_file.touch(exist_ok=True)
    with lock_file.open("r+") as lock_file_stream:
        try:
            fcntl.flock(lock_file_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"Skipping queue processing: lock already held at `{lock_file}`")
            return

        state_file = queue_directory / "state.jsonl"
        if not state_file.exists() or not state_file.read_text().strip():
            refresh_queue_state(queue_directory=queue_directory, dandiset_directory=dandiset_directory)

        running_count = _count_running_aind_ephys_pipeline_jobs()
        available_slots = max(0, 2 - running_count)
        if available_slots > 0:
            _submit_next(
                queue_directory=queue_directory,
                dandiset_directory=dandiset_directory,
                max_submissions=available_slots,
            )
