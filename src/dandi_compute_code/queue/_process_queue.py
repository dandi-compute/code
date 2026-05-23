import fcntl
import pathlib
import warnings

from ._count_running_aind_ephys_pipeline_jobs import _count_running_aind_ephys_pipeline_jobs
from ._submit_next import _submit_next


# TODO: remove inherent flock usage and offload that to `flock` in CLI submitter
def process_queue(
    *,
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    datalad_directory: pathlib.Path | None = None,
) -> None:
    """
    Submit jobs from ``state.jsonl`` up to two total
    running ``AIND-Ephys-Pipeline`` SLURM jobs.

    If ``state.jsonl`` is absent, a :class:`FileNotFoundError` is raised.
    If ``state.jsonl`` exists but is empty, a warning is emitted and the
    invocation returns without submitting jobs. Otherwise ``squeue --me`` is
    checked for currently running ``AIND-Ephys-Pipeline`` jobs, and up to the
    difference from two jobs are submitted.

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
    datalad_directory : pathlib.Path | None, optional
        Path to the DataLad-backed work tree used to resolve attempt
        directories. Defaults to ``dandiset_directory``.

    Raises
    ------
    FileNotFoundError
        If ``state.jsonl`` is not found in *queue_directory*.
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
        if not state_file.exists():
            message = f"State file not found: {state_file}"
            raise FileNotFoundError(message)
        if not state_file.read_text().strip():
            warnings.warn(f"No entries in {state_file}", stacklevel=2)
            return

        running_count = _count_running_aind_ephys_pipeline_jobs()
        available_slots = max(0, 2 - running_count)
        if available_slots > 0:
            datalad_root = datalad_directory if datalad_directory is not None else dandiset_directory
            _submit_next(
                queue_directory=queue_directory,
                datalad_directory=datalad_root,
                dandiset_directory=dandiset_directory,
                max_submissions=available_slots,
            )
