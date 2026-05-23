import logging
import subprocess

_log = logging.getLogger(__name__)


def _count_running_aind_ephys_pipeline_jobs() -> int:
    """
    Count currently running AIND Ephys pipeline jobs via the SLURM scheduler.

    Calls ``squeue --me --format=%j`` and counts jobs whose name is exactly
    ``AIND-Ephys-Pipeline``.

    Returns
    -------
    int
        Number of running jobs with SLURM job-name ``AIND-Ephys-Pipeline``.
    """
    command = ["squeue", "--me", "--format=%j"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 and result.stderr:
        message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)
    if result.stderr:
        _log.warning(result.stderr)
    return sum(1 for line in result.stdout.splitlines() if line.strip() == "AIND-Ephys-Pipeline")
