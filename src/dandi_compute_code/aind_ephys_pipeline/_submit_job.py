import logging
import os
import pathlib
import subprocess

import pydantic

from .._write_submitted_marker import write_submitted_marker

_log = logging.getLogger(__name__)


@pydantic.validate_call
def submit_job(script_file_path: pathlib.Path) -> None:
    """
    Submit a pipeline script via sbatch.

    Raises
    ------
    RuntimeError
        If the ``DANDI_API_KEY`` environment variable is not set, if the
        ``sbatch`` submission returns a non-zero exit code, or if the
        subsequent ``dandi upload`` invocation returns a non-zero exit code.
    """
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)

    absolute_script_file_path = script_file_path.absolute()
    command = ["sbatch", str(absolute_script_file_path)]
    _log.info(f"Submitting sbatch script: {absolute_script_file_path}")
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )
    _log.info(f"sbatch returned code: {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")
    if result.returncode != 0:
        message = "sbatch submission failed - please check the logs to see more details."
        raise RuntimeError(message)

    submitted_file_path = write_submitted_marker(submit_script_path=absolute_script_file_path)
    _log.info(f"Created `submitted` file at: {submitted_file_path.absolute()}")
    result = subprocess.run(
        ["dandi", "upload"],
        capture_output=True,
        text=True,
        cwd=absolute_script_file_path.parent,
    )
    _log.info(f"Upload to DANDI returned code: {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")
    if result.returncode != 0:
        message = "DANDI upload failed - please check the logs to see more details."
        raise RuntimeError(message)
