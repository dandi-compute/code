import logging
import os
import pathlib
import subprocess

import pydantic

_log = logging.getLogger(__name__)


@pydantic.validate_call
def submit_job(script_file_path: pathlib.Path) -> None:
    """Submit a pipeline script via sbatch."""
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
    _log.info(f"sbatch return code: {result.returncode}\n" f"stdout: {result.stdout}\n" f"stderr: {result.stderr}")
    if result.returncode != 0:
        message = f"command: {command}\n{result_message}"
        raise RuntimeError(message)
