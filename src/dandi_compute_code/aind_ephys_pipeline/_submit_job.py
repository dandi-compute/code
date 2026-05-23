import os
import pathlib
import subprocess
import warnings

import pydantic


@pydantic.validate_call
def submit_job(script_file_path: pathlib.Path) -> None:
    """Submit a pipeline script via sbatch."""
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)

    absolute_script_file_path = script_file_path.absolute()
    command = ["sbatch", str(absolute_script_file_path)]
    warnings.warn(f"Submitting sbatch script: {absolute_script_file_path}", stacklevel=2)
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )
    result_message = (
        f"sbatch return code: {result.returncode}\n" f"stdout: {result.stdout}\n" f"stderr: {result.stderr}"
    )
    warnings.warn(result_message, stacklevel=2)
    if result.returncode != 0:
        message = f"command: {command}\n{result_message}"
        raise RuntimeError(message)
