import os
import pathlib
import subprocess

import pydantic


@pydantic.validate_call
def submit_aind_ephys_job(script_file_path: pathlib.Path) -> None:
    """ """
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)

    command = ["sbatch", str(script_file_path)]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 and result.stderr:
        message = f"command: {command}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        raise RuntimeError(message)
