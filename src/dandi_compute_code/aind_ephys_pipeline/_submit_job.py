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

    subprocess.run(["sbatch", str(script_file_path)])
