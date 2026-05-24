import datetime
import logging
import os
import pathlib
import subprocess
import tempfile

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
    _log.info(f"sbatch returned code: {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")
    if result.returncode != 0:
        message = "sbatch submission failed - please check the logs to see more details."
        raise RuntimeError(message)

    # Infer the path within the dandiset relative to `001697/derivatives`.
    # e.g. if absolute_script_file_path is `/some/root/001697/derivatives/sub-X/ses-Y/script.sh`,
    # then `relative_derivatives_subpath` will be `sub-X/ses-Y`.
    path_parts = absolute_script_file_path.parts
    try:
        derivatives_index = path_parts.index("derivatives")
        dandiset_id_index = derivatives_index - 1
        if path_parts[dandiset_id_index] != "001697":
            message = (
                f"Expected dandiset ID `001697` before `derivatives` in path, "
                f"but found `{path_parts[dandiset_id_index]}`."
            )
            raise RuntimeError(message)
    except ValueError as exception:
        message = f"Could not find `derivatives` segment in path: {absolute_script_file_path}"
        raise RuntimeError(message) from exception

    relative_derivatives_subpath = pathlib.Path(*path_parts[derivatives_index + 1 : -1])
    dandi_remote_path = f"dandi://dandi/001697/derivatives/{relative_derivatives_subpath.as_posix()}"

    # Create a fresh working directory under the shared processing area and pull down
    # the existing derivatives tree so that the subsequent `dandi upload` from inside
    # it targets the matching remote path.
    processing_root = pathlib.Path("/orcd/data/dandi/001/dandi-compute/processing")
    processing_root.mkdir(parents=True, exist_ok=True)
    download_dir = pathlib.Path(tempfile.mkdtemp(dir=processing_root))
    _log.info(f"Downloading existing derivatives from {dandi_remote_path} into: {download_dir}")
    result = subprocess.run(
        ["dandi", "download", "-r", "--preserve-tree", dandi_remote_path],
        capture_output=True,
        text=True,
        cwd=download_dir,
    )
    _log.info(
        f"DANDI download returned code: {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    if result.returncode != 0:
        message = "DANDI download failed - please check the logs to see more details."
        raise RuntimeError(message)

    # With `--preserve-tree`, the download lands at `<download_dir>/001697/derivatives/<subpath>`.
    # Drop the `submitted` marker there so it gets picked up by the upload below.
    local_derivatives_dir = download_dir / "001697" / "derivatives" / relative_derivatives_subpath
    submitted_file_path = local_derivatives_dir / "submitted"
    _log.info(f"Creating `submitted` file at: {submitted_file_path}")
    submitted_file_path.write_text(datetime.datetime.now().isoformat())

    # Upload from the root of the downloaded dandiset so `dandi upload` can resolve
    # the dandiset metadata (dandiset.yaml lives at `<download_dir>/001697/`).
    dandiset_root = download_dir / "001697"
    result = subprocess.run(
        ["dandi", "upload"],
        capture_output=True,
        text=True,
        cwd=dandiset_root,
    )
    _log.info(f"Upload to DANDI returned code: {result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}")
    if result.returncode != 0:
        message = "DANDI upload failed - please check the logs to see more details."
        raise RuntimeError(message)
