import logging
import pathlib
import shutil
import subprocess
import tempfile

from .._write_submitted_marker import write_submitted_marker
from ..dandiset._load_assets_jsonld_metadata import load_assets_jsonld_metadata

_log = logging.getLogger(__name__)

_DANDISET_ID = "001697"


def _submit_next(
    *,
    processing_directory: pathlib.Path,
    max_submissions: int = 2,
    test: bool = False,
) -> bool:
    """
    Submit the next eligible pending entries from the DANDI assets metadata.

    Loads the DANDI ``assets.jsonld`` metadata and identifies all attempt
    directories that contain a ``code/submit.sh`` asset but no adjacent
    ``code/submitted`` asset.  For each candidate (up to *max_submissions*),
    a temporary working directory is created inside *processing_directory*,
    the ``code/`` tree is downloaded via ``dandi download --preserve-tree``,
    the submission script is executed via ``sbatch``, a ``submitted`` marker
    is written adjacent to ``submit.sh``, the marker is pushed back to the
    archive via ``dandi upload --validation skip``, and the temporary
    directory is removed on success.

    Parameters
    ----------
    processing_directory : pathlib.Path
        Directory in which temporary per-job working trees are created.
    max_submissions : int, optional
        Maximum number of pending jobs to submit.
    test : bool, optional
        When ``True``, leave temporary working directories on disk after
        successful submission for debugging.

    Returns
    -------
    bool
        ``True`` if at least one job was submitted, ``False`` otherwise.

    Raises
    ------
    RuntimeError
        If ``dandi download``, ``sbatch``, or ``dandi upload`` returns a
        non-zero exit code for any candidate.
    """
    if max_submissions < 1:
        return False

    metadata = load_assets_jsonld_metadata()
    paths = set(metadata.path_to_asset_metadata.keys())

    candidates: list[str] = []
    for asset_path in sorted(paths):
        if asset_path.endswith("/code/submit.sh"):
            submitted_path = asset_path[: -len("submit.sh")] + "submitted"
            if submitted_path not in paths:
                code_dir_path = asset_path[: -len("/submit.sh")]
                candidates.append(code_dir_path)

    if not candidates:
        _log.info("No eligible pending entries available for submission")
        return False

    for code_dir_path in candidates[:max_submissions]:
        dandi_url = f"dandi://dandi/{_DANDISET_ID}/{code_dir_path}/"
        # Temporary directory is intentionally left on disk when any step fails
        # so that it can be inspected for debugging.
        temp_dir = pathlib.Path(tempfile.mkdtemp(dir=processing_directory))
        _log.info("Submitting job run for %s in %s", code_dir_path, temp_dir)

        result = subprocess.run(
            ["dandi", "download", "--preserve-tree", dandi_url],
            capture_output=True,
            text=True,
            cwd=temp_dir,
        )
        _log.info("dandi download returned code %d for %s", result.returncode, dandi_url)
        _log.debug("dandi download stdout: %s\nstderr: %s", result.stdout, result.stderr)
        if result.returncode != 0:
            _log.warning("dandi download stdout: %s\nstderr: %s", result.stdout, result.stderr)
            message = f"dandi download failed for {dandi_url}"
            raise RuntimeError(message)

        dandiset_directory = temp_dir / _DANDISET_ID
        submit_sh_path = dandiset_directory / code_dir_path / "submit.sh"
        result = subprocess.run(
            ["sbatch", str(submit_sh_path.absolute())],
            capture_output=True,
            text=True,
        )
        _log.info("sbatch returned code %d; stdout %s", result.returncode, result.stdout)
        _log.debug("sbatch stdout: %s\nstderr: %s", result.stdout, result.stderr)
        if result.returncode != 0:
            _log.warning("sbatch stdout: %s\nstderr: %s", result.stdout, result.stderr)
            message = "sbatch submission failed - please check the logs to see more details."
            raise RuntimeError(message)

        submitted_marker = write_submitted_marker(submit_script_path=submit_sh_path)
        _log.info("Created `submitted` file at: %s", submitted_marker.absolute())

        result = subprocess.run(
            ["dandi", "upload", "--validation", "skip"],
            capture_output=True,
            text=True,
            cwd=dandiset_directory,
        )
        _log.info("dandi upload returned code %d", result.returncode)
        _log.debug("dandi upload stdout: %s\nstderr: %s", result.stdout, result.stderr)
        if result.returncode != 0:
            _log.warning("dandi upload stdout: %s\nstderr: %s", result.stdout, result.stderr)
            message = "dandi upload failed - please check the logs to see more details."
            raise RuntimeError(message)

        if test:
            _log.info("Leaving temporary directory in place for test mode: %s", temp_dir)
        else:
            shutil.rmtree(temp_dir)

    return True
