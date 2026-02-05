import pathlib

import jinja2
import pydantic

from ._globals import _RAW_TEMPLATE_FILE_PATH


@pydantic.validate_call
def generate_aind_ephys_submission_script(
    script_file_path: pathlib.Path,
    log_directory: str,
    nwbfile_path: str,
    results_directory: str,
    work_directory: str,
    apptainer_cache_directory: str,
    environment_directory: str,
    config_file_path: str,
    pipeline_file_path: str,
    temporary_processing_directory: str,
) -> None:
    """
    Generate AIND Ephys submission script from template.

    Arguments are ordered as they occur in the submission template.

    Parameters
    ----------
    script_file_path : pathlib.Path
        Where to write the submission script.
    log_directory : str
        The log directory.
    nwbfile_path : str
        The input NWB file path.
    results_directory : str
        The results directory.
    work_directory : str
        The work directory.
    apptainer_cache_directory : str
        The Apptainer cache directory.
    environment_directory : str
        The conda environment to activate.
    config_file_path : str
        The configuration file path.
    pipeline_file_path : str
        The pipeline file path.
    temporary_processing_directory : str
        The temporary processing directory.
    """
    raw_template = _RAW_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        log_directory=log_directory,
        nwbfile_path=nwbfile_path,
        results_directory=results_directory,
        work_directory=work_directory,
        apptainer_cache_directory=apptainer_cache_directory,
        environment_directory=environment_directory,
        config_file_path=config_file_path,
        pipeline_file_path=pipeline_file_path,
        temporary_processing_directory=temporary_processing_directory,
    )
    script_file_path.write_text(data=script)
