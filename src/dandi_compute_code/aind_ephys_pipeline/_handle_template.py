import pathlib

import jinja2
import pydantic

from ._globals import _RAW_TEMPLATE_FILE_PATH


@pydantic.validate_call
def generate_aind_ephys_submission_script(
    script_file_path: pathlib.Path,
    log_directory: str,
    nwb_file_path: str,
    results_directory: str,
    work_directory: str,
    apptainer_cache_directory: str,
    environment_directory: str,
    config_file_path: str,
    pipeline_file_path: str,
    pipeline_repo_directory: str,
    pipeline_version: str,
    temp_name: str,
    done_tracker_file_path: str,
    params_file_path: str,
) -> None:
    """
    Generate AIND Ephys submission script from template.

    Arguments are ordered as they occur in the submission template.

    :param script_file_path: Where to write the submission script.
    :type script_file_path: pathlib.Path
    :param log_directory: The log directory.
    :type log_directory: str
    :param nwb_file_path: The input NWB file path.
    :type nwb_file_path: str
    :param results_directory: The results directory.
    :type results_directory: str
    :param work_directory: The work directory.
    :type work_directory: str
    :param apptainer_cache_directory: The Apptainer cache directory.
    :type apptainer_cache_directory: str
    :param environment_directory: The conda environment to activate.
    :type environment_directory: str
    :param config_file_path: The configuration file path.
    :type config_file_path: str
    :param pipeline_file_path: The pipeline file path.
    :type pipeline_file_path: str
    :param pipeline_repo_directory: Path to the base pipeline repository intended to be used.
    :type pipeline_repo_directory: str
    :param pipeline_version: The pipeline version, which is used to checkout a branch of the pipeline repository.
    :type pipeline_version: str, optional
    :param temp_name: The name of the temporary processing directory.
    :type temp_name: str
    :param done_tracker_file_path: The path to the 'done' tracker file.
    :type done_tracker_file_path: str
    :param params_file_path: The parameters file path.
    :type params_file_path: str
    """
    raw_template = _RAW_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        log_directory=log_directory,
        nwb_file_path=nwb_file_path,
        data_path=str(pathlib.Path(nwb_file_path).parent),
        results_directory=results_directory,
        work_directory=work_directory,
        apptainer_cache_directory=apptainer_cache_directory,
        environment_directory=environment_directory,
        config_file_path=config_file_path,
        pipeline_file_path=pipeline_file_path,
        pipeline_repo_directory=pipeline_repo_directory,
        pipeline_version=pipeline_version,
        params_file_path=params_file_path,
        temp_name=temp_name,
        done_tracker_file_path=done_tracker_file_path,
    )
    script_file_path.write_text(data=f"{script}\n")  # Extra newline to prevent improper console wrapping in manual mode
