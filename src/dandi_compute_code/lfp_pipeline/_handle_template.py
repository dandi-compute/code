import pathlib

import jinja2

from ._globals import _RAW_TEMPLATE_FILE_PATH


def generate_lfp_submission_script(
    *,
    script_file_path: pathlib.Path,
    log_directory: str,
    dataset_directory: str,
    environment_directory: str,
    container_name: str,
    container_image: str,
    nwb_file_path: str,
    output_nwb_file_path: str,
    parameters_key: str,
    temp_name: str,
    done_tracker_file_path: str,
) -> None:
    """
    Generate the LFP pipeline sbatch submission script from the template.

    The script uses datalad-containers and duct to run the LFP runtime container
    on a local NWB file. It is intentionally much simpler than the AIND ephys
    submission script.

    :param script_file_path: Where to write the submission script.
    :type script_file_path: pathlib.Path
    :param log_directory: Directory for the slurm and duct logs.
    :type log_directory: str
    :param dataset_directory: The datalad dataset directory in which the container runs.
    :type dataset_directory: str
    :param environment_directory: The conda environment to activate. It must provide datalad,
        datalad-container, con-duct, and apptainer.
    :type environment_directory: str
    :param container_name: The datalad container registration name.
    :type container_name: str
    :param container_image: The container image reference, for example
        ``ghcr.io/dandi-compute/dandi-compute-lfp:latest``.
    :type container_image: str
    :param nwb_file_path: Path to the input NWB file. It is a valid NWB file even though it
        has no ``.nwb`` suffix.
    :type nwb_file_path: str
    :param output_nwb_file_path: Path to write the resulting NWB file.
    :type output_nwb_file_path: str
    :param parameters_key: The registered LFP parameters key to run.
    :type parameters_key: str
    :param temp_name: The name recorded in the done tracker file on completion.
    :type temp_name: str
    :param done_tracker_file_path: The path to the done tracker file.
    :type done_tracker_file_path: str
    """
    raw_template = _RAW_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        log_directory=log_directory,
        dataset_directory=dataset_directory,
        environment_directory=environment_directory,
        container_name=container_name,
        container_image=container_image,
        nwb_file_path=nwb_file_path,
        output_nwb_file_path=output_nwb_file_path,
        parameters_key=parameters_key,
        temp_name=temp_name,
        done_tracker_file_path=done_tracker_file_path,
    )
    script_file_path.write_text(data=f"{script}\n")
