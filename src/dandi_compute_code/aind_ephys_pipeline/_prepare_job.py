import contextlib
import hashlib
import io
import os
import pathlib
import tempfile
import typing

import dandi
import dandi.dandiapi
import dandi.download
import dandi.upload
import pydantic
import yaml

from ._handle_template import generate_aind_ephys_submission_script


@pydantic.validate_call
def prepare_aind_ephys_job(
    content_id: str,
    config_file_path: pathlib.Path | None = None,
    parameters_key: typing.Literal["default", "no-motion", "custom"] = "default",
    parameters_file_path: pathlib.Path | None = None,
    pipeline_file_path: pathlib.Path | None = None,
    pipeline_version: str = "v1.0.0",
    silent: bool = False,
) -> pathlib.Path:
    """
    Prepares an AIND ephys job by generating a submission script and returning the script file path.

    Parameters
    ----------
    content_id : str
        The content ID for the data to be processed.
    config_file_path : pathlib.Path, optional
        Path to the configuration file.
    parameters_key : one of "default", "no-motion", or "custom".
        The name of the parameters to use.
        If "custom" is selected, `parameters_file_path` must be provided.
    parameters_file_path : pathlib.Path, optional
        Path to the parameters file.
    pipeline_file_path : pathlib.Path, optional
        Path to the pipeline file.
    pipeline_version : str, optional
        The version of the pipeline to use, which will be used to checkout a branch of the pipeline repository.
        Default is "v1.0.0".
    silent : bool, optional
        Whether to suppress output messages from the DANDI client.
        Default is False.

    Returns
    -------
    script_file_path : pathlib.Path
        The path to the generated submission script.
    """
    if parameters_key == "custom" and parameters_file_path is None:
        message = "If `parameters_key` is 'custom', then `parameters_file_path` must be provided."
        raise ValueError(message)
    if parameters_key != "custom" and parameters_file_path is not None:
        message = "If `parameters_file_path` is provided, then `parameters_key` must be 'custom'."
        raise ValueError(message)
    # TODO: remove the API key use once Dandiset is public
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)
    client = dandi.dandiapi.DandiAPIClient(token=os.environ["DANDI_API_KEY"])
    dandiset = client.get_dandiset(dandiset_id="001697")

    if parameters_key == "default":
        parameters_file_path = pathlib.Path(__file__).parent / "default_parameters.json"
        params_id = parameters_key
    elif parameters_key == "no-motion":
        parameters_file_path = pathlib.Path(__file__).parent / "no_motion_parameters.json"
        params_id = parameters_key
    else:
        params_id = hashlib.md5(parameters_file_path.read_bytes()).hexdigest()

    dandi_compute_dir = pathlib.Path("/orcd/data/dandi/001/dandi-compute")
    dandi_cache_directory = dandi_compute_dir / "dandi-cache"
    content_id_to_unique_dandiset_path_file = (
        dandi_cache_directory
        / "content-id-to-unique-dandiset-path"
        / "derivatives"
        / "content_id_to_unique_dandiset_path.yaml"
    )
    with content_id_to_unique_dandiset_path_file.open(mode="r") as file_stream:
        content_id_to_unique_dandiset_path = yaml.safe_load(file_stream)

    if content_id not in content_id_to_unique_dandiset_path:
        message = (
            f"Content ID {content_id} not found in content ID to unique Dandiset path mapping. "
            "This likely means that the content ID is not associated with a Dandiset, "
            "or that the mapping file is out of date."
        )
        raise ValueError(message)

    dandiset_id, dandiset_path = next(iter(content_id_to_unique_dandiset_path[content_id].items()))
    dandiset_path_no_suffix = dandiset_path.removesuffix(".nwb")

    # TODO: if first run for asset, skip below and add sourcedata

    # Assign the lowest integer run ID that has not been used yet, up to a maximum limit
    maximum_run_id = 99
    run_id = 1
    output_dandiset_path = (
        f"derivatives/pipeline-aind+ephys_version-{pipeline_version}/derivatives/dandiset-{dandiset_id}/"
        f"{dandiset_path_no_suffix}/attempt-{run_id}_params-{params_id}"
    )
    for _ in range(maximum_run_id + 1):
        assets_checker = dandiset.get_assets_with_path_prefix(path=output_dandiset_path)
        if next(assets_checker, None) is None:
            continue

        run_id += 1
        output_dandiset_path = (
            f"derivatives/pipeline-aind+ephys_version-{pipeline_version}/derivatives/dandiset-{dandiset_id}/"
            f"{dandiset_path_no_suffix}/attempt-{run_id}_params-{params_id}"
        )

    config_file_path = config_file_path or pathlib.Path(__file__).parent / "mit_engaging.config"
    pipeline_file_path = (
        pipeline_file_path or dandi_compute_dir / "aind-ephys-pipeline.cody/pipeline/main_multi_backend.nf"
    )
    blob_head = content_id[0]
    partition = "001" if ord(blob_head) - ord("0") < 10 else "002"
    nwbfile_path = f"/orcd/data/dandi/{partition}/s3dandiarchive/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}"
    # TODO: figure out if Zarr or not - only supports blobs ATM

    # Create an empty copy of Dandiset
    processing_directory = dandi_compute_dir / "processing"
    temporary_processing_directory = pathlib.Path(tempfile.mkdtemp(dir=processing_directory))
    dandi.download.download(
        urls="DANDI:001697",
        output_dir=temporary_processing_directory,
        get_metadata=True,
        get_assets=False,
    )

    # Construct BIDS derivative content
    dandiset_output_dir = temporary_processing_directory / "001697" / output_dandiset_path

    code_dir = dandiset_output_dir / "code"
    script_file_path = code_dir / "submit.sh"
    code_config_file_path = code_dir / config_file_path.name
    code_parameters_file_path = code_dir / parameters_file_path.name
    code_capsule_versions_file_path = code_dir / "capsule_versions.env"

    log_directory = dandiset_output_dir / "logs"

    code_dir.mkdir(parents=True)
    log_directory.mkdir()
    intermediate_dir = dandiset_output_dir / "intermediate"
    intermediate_dir.mkdir()

    results_directory = intermediate_dir  # Start off the results in the intermediate folder and separate later
    work_directory = dandi_compute_dir / "work"
    apptainer_cache_directory = work_directory / "apptainer_cache"
    # NOTE: NUMBA_CACHE_DIR is also needed for the pipeline
    # but must be set in `~/.bashrc`, and must be the same as WORKDIR
    # NUMBA_CACHE_DIR = "/orcd/data/dandi/001/dandi-compute/work"
    environment_directory = "/orcd/data/dandi/001/environments/name-nextflow_environment"
    done_tracker_file_path = processing_directory / "done.txt"

    pipeline_repo_directory = pipeline_file_path.parent.parent
    capsule_versions_file_path = pipeline_repo_directory / "pipeline" / "capsule_versions.env"

    # Construct submission script from template
    generate_aind_ephys_submission_script(
        script_file_path=script_file_path,
        log_directory=str(log_directory),
        nwb_file_path=nwbfile_path,
        results_directory=str(results_directory),
        work_directory=str(work_directory),
        apptainer_cache_directory=str(apptainer_cache_directory),
        environment_directory=environment_directory,
        config_file_path=str(code_config_file_path),
        pipeline_file_path=str(pipeline_file_path),
        pipeline_repo_directory=str(pipeline_repo_directory),
        pipeline_version=pipeline_version,
        temp_name=temporary_processing_directory.name,
        done_tracker_file_path=str(done_tracker_file_path),
        params_file_path=str(code_parameters_file_path),
    )
    code_config_file_path.write_text(data=config_file_path.read_text())
    code_parameters_file_path.write_text(data=parameters_file_path.read_text())
    code_capsule_versions_file_path.write_text(data=capsule_versions_file_path.read_text())

    # Upload preparation to 'reserve' spot during processing
    if silent:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            dandi.upload.upload(
                paths=[dandiset_output_dir],
                allow_any_path=True,
                validation=dandi.upload.UploadValidation.SKIP,
            )
    else:
        dandi.upload.upload(
            paths=[dandiset_output_dir],
            allow_any_path=True,
            validation=dandi.upload.UploadValidation.SKIP,
        )

    return script_file_path
