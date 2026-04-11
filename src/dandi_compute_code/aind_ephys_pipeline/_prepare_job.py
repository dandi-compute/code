import contextlib
import datetime
import hashlib
import io
import json
import os
import pathlib
import re
import subprocess
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
    parameters_key: typing.Literal["default", "no+motion", "custom"] = "default",
    parameters_file_path: pathlib.Path | None = None,
    pipeline_directory: pathlib.Path | None = None,
    pipeline_version: str = "v1.0.0-fixes",
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
    parameters_key : one of "default", "no+motion", or "custom".
        The name of the parameters to use.
        If "custom" is selected, `parameters_file_path` must be provided.
    parameters_file_path : pathlib.Path, optional
        Path to the parameters file.
    pipeline_directory : pathlib.Path, optional
        Local path to the AIND pipeline repository.
    pipeline_version : str, optional
        The version of the pipeline to use, which will be used to checkout a branch of the pipeline repository.
        Default is "v1.0.0-fixes".
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
    if pipeline_version == "v1.0.0":
        message = (
            "Version `v1.0.0` is incompatible with the new parameters file usage." "Please use `v1.0.0-fixes` instead."
        )
        raise ValueError(message)
    config_file_path = config_file_path or pathlib.Path(__file__).parent / "configs" / "mit_engaging.config"

    # TODO: remove the API key use once Dandiset is public
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)

    if parameters_key == "default":
        parameters_file_path = pathlib.Path(__file__).parent / "params" / "default_parameters.json"
    elif parameters_key == "no+motion":
        parameters_file_path = pathlib.Path(__file__).parent / "params" / "no_motion_parameters.json"
    params_id = hashlib.md5(parameters_file_path.read_bytes()).hexdigest()[0:7]
    config_id = hashlib.md5(config_file_path.read_bytes()).hexdigest()[0:7] if config_file_path else "default"

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
    # Parse BIDS entities from the filename stem only (not directory parts),
    # and skip tokens that are modality suffixes (no "-" separator, e.g. "ecephys").
    entities = {}
    for token in pathlib.Path(dandiset_path).stem.split("_"):
        if "-" not in token:
            continue
        key, value = token.split("-", 1)
        entities[key] = value

    # Special case - add date entity for testing asset
    if content_id == "048d1ee9-83b7-491f-8f02-1ca615b1d455":
        today = datetime.date.today().isoformat().replace("-", "+")
        config_id += f"_date-{today}"

    # TODO: if first run for asset, skip below and add sourcedata

    pipeline_directory = pipeline_directory or dandi_compute_dir / "aind-ephys-pipeline.cody"
    pipeline_file_path = pipeline_directory / "pipeline" / "main_multi_backend.nf"
    dandi_compute_code_source_dir = dandi_compute_dir / "code"

    pipeline_commit_hash = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=pipeline_directory,
        text=True,
    ).strip()
    if not re.match(r"^[0-9a-f]{40}$", pipeline_commit_hash):
        message = f"Unexpected commit hash format: {pipeline_commit_hash}"
        raise ValueError(message)
    pipeline_commit_head = pipeline_commit_hash[0:7]

    dandi_comptue_code_commit_hash = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=dandi_compute_code_source_dir,
        text=True,
    ).strip()
    if not re.match(r"^[0-9a-f]{40}$", dandi_comptue_code_commit_hash):
        message = f"Unexpected commit hash format: {dandi_comptue_code_commit_hash}"
        raise ValueError(message)
    dandi_compute_code_source_commit_head = dandi_comptue_code_commit_hash[0:7]

    bidsy_pipeline_version = pipeline_version.replace("-", "+")
    output_dandiset_path_base = f"derivatives/dandiset-{dandiset_id}/sub-{entities['sub']}/"
    if "ses" in entities:
        output_dandiset_path_base += f"ses-{entities['ses']}/"
    output_dandiset_path_base += (
        f"pipeline-aind+ephys/version-{bidsy_pipeline_version}+{pipeline_commit_head}/"
        f"params-{params_id}_config-{config_id}"
    )

    # Assign the lowest integer run ID that has not been used yet, up to a maximum limit
    client = dandi.dandiapi.DandiAPIClient(token=os.environ["DANDI_API_KEY"])
    dandiset = client.get_dandiset(dandiset_id="001697")

    maximum_run_id = 99
    run_id = 1
    output_dandiset_path = f"{output_dandiset_path_base}_attempt-{run_id}"
    for _ in range(maximum_run_id + 1):
        assets_checker = dandiset.get_assets_with_path_prefix(path=output_dandiset_path)
        if next(assets_checker, None) is None:
            continue

        run_id += 1
        output_dandiset_path = f"{output_dandiset_path_base}_attempt-{run_id}"

    blob_head = content_id[0]
    partition = "001" if ord(blob_head) - ord("0") <= 8 else "002"  # TODO: pull from source to keep up to date
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
    code_pipeline_file_path = code_dir / pipeline_file_path.name
    code_capsule_versions_file_path = code_dir / "capsule_versions.env"
    dataset_description_file_path = dandiset_output_dir / "dataset_description.json"

    log_directory = dandiset_output_dir / "logs"

    code_dir.mkdir(parents=True)
    log_directory.mkdir()
    intermediate_dir = dandiset_output_dir / "intermediate"
    intermediate_dir.mkdir()

    work_directory = dandi_compute_dir / "work"
    apptainer_cache_directory = work_directory / "apptainer_cache"
    # NOTE: NUMBA_CACHE_DIR is also needed for the pipeline
    # but must be set in `~/.bashrc`, and must be the same as WORKDIR
    # NUMBA_CACHE_DIR = "/orcd/data/dandi/001/dandi-compute/work"
    environment_directory = "/orcd/data/dandi/001/environments/name-nextflow_environment"
    done_tracker_file_path = processing_directory / "done.txt"

    pipeline_repo_directory = pipeline_file_path.parent.parent
    capsule_versions_file_path = pipeline_repo_directory / "pipeline" / "capsule_versions.env"

    # TODO: could look up description, authors, license, etc. from source dandiset metadata
    pipeline_url = f"https://github.com/CodyCBakerPhD/aind-ephys-pipeline/tree/{pipeline_version.replace('+','%2B')}"
    dataset_description = {
        "Name": f"DANDI Compute: AIND Ephys pipeline output for Dandiset {dandiset_id}",
        "BIDSVersion": "1.10",
        "DatasetType": "study",
        "GeneratedBy": [
            {
                "Name": "AIND Ephys Pipeline",
                "Description": "A customized and version-locked branch of the main AIND ephys pipeline.",
                "Version": pipeline_version,
                "CodeURL": pipeline_url,
            },
            {
                "Name": "DANDI Compute: Code",
                "Description": "The primary source code for orchestration of AIND on MIT Engaging.",
                "Version": dandi_compute_code_source_commit_head,
                "CodeURL": "https://github.com/dandi-compute/code",
            },
        ],
        "SourceDatasets": [{"URL": f"https://dandiarchive.org/dandiset/{dandiset_id}/"}],
    }

    # Construct submission script from template
    generate_aind_ephys_submission_script(
        script_file_path=script_file_path,
        log_directory=str(log_directory),
        nwb_file_path=nwbfile_path,
        results_directory=str(intermediate_dir),  # Start off the results in the intermediate folder and separate later
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
    code_pipeline_file_path.write_text(data=pipeline_file_path.read_text())
    dataset_description_file_path.write_text(data=json.dumps(obj=dataset_description, indent=2))

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
