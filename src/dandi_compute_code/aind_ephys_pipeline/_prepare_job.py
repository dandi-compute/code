import os
import pathlib
import tempfile

import dandi
import dandi.dandiapi
import dandi.dandiarchive
import dandi.download
import dandi.upload
import pydantic

from ._handle_template import generate_aind_ephys_submission_script


@pydantic.validate_call
def prepare_aind_ephys_job(
    content_id: str,
    config_file_path: pathlib.Path | None = None,
    pipeline_file_path: pathlib.Path | None = None,
) -> pathlib.Path:
    """
    Prepares an AIND ephys job by generating a submission script and returning the script file path.

    Parameters
    ----------
    content_id : str
        The content ID for the data to be processed.
    config_file_path : pathlib.Path, optional
        Path to the configuration file.
    pipeline_file_path : pathlib.Path, optional
        Path to the pipeline file.

    Returns
    -------
    script_file_path : pathlib.Path
        The path to the generated submission script.
    """
    # TODO: remove the API key use once Dandiset is public
    if "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)
    client = dandi.dandiapi.DandiAPIClient(token=os.environ["DANDI_API_KEY"])
    dandiset = client.get_dandiset(dandiset_id="001697")

    short_content_id = content_id[:8]

    # TODO: if first run for asset, skip below and add sourcedata

    # Assign the lowest integer run ID that has not been used yet, up to a maximum limit
    maximum_run_id = 99
    run_id = 1
    dandiset_path = f"derivatives/pipeline-aind+ephys/derivatives/asset-{short_content_id}/result-{run_id}"
    for _ in range(maximum_run_id + 1):
        assets_checker = dandiset.get_assets_with_path_prefix(path=dandiset_path)
        if next(assets_checker, None) is None:
            continue

        run_id += 1
        dandiset_path = f"derivatives/pipeline-aind+ephys/derivatives/asset-{short_content_id}/result-{run_id}"

    # TODO: update default path once upstream hashes have been updated
    dandi_compute_dir = pathlib.Path("/orcd/data/dandi/001/dandi-compute")

    config_file_path = config_file_path or pathlib.Path(__file__).parent / "mit_engaging.config"
    pipeline_file_path = (
        pipeline_file_path or dandi_compute_dir / "aind-ephys-pipeline.cody/pipeline/main_multi_backend.nf"
    )
    blob_head = content_id[0]
    partition = "001" if ord(blob_head) - ord("0") < 10 else "002"
    nwbfile_path = f"/orcd/data/dandi/{partition}/s3dandiarchive/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}"
    # TODO: figure out if Zarr or not - only supports blobs ATM

    # Create an empty copy of Dandiset
    processing_directory = f"{dandi_compute_dir}/processing"
    temporary_processing_directory = pathlib.Path(tempfile.mkdtemp(dir=processing_directory))
    dandi.download.download(
        urls="DANDI:001697",
        output_dir=temporary_processing_directory,
        get_metadata=True,
        get_assets=False,
    )

    # Construct BIDS derivative content
    dandiset_results_dir = temporary_processing_directory / "001697" / dandiset_path

    code_dir = dandiset_results_dir / "code"
    script_file_path = code_dir / "submit.sh"
    code_config_file_path = code_dir / config_file_path.name

    log_directory = dandiset_results_dir / "logs"

    code_dir.mkdir(parents=True)
    log_directory.mkdir()
    intermediate_dir = dandiset_results_dir / "intermediate"
    intermediate_dir.mkdir()
    (dandiset_results_dir / "output").mkdir()

    results_directory = intermediate_dir  # Start off the results in the intermediate folder and separate later
    work_directory = dandi_compute_dir / "work"
    apptainer_cache_directory = work_directory / "apptainer_cache"
    # NOTE: NUMBA_CACHE_DIR is also needed for the pipeline
    # but must be set in `~/.bashrc`, and must be the same as WORKDIR
    # NUMBA_CACHE_DIR = "/orcd/data/dandi/001/dandi-compute/work"
    environment_directory = "/orcd/data/dandi/001/environments/name-nextflow_environment"
    done_tracker_file_path = processing_directory / "done.txt"

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
        temp_name=temporary_processing_directory.name,
        done_tracker_file_path=done_tracker_file_path,
    )
    code_config_file_path.write_text(data=config_file_path.read_text())

    # Upload preparation to 'reserve' spot during processing
    dandi.upload.upload(
        paths=[dandiset_results_dir],
        allow_any_path=True,
        validation=dandi.upload.UploadValidation.SKIP,
    )

    return script_file_path
