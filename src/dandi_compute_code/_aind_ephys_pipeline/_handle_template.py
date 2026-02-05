import os
import pathlib
import tempfile
import subprocess

import pydantic
import dandi
import dandi.dandiarchive
import dandi.download
import dandi.dandiapi
import jinja2

from ._globals import _RAW_TEMPLATE_FILE_PATH

@pydantic.validate_call
def generate_aind_ephys_submission_script(
    blob_id: str,
    config_file_path: pathlib.Path | None = None,
    pipeline_file_path: pathlib.Path | None = None,
# LOG_PATH={{ log_file_path }}
# PIPELINE_PATH={{ pipeline_path }}
# PIPELINE_PATH="$DANDI_COMPUTE_BASE_DIR/aind-ephys-pipeline.cody"
) -> None:
    """
    Generate AIND Ephys submission script.

    Parameters
    ----------
    blob_id : str
        The blob ID for the job.
    pipeline_file_path : pathlib.Path, optional
        The path to the pipeline file.
    """
    # TODO: remove the API key use once Dandiset is public
    if "DANDI_API_KEY" is not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)
    client = dandi.dandiapi.DandiAPIClient(token=os.environ["DANDI_API_KEY"])
    dandiset = client.get_dandiset(dandiset_id="001697")

    content_id = blob_id[:8]

    # TODO: if first run for asset, skip below and add sourcedata

    # Assign lowest integer run ID that has not been used yet, up to a maximum limit
    maximum_run_id = 99
    run_id = 1
    dandiset_path = f"derivatives/pipeline-aind+ephys/derivatives/asset-{content_id}/result-{run_id}"
    for _ in range(maximum_run_id+1):
        assets_checker = dandiset.get_assets_with_path_prefix(path=dandiset_path)
        if next(assets_checker, None) is None:
            continue

        run_id += 1
        dandiset_path = f"derivatives/pipeline-aind+ephys/derivatives/asset-{content_id}/result-{run_id}"

    # TODO: update default path once upstream hashes have been updated
    base_dandi_dir = "/orcd/data/dandi/001"
    config_file_path = config_file_path or pathlib.Path(
        f"{base_dandi_dir}/dandi-compute/aind-ephys-pipeline.cody/default.config"
    )
    pipeline_file_path = pipeline_file_path or pathlib.Path(
        f"{base_dandi_dir}/dandi-compute/aind-ephys-pipeline.cody/pipeline/main_multi_backend.nf"
    )
    blob_head = blob_id[0]
    partition = "001" if ord(blob_head) - ord("0") < 10 else "002"
    nwbfile_path = "$DANDI_ARCHIVE_DIR/blobs/${BLOB_ID:0:3}/${BLOB_ID:3:3}/$BLOB_ID"

    # Create empty copy of Dandiset
    processing_dir = "/orcd/data/dandi/001/dandi-compute/processing"
    temp_processing_dir = pathlib.Path(tempfile.mkdtemp(dir=processing_dir))
    dandi.download.download(
        urls="DANDI:001697",
        output_dir=temp_processing_dir,
        get_metadata=True,
        get_assets=False,
    )

    # Construct BIDS derivative content
    dandiset_results_dir = temp_processing_dir / "001697" / dandiset_path
    dandiset_results_dir.mkdir(parents=True)

    code_dir = dandiset_results_dir / "code"
    code_dir.mkdir()
    script_file_path = code_dir / "submit.sh"
    code_config_file_path = code_dir / config_file_path.name
    code_config_file_path.write_text(data=config_file_path.read_text())
    (dandiset_results_dir / "intermediate").mkdir()
    log_directory = dandiset_results_dir / "logs"
    log_directory.mkdir()
    (dandiset_results_dir / "output").mkdir()

    # Construct submission script from template
    raw_template = _RAW_TEMPLATE_FILE_PATH.read_text()
    template = jinja2.Template(source=raw_template)
    script = template.render(
        log_directory=log_directory,
        blob_id=blob_id,
        run_id=run_id,
        partition=partition,
        config_file_path=config_file_path,
        pipeline_file_path=pipeline_file_path,
        nwbfile_path=nwbfile_path,
    )
    script_file_path.write_text(data=script)

    subprocess.run(["sbatch", str(script_file_path)])
