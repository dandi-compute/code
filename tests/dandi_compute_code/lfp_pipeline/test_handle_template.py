import pytest

from dandi_compute_code.lfp_pipeline import generate_lfp_submission_script


def _render(tmp_path) -> str:
    script_file_path = tmp_path / "submit.sh"
    generate_lfp_submission_script(
        script_file_path=script_file_path,
        log_directory="/logs",
        dataset_directory="/dataset",
        environment_directory="/envs/name-lfp",
        container_name="dandi-compute-lfp",
        container_image="ghcr.io/dandi-compute/dandi-compute-lfp:latest",
        nwb_file_path="/data/blobs/abc/def/0123456789",
        output_nwb_file_path="/dataset/derivatives/lfp/sub-01_lfp",
        parameters_key="default",
        temp_name="prepare-job-xyz",
        done_tracker_file_path="/processing/done.txt",
    )
    return script_file_path.read_text()


@pytest.mark.ai_generated
def test_rendered_script_has_sbatch_header(tmp_path) -> None:
    script = _render(tmp_path)

    assert script.startswith("#!/bin/bash")
    assert "#SBATCH --job-name=LFP-Pipeline" in script
    assert "#SBATCH --output=/logs/job-%j_slurm.log" in script


@pytest.mark.ai_generated
def test_rendered_script_uses_datalad_containers_and_duct(tmp_path) -> None:
    script = _render(tmp_path)

    assert "duct --output-prefix" in script
    assert "datalad containers-run" in script
    assert "datalad containers-add" in script
    assert '--container-name "dandi-compute-lfp"' in script
    assert "docker://ghcr.io/dandi-compute/dandi-compute-lfp:latest" in script


@pytest.mark.ai_generated
def test_rendered_script_invokes_pipeline_on_input_path(tmp_path) -> None:
    script = _render(tmp_path)

    assert "python -m dandi_compute_code.lfp_pipeline" in script
    assert "--input '/data/blobs/abc/def/0123456789'" in script
    assert "--output '/dataset/derivatives/lfp/sub-01_lfp'" in script
    assert "--params 'default'" in script


@pytest.mark.ai_generated
def test_rendered_script_records_done_tracker(tmp_path) -> None:
    script = _render(tmp_path)

    assert 'echo "prepare-job-xyz" >> /processing/done.txt' in script
