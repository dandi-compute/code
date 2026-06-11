import logging
import pathlib
import re
from unittest import mock

import pytest

from dandi_compute_code.aind_ephys_pipeline import submit_job


def _prepare_submit_script_path(*, tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    relative_derivatives_subpath = pathlib.Path("sub-mouse01") / "pipeline-test" / "version-v1.0" / "code"
    script_file_path = tmp_path / "001697" / "derivatives" / relative_derivatives_subpath / "submit.sh"
    script_file_path.parent.mkdir(parents=True, exist_ok=True)
    script_file_path.write_text("#!/usr/bin/env bash\n")
    return script_file_path, relative_derivatives_subpath


@pytest.mark.ai_generated
def test_submit_job_logs_sbatch_script_path(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    script_file_path, relative_derivatives_subpath = _prepare_submit_script_path(tmp_path=tmp_path)
    download_dir = tmp_path / "download"
    (download_dir / "001697" / "derivatives" / relative_derivatives_subpath).mkdir(parents=True, exist_ok=True)

    sbatch_process = mock.Mock(
        returncode=0,
        stdout="Submitted batch job 123\n",
        stderr="",
    )
    download_process = mock.Mock(returncode=0, stdout="", stderr="")
    upload_process = mock.Mock(returncode=0, stdout="", stderr="")

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "fake-key"}, clear=True),
        mock.patch(
            "subprocess.run",
            side_effect=[sbatch_process, download_process, upload_process],
        ) as mock_run,
        mock.patch("tempfile.mkdtemp", return_value=str(download_dir)),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._submit_job.pathlib.Path.mkdir"),
        caplog.at_level(logging.INFO, logger="dandi_compute_code.aind_ephys_pipeline._submit_job"),
    ):
        submit_job(script_file_path=script_file_path)

    log_messages = [record.message for record in caplog.records]
    assert log_messages[0] == f"Submitting sbatch script: {script_file_path}"
    assert log_messages[1] == "sbatch returned code: 0\nstdout: Submitted batch job 123\n\nstderr: "
    assert mock_run.call_args_list[0] == mock.call(
        ["sbatch", str(script_file_path)],
        capture_output=True,
        text=True,
    )


@pytest.mark.ai_generated
def test_submit_job_logs_absolute_sbatch_script_path_for_relative_input(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.chdir(tmp_path)
    relative_script_file_path = pathlib.Path("001697/derivatives/sub-mouse01/pipeline-test/version-v1.0/code/submit.sh")
    relative_script_file_path.parent.mkdir(parents=True, exist_ok=True)
    relative_script_file_path.write_text("#!/usr/bin/env bash\n")
    absolute_script_file_path = relative_script_file_path.absolute()
    relative_derivatives_subpath = pathlib.Path("sub-mouse01") / "pipeline-test" / "version-v1.0" / "code"
    download_dir = tmp_path / "download"
    (download_dir / "001697" / "derivatives" / relative_derivatives_subpath).mkdir(parents=True, exist_ok=True)

    sbatch_process = mock.Mock(
        returncode=0,
        stdout="Submitted batch job 123\n",
        stderr="",
    )
    download_process = mock.Mock(returncode=0, stdout="", stderr="")
    upload_process = mock.Mock(returncode=0, stdout="", stderr="")

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "fake-key"}, clear=True),
        mock.patch(
            "subprocess.run",
            side_effect=[sbatch_process, download_process, upload_process],
        ) as mock_run,
        mock.patch("tempfile.mkdtemp", return_value=str(download_dir)),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._submit_job.pathlib.Path.mkdir"),
        caplog.at_level(logging.INFO, logger="dandi_compute_code.aind_ephys_pipeline._submit_job"),
    ):
        submit_job(script_file_path=relative_script_file_path)

    log_messages = [record.message for record in caplog.records]
    assert log_messages[0] == f"Submitting sbatch script: {absolute_script_file_path}"
    assert log_messages[1] == "sbatch returned code: 0\nstdout: Submitted batch job 123\n\nstderr: "
    assert mock_run.call_args_list[0] == mock.call(
        ["sbatch", str(absolute_script_file_path)],
        capture_output=True,
        text=True,
    )


@pytest.mark.ai_generated
def test_submit_job_raises_when_dandi_api_key_missing(tmp_path: pathlib.Path) -> None:
    script_file_path = tmp_path / "submit.sh"
    script_file_path.write_text("#!/usr/bin/env bash\n")

    with (
        mock.patch.dict("os.environ", {}, clear=True),
        pytest.raises(RuntimeError, match="`DANDI_API_KEY` environment variable is not set"),
    ):
        submit_job(script_file_path=script_file_path)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("stdout", "stderr"),
    [
        ("Submitted batch job 123\n", "sbatch: error: invalid account or account/partition combination specified\n"),
        ("", ""),
    ],
)
def test_submit_job_raises_with_relayed_streams_on_nonzero_exit(
    tmp_path: pathlib.Path,
    stdout: str,
    stderr: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script_file_path = tmp_path / "submit.sh"
    script_file_path.write_text("#!/usr/bin/env bash\n")
    completed_process = mock.Mock(returncode=1, stdout=stdout, stderr=stderr)

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "fake-key"}, clear=True),
        mock.patch("subprocess.run", return_value=completed_process),
        caplog.at_level(logging.INFO, logger="dandi_compute_code.aind_ephys_pipeline._submit_job"),
        pytest.raises(RuntimeError) as exc_info,
    ):
        submit_job(script_file_path=script_file_path)

    log_messages = [record.message for record in caplog.records]
    assert log_messages == [
        f"Submitting sbatch script: {script_file_path}",
        f"sbatch returned code: 1\nstdout: {stdout}\nstderr: {stderr}",
    ]
    assert str(exc_info.value) == "sbatch submission failed - please check the logs to see more details."


@pytest.mark.ai_generated
def test_submit_job_writes_integer_timestamped_submitted_marker(tmp_path: pathlib.Path) -> None:
    script_file_path = tmp_path / "submit.sh"
    script_file_path.write_text("#!/usr/bin/env bash\n")
    sbatch_process = mock.Mock(returncode=0, stdout="Submitted batch job 123\n", stderr="")
    upload_process = mock.Mock(returncode=0, stdout="", stderr="")

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "fake-key"}, clear=True),
        mock.patch("subprocess.run", side_effect=[sbatch_process, upload_process]),
    ):
        submit_job(script_file_path=script_file_path)

    marker_files = list(script_file_path.parent.glob("submitted_date-*"))
    assert len(marker_files) == 1
    submitted_marker = marker_files[0]
    assert re.fullmatch(
        r"submitted_date-\d{4}\+\d{2}\+\d{2}_time-\d{2}\+\d{2}\+\d{2}",
        submitted_marker.name,
    )
    assert submitted_marker.read_bytes() == b"1"
