import logging
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.aind_ephys_pipeline import submit_job


@pytest.mark.ai_generated
def test_submit_job_logs_sbatch_script_path(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    script_file_path = tmp_path / "submit.sh"
    script_file_path.write_text("#!/usr/bin/env bash\n")

    completed_process = mock.Mock(
        returncode=0,
        stdout="Submitted batch job 123\n",
        stderr="",
    )

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "fake-key"}, clear=True),
        mock.patch("subprocess.run", return_value=completed_process) as mock_run,
        caplog.at_level(logging.INFO, logger="dandi_compute_code.aind_ephys_pipeline._submit_job"),
    ):
        submit_job(script_file_path=script_file_path)

    log_messages = [record.message for record in caplog.records]
    assert log_messages == [
        f"Submitting sbatch script: {script_file_path}",
        "sbatch return code: 0\nstdout: Submitted batch job 123\n\nstderr: ",
    ]
    mock_run.assert_called_once_with(
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
    relative_script_file_path = pathlib.Path("submit.sh")
    relative_script_file_path.write_text("#!/usr/bin/env bash\n")
    absolute_script_file_path = relative_script_file_path.absolute()

    completed_process = mock.Mock(
        returncode=0,
        stdout="Submitted batch job 123\n",
        stderr="",
    )

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "fake-key"}, clear=True),
        mock.patch("subprocess.run", return_value=completed_process) as mock_run,
        caplog.at_level(logging.INFO, logger="dandi_compute_code.aind_ephys_pipeline._submit_job"),
    ):
        submit_job(script_file_path=relative_script_file_path)

    log_messages = [record.message for record in caplog.records]
    assert log_messages == [
        f"Submitting sbatch script: {absolute_script_file_path}",
        "sbatch return code: 0\nstdout: Submitted batch job 123\n\nstderr: ",
    ]
    mock_run.assert_called_once_with(
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
        f"sbatch return code: 1\nstdout: {stdout}\nstderr: {stderr}",
    ]
    assert f"stdout: {stdout}" in str(exc_info.value)
    assert f"stderr: {stderr}" in str(exc_info.value)
