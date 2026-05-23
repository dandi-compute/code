import pathlib
import re
from unittest import mock

import pytest

from dandi_compute_code.aind_ephys_pipeline import submit_job


@pytest.mark.ai_generated
def test_submit_job_warns_with_sbatch_script_path(tmp_path: pathlib.Path) -> None:
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
        pytest.warns(UserWarning, match=re.escape(f"Submitting sbatch script: {script_file_path}")),
    ):
        submit_job(script_file_path=script_file_path)

    mock_run.assert_called_once_with(
        ["sbatch", str(script_file_path)],
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
