from unittest import mock

import pytest

from dandi_compute_code.queue._count_running_aind_ephys_pipeline_jobs import _count_running_aind_ephys_pipeline_jobs


@pytest.mark.ai_generated
def test_count_running_aind_ephys_pipeline_jobs_counts_exact_name_matches() -> None:
    """Counts only exact AIND-Ephys-Pipeline job names from squeue output."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(
            stdout="JOBNAME\nAIND-Ephys-Pipeline\nAIND-Ephys-Pipeline\nother_job\nAIND_ephys_job\n",
            stderr="",
        )
        assert _count_running_aind_ephys_pipeline_jobs() == 2


@pytest.mark.ai_generated
def test_count_running_aind_ephys_pipeline_jobs_returns_zero_when_no_exact_match() -> None:
    """Returns zero when squeue output has no exact AIND-Ephys-Pipeline names."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(stdout="JOBNAME\nsome_other_job\nAIND\n", stderr="")
        assert _count_running_aind_ephys_pipeline_jobs() == 0
