# ruff: noqa: F821
import importlib.util as _importlib_util
import pathlib as _pathlib

_spec = _importlib_util.spec_from_file_location(
    "_process_queue_test_cases",
    _pathlib.Path(__file__).with_name("_process_queue_test_cases.py"),
)
assert _spec is not None
assert _spec.loader is not None
_support = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_support)

globals().update(
    {
        name: value
        for name, value in vars(_support).items()
        if not name.startswith("__") and not name.startswith("test_")
    }
)


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
