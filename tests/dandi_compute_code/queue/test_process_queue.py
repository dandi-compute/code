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
def test_process_queue_handles_empty_scan_when_waiting_file_missing(tmp_path: pathlib.Path) -> None:
    """process_queue raises when state.jsonl is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="State file not found"):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )


@pytest.mark.ai_generated
def test_process_queue_refreshes_state_when_empty(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    """process_queue logs and returns when state.jsonl is empty."""
    queue_dir = _make_queue_dir(tmp_path)
    (queue_dir / "state.jsonl").write_text("")
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._process_queue"),
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_not_called()
    assert any("No entries in" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_process_queue_skips_refresh_when_state_non_empty(tmp_path: pathlib.Path) -> None:
    """process_queue runs without warning when state.jsonl already has entries."""
    queue_dir = _make_queue_dir(tmp_path)
    entry = _make_state_entry()
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_skips_when_lock_is_already_held(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """process_queue skips submission when another process holds the lock."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._process_queue"),
        mock.patch(
            "dandi_compute_code.queue._process_queue.fcntl.flock",
            side_effect=BlockingIOError,
        ),
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs") as mock_running,
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    assert any("Skipping queue processing: lock already held" in record.message for record in caplog.records)
    mock_running.assert_not_called()
    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue requests two submissions when no AIND jobs are running."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=dandiset_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=2,
    )


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue does not submit when two AIND-Ephys-Pipeline jobs already run."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_one_when_one_job_running(tmp_path: pathlib.Path) -> None:
    """process_queue requests one submission when exactly one AIND-Ephys-Pipeline job is running."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=1),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=dandiset_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=1,
    )


@pytest.mark.ai_generated
def test_process_queue_passes_datalad_directory_to_submit_next_when_matching_dandiset(
    tmp_path: pathlib.Path,
) -> None:
    """process_queue forwards datalad_directory to _submit_next when idle."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=dandiset_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=2,
    )


@pytest.mark.ai_generated
def test_process_queue_passes_datalad_directory_to_submit_next(tmp_path: pathlib.Path) -> None:
    """process_queue forwards explicit datalad_directory to _submit_next when idle."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()
    datalad_dir = tmp_path / "datalad"
    datalad_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(queue_directory=queue_dir, dandiset_directory=dandiset_dir, datalad_directory=datalad_dir)

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=datalad_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=2,
    )
