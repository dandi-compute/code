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


def _make_metadata_with_submit_sh(*code_dir_paths: str) -> AssetsJsonldMetadata:
    """Return metadata containing a ``code/submit.sh`` asset for each given path."""
    path_to_asset_metadata = {}
    for code_dir_path in code_dir_paths:
        asset_path = f"{code_dir_path}/submit.sh"
        path_to_asset_metadata[asset_path] = AssetMetadata(
            path=asset_path,
            date_modified="2025-01-01T00:00:00+00:00",
            content_size=100,
            content_id="abc123",
        )
    return AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=path_to_asset_metadata)


def _make_metadata_with_submitted(*code_dir_paths: str) -> AssetsJsonldMetadata:
    """Return metadata with both ``code/submit.sh`` and ``code/submitted`` for each path."""
    path_to_asset_metadata = {}
    for code_dir_path in code_dir_paths:
        for filename in ("submit.sh", "submitted"):
            asset_path = f"{code_dir_path}/{filename}"
            path_to_asset_metadata[asset_path] = AssetMetadata(
                path=asset_path,
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=100,
                content_id="abc123",
            )
    return AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=path_to_asset_metadata)


def _download_side_effect(command: list, **kwargs: object) -> mock.MagicMock:
    """subprocess.run side effect that creates submit.sh on dandi download."""
    result = mock.MagicMock()
    result.returncode = 0
    result.stdout = ""
    result.stderr = ""
    if len(command) >= 4 and command[0] == "dandi" and command[1] == "download":
        url = command[-1]
        _, code_dir_path = url.split("/001697/", 1)
        cwd = pathlib.Path(str(kwargs.get("cwd", ".")))
        submit_sh = cwd / "001697" / code_dir_path / "submit.sh"
        submit_sh.parent.mkdir(parents=True, exist_ok=True)
        submit_sh.write_text("#!/bin/bash\necho test\n")
    return result


_EXAMPLE_CODE_DIR_PATH = (
    "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys"
    "/pipeline-aind+ephys/version-v1.0_params-default_config-abc123_attempt-1/code"
)


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_max_submissions_less_than_one(tmp_path: pathlib.Path) -> None:
    """_submit_next returns False for invalid max_submissions before loading metadata."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    result = _submit_next(processing_directory=processing_dir, max_submissions=0)

    assert result is False


@pytest.mark.ai_generated
def test_submit_next_returns_false_and_logs_when_no_eligible_entries(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """_submit_next returns False when metadata has no code/submit.sh paths."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._submit_next"),
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
    ):
        result = _submit_next(processing_directory=processing_dir)

    assert result is False
    assert any("No eligible pending entries" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_all_submit_sh_have_submitted_marker(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """_submit_next returns False when every code/submit.sh has an adjacent code/submitted."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    metadata = _make_metadata_with_submitted(_EXAMPLE_CODE_DIR_PATH)

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._submit_next"),
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
    ):
        result = _submit_next(processing_directory=processing_dir)

    assert result is False
    assert any("No eligible pending entries" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_submit_next_calls_dandi_download_for_unsubmitted_candidate(tmp_path: pathlib.Path) -> None:
    """_submit_next calls dandi download --preserve-tree with the correct DANDI URL."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    metadata = _make_metadata_with_submit_sh(_EXAMPLE_CODE_DIR_PATH)

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree"),
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        _submit_next(processing_directory=processing_dir)

    expected_url = f"dandi://dandi/001697/{_EXAMPLE_CODE_DIR_PATH}/"
    download_call = mock_run.call_args_list[0]
    assert download_call.args[0] == ["dandi", "download", "--preserve-tree", expected_url]
    assert download_call.kwargs.get("cwd") == fixed_temp_dir


@pytest.mark.ai_generated
def test_submit_next_calls_sbatch_with_submit_sh_path(tmp_path: pathlib.Path) -> None:
    """_submit_next calls sbatch with the absolute path to submit.sh in the temp dir."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    metadata = _make_metadata_with_submit_sh(_EXAMPLE_CODE_DIR_PATH)

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree"),
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        _submit_next(processing_directory=processing_dir)

    sbatch_call = mock_run.call_args_list[1]
    expected_submit_sh = (fixed_temp_dir / "001697" / _EXAMPLE_CODE_DIR_PATH / "submit.sh").absolute()
    assert sbatch_call.args[0] == ["sbatch", str(expected_submit_sh)]


@pytest.mark.ai_generated
def test_submit_next_writes_submitted_marker_adjacent_to_submit_sh(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """_submit_next writes code/submitted adjacent to code/submit.sh after sbatch."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    metadata = _make_metadata_with_submit_sh(_EXAMPLE_CODE_DIR_PATH)

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._submit_next"),
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree"),
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        result = _submit_next(processing_directory=processing_dir)

    assert result is True
    submitted_marker = fixed_temp_dir / "001697" / _EXAMPLE_CODE_DIR_PATH / "submitted"
    assert submitted_marker.exists()
    assert submitted_marker.read_bytes() == b"1"
    expected_message = f"Created `submitted` file at: {submitted_marker.absolute()}"
    assert any(expected_message in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_submit_next_calls_dandi_upload_with_validation_skip(tmp_path: pathlib.Path) -> None:
    """_submit_next calls dandi upload --validation skip from the temp dir."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    metadata = _make_metadata_with_submit_sh(_EXAMPLE_CODE_DIR_PATH)

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree"),
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        _submit_next(processing_directory=processing_dir)

    upload_call = mock_run.call_args_list[2]
    assert upload_call.args[0] == ["dandi", "upload", "--validation", "skip"]
    assert upload_call.kwargs.get("cwd") == fixed_temp_dir / "001697"


@pytest.mark.ai_generated
def test_submit_next_cleans_up_temp_dir_on_success(tmp_path: pathlib.Path) -> None:
    """_submit_next removes the temporary directory when all steps succeed."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    metadata = _make_metadata_with_submit_sh(_EXAMPLE_CODE_DIR_PATH)

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        _submit_next(processing_directory=processing_dir)

    mock_rmtree.assert_called_once_with(fixed_temp_dir)


@pytest.mark.ai_generated
def test_submit_next_does_not_clean_up_temp_dir_on_download_failure(tmp_path: pathlib.Path) -> None:
    """_submit_next does not remove the temp dir when dandi download fails."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    metadata = _make_metadata_with_submit_sh(_EXAMPLE_CODE_DIR_PATH)

    failed_result = mock.MagicMock()
    failed_result.returncode = 1
    failed_result.stdout = ""
    failed_result.stderr = "error"

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run", return_value=failed_result),
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree") as mock_rmtree,
    ):
        with pytest.raises(RuntimeError, match="dandi download failed"):
            _submit_next(processing_directory=processing_dir)

    mock_rmtree.assert_not_called()


@pytest.mark.ai_generated
def test_submit_next_does_not_clean_up_temp_dir_in_test_mode(tmp_path: pathlib.Path) -> None:
    """_submit_next preserves temp dir after success when test mode is enabled."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    metadata = _make_metadata_with_submit_sh(_EXAMPLE_CODE_DIR_PATH)

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree") as mock_rmtree,
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        _submit_next(processing_directory=processing_dir, test=True)

    mock_rmtree.assert_not_called()


@pytest.mark.ai_generated
def test_submit_next_submits_up_to_max_submissions(tmp_path: pathlib.Path) -> None:
    """_submit_next submits at most max_submissions candidates."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    code_dir_paths = [
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys"
        "/pipeline-aind+ephys/version-v1.0_params-default_config-abc123_attempt-1/code",
        "derivatives/dandiset-001697/sub-mouse02/sub-mouse02_ecephys"
        "/pipeline-aind+ephys/version-v1.0_params-default_config-abc123_attempt-1/code",
        "derivatives/dandiset-001697/sub-mouse03/sub-mouse03_ecephys"
        "/pipeline-aind+ephys/version-v1.0_params-default_config-abc123_attempt-1/code",
    ]
    metadata = _make_metadata_with_submit_sh(*code_dir_paths)

    temp_dirs = [tmp_path / f"temp_{i}" for i in range(3)]
    for d in temp_dirs:
        d.mkdir()

    temp_dir_iter = iter([str(d) for d in temp_dirs])

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            side_effect=lambda **kw: next(temp_dir_iter),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree"),
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        result = _submit_next(processing_directory=processing_dir, max_submissions=2)

    assert result is True
    # 3 calls per submission (download, sbatch, upload) × 2 submissions = 6
    assert mock_run.call_count == 6


@pytest.mark.ai_generated
def test_submit_next_skips_candidates_with_submitted_in_metadata(tmp_path: pathlib.Path) -> None:
    """_submit_next skips code dirs where code/submitted is already in the metadata."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    fixed_temp_dir = tmp_path / "temp_work"
    fixed_temp_dir.mkdir()

    submitted_path = _EXAMPLE_CODE_DIR_PATH
    second_path = (
        "derivatives/dandiset-001697/sub-mouse02/sub-mouse02_ecephys"
        "/pipeline-aind+ephys/version-v1.0_params-default_config-abc123_attempt-1/code"
    )

    path_to_asset_metadata = {}
    # First candidate has submitted marker → should be skipped
    for filename in ("submit.sh", "submitted"):
        p = f"{submitted_path}/{filename}"
        path_to_asset_metadata[p] = AssetMetadata(
            path=p, date_modified="2025-01-01T00:00:00+00:00", content_size=1, content_id="x"
        )
    # Second candidate has only submit.sh → should be submitted
    p2 = f"{second_path}/submit.sh"
    path_to_asset_metadata[p2] = AssetMetadata(
        path=p2, date_modified="2025-01-01T00:00:00+00:00", content_size=1, content_id="y"
    )
    metadata = AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=path_to_asset_metadata)

    with (
        mock.patch(
            "dandi_compute_code.queue._submit_next.load_assets_jsonld_metadata",
            return_value=metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._submit_next.tempfile.mkdtemp",
            return_value=str(fixed_temp_dir),
        ),
        mock.patch("dandi_compute_code.queue._submit_next.subprocess.run") as mock_run,
        mock.patch("dandi_compute_code.queue._submit_next.shutil.rmtree"),
    ):
        mock_run.side_effect = lambda cmd, **kw: _download_side_effect(cmd, **kw)
        result = _submit_next(processing_directory=processing_dir, max_submissions=1)

    assert result is True
    # Only one submission happened → 3 subprocess calls
    assert mock_run.call_count == 3
    download_url = mock_run.call_args_list[0].args[0][-1]
    assert second_path in download_url
