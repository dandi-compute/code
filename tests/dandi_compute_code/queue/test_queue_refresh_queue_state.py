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
def test_refresh_queue_state_raises_when_queue_config_fails_linkml_validation(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state raises when queue_config violates LinkML constraints."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    invalid_queue_config = {
        "pipelines": {
            # Violates schema minimum_value: 0 constraint.
            "test": {"version_priority": ["v1.0"], "params_priority": ["default"], "max_attempts_per_asset": -1}
        }
    }
    (queue_dir / "queue_config.json").write_text(json.dumps(invalid_queue_config))

    with (
        mock.patch("dandi_compute_code.queue._refresh_queue.scan_dandiset_directory", return_value=[]),
        pytest.raises(ValueError, match="LinkML validation failed"),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_refresh_queue_state_writes_empty_files_for_missing_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes an empty state file when dandiset directory does not exist."""
    queue_dir = _make_queue_dir(tmp_path)
    missing_dandiset_dir = tmp_path / "missing_dandiset"

    refresh_queue_state(queue_directory=queue_dir, dandiset_directory=missing_dandiset_dir)

    assert (queue_dir / "state.jsonl").read_text() == ""


@pytest.mark.ai_generated
def test_refresh_queue_state_writes_all_ordered_pending_entries(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes all scanned entries to state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)

    entries = [
        _make_state_entry(dandiset_id=f"00000{i}", has_code=True, has_output=False, has_logs=False) for i in range(1, 6)
    ]
    with mock.patch("dandi_compute_code.queue._refresh_queue.scan_dandiset_directory", return_value=entries):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 5


@pytest.mark.ai_generated
def test_refresh_queue_state_excludes_entries_with_submitted_markers(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes submitted-marker entries into state.jsonl unchanged."""
    queue_dir = _make_queue_dir(tmp_path)

    pending_entry = _make_state_entry(dandiset_id="000001", has_code=True, has_output=False, has_logs=False)
    submitted_entry = _make_state_entry(dandiset_id="000002", has_code=True, has_output=False, has_logs=False)
    submitted_attempt_dir = _make_attempt_dir_with_script(
        tmp_path,
        dandiset_id="000002",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    (submitted_attempt_dir / "code" / "submitted").touch()

    with mock.patch(
        "dandi_compute_code.queue._refresh_queue.scan_dandiset_directory",
        return_value=[pending_entry, submitted_entry],
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 2
    assert {entry["dandiset_id"] for entry in state_entries} == {"000001", "000002"}
