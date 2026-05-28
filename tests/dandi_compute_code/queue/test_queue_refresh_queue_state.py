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

    with pytest.raises(ValueError, match="LinkML validation failed"):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_refresh_queue_state_writes_empty_files_for_missing_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state does not require local dandiset scan."""
    queue_dir = _make_queue_dir(tmp_path)
    missing_dandiset_dir = tmp_path / "missing_dandiset"

    with mock.patch("dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata", return_value=({}, {})):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=missing_dandiset_dir)
    assert (queue_dir / "state.jsonl").exists()
    assert (queue_dir / "state.jsonl").read_text() == ""


@pytest.mark.ai_generated
def test_refresh_queue_state_writes_all_ordered_pending_entries(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes ordered pending entries from metadata."""
    queue_dir = _make_queue_dir(tmp_path)
    content_id_to_asset = {
        f"id-{i}": {
            "path": f"sub-{i:02d}/sub-{i:02d}_ecephys.nwb",
            "contentSize": i,
            "blobDateModified": "2024-01-01T00:00:00+00:00",
        }
        for i in range(1, 6)
    }
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=(content_id_to_asset, {}),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 5
    assert all(not entry["has_code"] and not entry["has_output"] and not entry["has_logs"] for entry in state_entries)


@pytest.mark.ai_generated
def test_refresh_queue_state_excludes_entries_with_submitted_markers(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state no longer depends on local submitted marker files."""
    queue_dir = _make_queue_dir(tmp_path)
    (tmp_path / "sub-mouse01" / "code").mkdir(parents=True)
    (tmp_path / "sub-mouse01" / "code" / "submitted").write_text("")
    content_id_to_asset = {
        "id-1": {
            "path": "sub-mouse01/sub-mouse01_ecephys.nwb",
            "contentSize": 1234,
            "blobDateModified": "2024-01-01T00:00:00+00:00",
        }
    }
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=(content_id_to_asset, {}),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)
    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
