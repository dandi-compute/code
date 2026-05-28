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
def test_order_queue_raises_when_queue_config_missing(tmp_path: pathlib.Path) -> None:
    """write_queue_state raises FileNotFoundError when queue_config.json is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="queue_config.json"):
        write_queue_state(queue_directory=queue_dir)


@pytest.mark.ai_generated
def test_order_queue_writes_waiting_jsonl_from_state_entries(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes state entries emitted by write_queue_state."""
    queue_dir = _make_queue_dir(tmp_path)
    expected_dandiset_id = "123456"

    content_id_to_asset = {
        "id-1": {
            "path": "sub-01/sub-01_ecephys.nwb",
            "contentSize": 1,
            "blobDateModified": "2024-01-01T00:00:00+00:00",
        },
        "id-2": {
            "path": "sub-02/sub-02_ecephys.nwb",
            "contentSize": 2,
            "blobDateModified": "2024-01-02T00:00:00+00:00",
        },
    }
    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset=content_id_to_asset,
                path_to_asset_metadata={
                    "sub-01/sub-01_ecephys.nwb": AssetMetadata(
                        path="sub-01/sub-01_ecephys.nwb",
                        date_modified="2024-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="id-1",
                    ),
                    "sub-02/sub-02_ecephys.nwb": AssetMetadata(
                        path="sub-02/sub-02_ecephys.nwb",
                        date_modified="2024-01-02T00:00:00+00:00",
                        content_size=2,
                        content_id="id-2",
                    ),
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._ASSETS_JSONLD_URL",
            f"https://example.test/dandisets/{expected_dandiset_id}/draft/assets.jsonld",
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    state_entries = _read_jsonl(state_file)
    assert len(state_entries) == 2
    assert all(entry["dandiset_id"] == expected_dandiset_id for entry in state_entries)


@pytest.mark.ai_generated
def test_order_queue_returns_ordered_pending_entries_only() -> None:
    """order_queue returns pending entries without writing files."""
    state_entries = [
        _make_state_entry(dandiset_id="000001", has_code=True, has_output=False, has_logs=False),
        _make_state_entry(dandiset_id="000002", has_code=True, has_output=True, has_logs=False),
    ]
    queue_config = {"pipelines": {"test": {"version_priority": ["v1.0"], "params_priority": ["default"]}}}

    ordered = order_queue(state_entries=state_entries, queue_config=queue_config)

    assert len(ordered) == 1
    assert ordered[0]["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_order_queue_respects_limit_parameter() -> None:
    """order_queue truncates ordered entries when limit is provided."""
    state_entries = [
        _make_state_entry(dandiset_id=f"00000{i}", has_code=True, has_output=False, has_logs=False) for i in range(1, 6)
    ]
    queue_config = {"pipelines": {"test": {"version_priority": ["v1.0"], "params_priority": ["default"]}}}

    ordered = order_queue(state_entries=state_entries, queue_config=queue_config, limit=2)

    assert len(ordered) == 2
