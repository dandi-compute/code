# ruff: noqa: F821
import importlib.util as _importlib_util
import pathlib as _pathlib

_spec = _importlib_util.spec_from_file_location(
    "_scan_dandiset_test_cases",
    _pathlib.Path(__file__).with_name("_scan_dandiset_test_cases.py"),
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
def test_refresh_queue_state_with_dandiset_directory_creates_valid_files(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes state.jsonl from assets.jsonld metadata."""
    content_id = "0fbbca6a-0000-0000-0000-000000000001"
    resolved_asset_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0"],
                        "params_priority": ["abc1234"],
                    }
                }
            }
        )
    )
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=_build_assets_metadata(
            content_id=content_id,
            asset_path=resolved_asset_path,
            content_size=1234,
        ),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)
    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    lines = [line for line in state_file.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "001697"
    assert record["content_id"] == content_id
    assert record["dandi_path"] == resolved_asset_path
    assert record["asset_size_bytes"] == 1234
    assert record["has_code"] is False


@pytest.mark.ai_generated
def test_refresh_queue_state_writes_resolved_dandi_path_to_state(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes assets.jsonld-resolved source path in state.jsonl."""
    content_id = "0fbbca6a-0000-0000-0000-000000000001"
    asset_size_bytes = 1234
    resolved_asset_path = "sub-mouse01/sub-mouse01_ses-ses001_obj-raw.nwb"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0"],
                        "params_priority": ["abc1234"],
                    }
                }
            }
        )
    )

    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=_build_assets_metadata(
            content_id=content_id,
            asset_path=resolved_asset_path,
            content_size=asset_size_bytes,
        ),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["asset_size_bytes"] == asset_size_bytes
    assert state_records[0]["dandi_path"] == resolved_asset_path


@pytest.mark.ai_generated
def test_refresh_queue_state_writes_resolved_dandi_path_for_root_level_asset(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes resolved dandi_path even when matched asset path is at dandiset root."""
    content_id = "0fbbca6a-0000-0000-0000-000000000002"
    asset_size_bytes = 4321
    root_asset_path = "sub-mouse01_ses-ses001_obj-raw.nwb"

    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0"],
                        "params_priority": ["abc1234"],
                    }
                }
            }
        )
    )

    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=_build_assets_metadata(
            content_id=content_id,
            asset_path=root_asset_path,
            content_size=asset_size_bytes,
        ),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["asset_size_bytes"] == asset_size_bytes
    assert state_records[0]["dandi_path"] == root_asset_path


@pytest.mark.ai_generated
def test_refresh_queue_state_with_dandiset_directory_empty_when_no_attempts(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes an empty state file with no assets metadata."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=({}, {}),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)
    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    assert state_file.read_text() == ""


@pytest.mark.ai_generated
def test_refresh_queue_state_does_not_require_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state works when DANDI_API_KEY is not set."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    with (
        mock.patch.dict("os.environ", {}, clear=True),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
            return_value=({}, {}),
        ),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_refresh_queue_state_with_dandiset_directory_includes_only_pending_in_waiting(tmp_path: pathlib.Path) -> None:
    """state.jsonl contains all entries derived from assets metadata."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "test-pipeline": {
                        "version_priority": ["v1.0"],
                        "params_priority": ["abc1234"],
                    }
                }
            }
        )
    )

    metadata = (
        {
            "0fbbca6a-0000-0000-0000-000000000001": {
                "path": "sub-mouse01/sub-mouse01_ecephys.nwb",
                "contentSize": 11,
                "blobDateModified": "2025-01-01T00:00:00+00:00",
                "dateModified": "2025-01-01T00:00:00+00:00",
            },
            "0fbbca6a-0000-0000-0000-000000000002": {
                "path": "sub-mouse02/sub-mouse02_ecephys.nwb",
                "contentSize": 22,
                "blobDateModified": "2025-01-02T00:00:00+00:00",
                "dateModified": "2025-01-02T00:00:00+00:00",
            },
        },
        {},
    )
    with mock.patch("dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata", return_value=metadata):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_lines = [line for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_lines) == 2
    state_records = [json.loads(line) for line in state_lines]
    assert {record["dandi_path"] for record in state_records} == {
        "sub-mouse01/sub-mouse01_ecephys.nwb",
        "sub-mouse02/sub-mouse02_ecephys.nwb",
    }


@pytest.mark.ai_generated
def test_refresh_queue_state_with_dandiset_directory_excludes_entries_with_submitted_markers(
    tmp_path: pathlib.Path,
) -> None:
    """refresh_queue_state output is independent of local submitted marker files."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "test-pipeline": {
                        "version_priority": ["v1.0"],
                        "params_priority": ["abc1234"],
                    }
                }
            }
        )
    )
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=(
            {
                "0fbbca6a-0000-0000-0000-000000000001": {
                    "path": "sub-mouse01/sub-mouse01_ecephys.nwb",
                    "contentSize": 11,
                    "blobDateModified": "2025-01-01T00:00:00+00:00",
                    "dateModified": "2025-01-01T00:00:00+00:00",
                }
            },
            {},
        ),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_lines = [line for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_lines) == 1
    state_records = [json.loads(line) for line in state_lines]
    assert state_records[0]["dandi_path"] == "sub-mouse01/sub-mouse01_ecephys.nwb"
