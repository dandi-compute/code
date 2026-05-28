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


def _build_assets_metadata(
    *,
    content_id: str,
    asset_path: str,
    content_size: int | str | None = None,
    blob_date_modified: str | None = None,
    log_asset_path: str | None = None,
    log_date_modified: str | None = None,
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    """Build source-asset and log-path timestamp indexes from one synthetic asset."""
    source_asset: dict[str, object] = {"path": asset_path}
    if content_size is not None:
        source_asset["contentSize"] = content_size
    if blob_date_modified is not None:
        source_asset["blobDateModified"] = blob_date_modified
    path_to_date_modified: dict[str, str] = {}
    if log_asset_path is not None and log_date_modified is not None:
        path_to_date_modified[log_asset_path] = log_date_modified
    return {content_id: source_asset}, path_to_date_modified


@pytest.fixture(autouse=True)
def _mock_dandi_api_asset_lookup() -> Iterator[None]:
    """Prevent network calls by defaulting metadata loader to empty tuple indexes."""
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=({}, {}),
    ):
        yield


@pytest.mark.ai_generated
def test_write_queue_state_raises_when_queue_config_fails_linkml_validation(tmp_path: pathlib.Path) -> None:
    """write_queue_state raises when queue_config violates LinkML constraints."""
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
        write_queue_state(queue_directory=queue_dir)


@pytest.mark.ai_generated
def test_write_queue_state_writes_empty_files_for_missing_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """write_queue_state does not require local dandiset scan."""
    queue_dir = _make_queue_dir(tmp_path)

    content_id_to_asset: dict[str, dict[str, object]] = {}
    path_to_date_modified: dict[str, str] = {}
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=(content_id_to_asset, path_to_date_modified),
    ):
        write_queue_state(queue_directory=queue_dir)
    assert (queue_dir / "state.jsonl").exists()
    assert (queue_dir / "state.jsonl").read_text() == ""


@pytest.mark.ai_generated
def test_write_queue_state_writes_all_ordered_pending_entries(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes ordered pending entries from metadata."""
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
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 5
    assert all(not entry["has_code"] and not entry["has_output"] and not entry["has_logs"] for entry in state_entries)


@pytest.mark.ai_generated
def test_write_queue_state_excludes_entries_with_submitted_markers(tmp_path: pathlib.Path) -> None:
    """write_queue_state no longer depends on local submitted marker files."""
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
        write_queue_state(queue_directory=queue_dir)
    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1


@pytest.mark.ai_generated
def test_write_queue_state_parses_attempt_fields_and_presence_flags_from_assets_paths(tmp_path: pathlib.Path) -> None:
    """write_queue_state parses attempt metadata from derivatives asset paths."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-test/sourcedata/aind-sample.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001849/sub-test/sourcedata/aind-sample/pipeline-aind+ephys/"
        "version-v1.1.1+b268fd2+2372f8e_params-4af6a25_config-0d4bf36_date-2026+05+24_attempt-1"
    )
    metadata = (
        {
            "source-content-id": {
                "path": source_path,
                "contentSize": 1234,
                "blobDateModified": "2026-05-24T10:00:00+00:00",
            },
            "code-content-id": {
                "path": f"{attempt_prefix}/code/submit.sh",
                "contentSize": 1,
                "dateModified": "2026-05-24T10:10:00+00:00",
            },
            "output-content-id": {
                "path": f"{attempt_prefix}/derivatives/output.nwb",
                "contentSize": 2,
                "dateModified": "2026-05-24T10:20:00+00:00",
            },
            "log-content-id": {
                "path": f"{attempt_prefix}/logs/stdout.txt",
                "contentSize": 3,
                "dateModified": "2026-05-24T10:30:00+00:00",
            },
        },
        {f"{attempt_prefix}/logs/stdout.txt": "2026-05-24T10:30:00+00:00"},
    )
    with mock.patch("dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata", return_value=metadata):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
    assert state_entries[0]["dandiset_id"] == "001849"
    assert state_entries[0]["dandi_path"] == source_path
    assert state_entries[0]["pipeline"] == "aind+ephys"
    assert state_entries[0]["version"] == "v1.1.1+b268fd2+2372f8e"
    assert state_entries[0]["params"] == "4af6a25"
    assert state_entries[0]["config"] == "0d4bf36"
    assert state_entries[0]["attempt"] == 1
    assert state_entries[0]["content_id"] == "source-content-id"
    assert state_entries[0]["asset_size_bytes"] == 1234
    assert state_entries[0]["has_code"] is True
    assert state_entries[0]["has_output"] is True
    assert state_entries[0]["has_logs"] is True
    assert state_entries[0]["job_completion_time"] == "2026-05-24T10:30:00+00:00"
