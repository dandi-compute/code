import json
import pathlib
from collections.abc import Iterator
from unittest import mock

import pytest
from _process_queue_test_cases import _make_queue_dir, _read_jsonl

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import write_queue_state


def _build_assets_metadata(
    *,
    content_id: str,
    asset_path: str,
    content_size: int | str | None = None,
    blob_date_modified: str | None = None,
    log_asset_path: str | None = None,
    log_date_modified: str | None = None,
) -> AssetsJsonldMetadata:
    """Build indexed synthetic assets metadata for tests."""
    source_asset: dict[str, object] = {"path": asset_path}
    if content_size is not None:
        source_asset["contentSize"] = content_size
    if blob_date_modified is not None:
        source_asset["blobDateModified"] = blob_date_modified
    date_modified = blob_date_modified or "2024-01-01T00:00:00+00:00"
    path_to_asset_metadata: dict[str, AssetMetadata] = {
        asset_path: AssetMetadata(
            path=asset_path,
            date_modified=date_modified,
            content_size=content_size if isinstance(content_size, int) else 1,
            content_id=content_id,
        )
    }
    content_id_to_asset = {content_id: source_asset}
    if log_asset_path is not None and log_date_modified is not None:
        log_content_id = f"{content_id}-log"
        path_to_asset_metadata[log_asset_path] = AssetMetadata(
            path=log_asset_path,
            date_modified=log_date_modified,
            content_size=1,
            content_id=log_content_id,
        )
        content_id_to_asset[log_content_id] = {
            "path": log_asset_path,
            "contentSize": 1,
            "dateModified": log_date_modified,
        }
    return AssetsJsonldMetadata(content_id_to_asset=content_id_to_asset, path_to_asset_metadata=path_to_asset_metadata)


@pytest.fixture(autouse=True)
def _mock_dandi_api_asset_lookup() -> Iterator[None]:
    """Prevent network calls by defaulting metadata loader to empty indexes."""
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
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
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(content_id_to_asset=content_id_to_asset, path_to_asset_metadata={}),
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
        "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(
            content_id_to_asset=content_id_to_asset,
            path_to_asset_metadata={
                asset["path"]: AssetMetadata(
                    path=asset["path"],
                    date_modified="2024-01-01T00:00:00+00:00",
                    content_size=asset["contentSize"],
                    content_id=content_id,
                )
                for content_id, asset in content_id_to_asset.items()
            },
        ),
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
        "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(
            content_id_to_asset=content_id_to_asset,
            path_to_asset_metadata={
                "sub-mouse01/sub-mouse01_ecephys.nwb": AssetMetadata(
                    path="sub-mouse01/sub-mouse01_ecephys.nwb",
                    date_modified="2024-01-01T00:00:00+00:00",
                    content_size=1234,
                    content_id="id-1",
                )
            },
        ),
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
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={
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
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2026-05-24T10:00:00+00:00",
                content_size=1234,
                content_id="source-content-id",
            ),
            f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                path=f"{attempt_prefix}/code/submit.sh",
                date_modified="2026-05-24T10:10:00+00:00",
                content_size=1,
                content_id="code-content-id",
            ),
            f"{attempt_prefix}/derivatives/output.nwb": AssetMetadata(
                path=f"{attempt_prefix}/derivatives/output.nwb",
                date_modified="2026-05-24T10:20:00+00:00",
                content_size=2,
                content_id="output-content-id",
            ),
            f"{attempt_prefix}/logs/stdout.txt": AssetMetadata(
                path=f"{attempt_prefix}/logs/stdout.txt",
                date_modified="2026-05-24T10:30:00+00:00",
                content_size=3,
                content_id="log-content-id",
            ),
        },
    )
    with mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata):
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
