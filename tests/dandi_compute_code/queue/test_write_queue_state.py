import json
import pathlib
from collections.abc import Iterator
from unittest import mock

import pytest
from _process_queue_test_cases import _make_queue_dir, _read_jsonl

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import QueueState, write_queue_state


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
    attempt_metadata_by_path = {
        f"derivatives/dandiset-001697/sub-{i:02d}/sub-{i:02d}_ecephys/pipeline-test/"
        f"version-v1.0_codebase-v0.3.0_params-default_config-{i:07d}_attempt-1/code/submit.sh": AssetMetadata(
            path=(
                f"derivatives/dandiset-001697/sub-{i:02d}/sub-{i:02d}_ecephys/pipeline-test/"
                f"version-v1.0_codebase-v0.3.0_params-default_config-{i:07d}_attempt-1/code/submit.sh"
            ),
            date_modified="2024-01-01T00:00:00+00:00",
            content_size=1,
            content_id=f"attempt-{i}",
        )
        for i in range(1, 6)
    }
    source_metadata_by_path = {
        f"sub-{i:02d}/sub-{i:02d}_ecephys.nwb": AssetMetadata(
            path=f"sub-{i:02d}/sub-{i:02d}_ecephys.nwb",
            date_modified="2024-01-01T00:00:00+00:00",
            content_size=i,
            content_id=f"id-{i}",
        )
        for i in range(1, 6)
    }
    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=attempt_metadata_by_path),
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=source_metadata_by_path),
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 5
    assert all(
        entry["has_code"] and not entry["has_been_submitted"] and not entry["has_output"] and not entry["has_logs"]
        for entry in state_entries
    )


@pytest.mark.ai_generated
def test_write_queue_state_excludes_entries_with_submitted_markers(tmp_path: pathlib.Path) -> None:
    """write_queue_state no longer depends on local submitted marker files."""
    queue_dir = _make_queue_dir(tmp_path)
    (tmp_path / "sub-mouse01" / "code").mkdir(parents=True)
    (tmp_path / "sub-mouse01" / "code" / "submitted_date-date-2025+01+01_time-00+00+00").write_text("")
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-default_config-def5678_attempt-1/code/submit.sh"
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    attempt_path: AssetMetadata(
                        path=attempt_path,
                        date_modified="2024-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="attempt-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2024-01-01T00:00:00+00:00",
                        content_size=1234,
                        content_id="id-1",
                    )
                },
            ),
        ),
    ):
        write_queue_state(queue_directory=queue_dir)
    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1


@pytest.mark.ai_generated
def test_write_queue_state_submitted_marker_sets_has_been_submitted(tmp_path: pathlib.Path) -> None:
    """Tests that write_queue_state sets has_been_submitted when code/submitted_date-* exists."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-default_config-def5678_attempt-1"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                path=f"{attempt_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="attempt-code-id",
            ),
            f"{attempt_prefix}/code/submitted_date-date-2025+01+01_time-00+00+00": AssetMetadata(
                path=f"{attempt_prefix}/code/submitted_date-date-2025+01+01_time-00+00+00",
                date_modified="2024-01-01T00:01:00+00:00",
                content_size=1,
                content_id="attempt-submitted-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        write_queue_state(queue_directory=queue_dir)
    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
    assert state_entries[0]["has_code"] is True
    assert state_entries[0]["has_been_submitted"] is True
    assert state_entries[0]["has_output"] is False
    assert state_entries[0]["has_logs"] is False


@pytest.mark.ai_generated
def test_write_queue_state_parses_attempt_fields_and_presence_flags_from_assets_paths(tmp_path: pathlib.Path) -> None:
    """write_queue_state parses attempt metadata from derivatives asset paths."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sourcedata/aind-sample.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001849/sub-mouse01/sourcedata/aind-sample/pipeline-aind+ephys/"
        "version-v1.1.1+b268fd2+2372f8e_codebase-v0.3.0_params-4af6a25_config-0d4bf36_attempt-1"
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
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2026-05-24T10:00:00+00:00",
                content_size=1234,
                content_id="source-content-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
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
    assert state_entries[0]["has_been_submitted"] is False
    assert state_entries[0]["has_output"] is True
    assert state_entries[0]["has_logs"] is True
    assert state_entries[0]["job_completion_time"] == "2026-05-24T10:30:00+00:00"
    assert state_entries[0]["output_paths"] == {f"{attempt_prefix}/derivatives/output.nwb": "output-content-id"}
    assert state_entries[0]["log_paths"] == {f"{attempt_prefix}/logs/stdout.txt": "log-content-id"}


@pytest.mark.ai_generated
def test_write_queue_state_with_dandiset_directory_creates_valid_files(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes state.jsonl from assets.jsonld metadata."""
    content_id = "0fbbca6a-0000-0000-0000-000000000001"
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1/code/submit.sh"
    )
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
    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    attempt_path: AssetMetadata(
                        path=attempt_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="attempt-code-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1234,
                        content_id=content_id,
                    )
                },
            ),
        ),
    ):
        write_queue_state(queue_directory=queue_dir)
    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    lines = [line for line in state_file.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "001697"
    assert record["content_id"] == content_id
    assert record["dandi_path"] == source_path
    assert record["asset_size_bytes"] == 1234
    assert record["has_code"] is True


@pytest.mark.ai_generated
def test_write_queue_state_writes_resolved_dandi_path_to_state(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes assets.jsonld-resolved source path in state.jsonl."""
    content_id = "0fbbca6a-0000-0000-0000-000000000001"
    asset_size_bytes = 1234
    resolved_asset_path = "sub-mouse01/sub-mouse01_ses-ses001_obj-raw.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ses-ses001_obj-raw/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-2222222_attempt-1/code/submit.sh"
    )
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

    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    attempt_path: AssetMetadata(
                        path=attempt_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="attempt-code-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    resolved_asset_path: AssetMetadata(
                        path=resolved_asset_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=asset_size_bytes,
                        content_id=content_id,
                    )
                },
            ),
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["asset_size_bytes"] == asset_size_bytes
    assert state_records[0]["dandi_path"] == resolved_asset_path


@pytest.mark.ai_generated
def test_write_queue_state_writes_resolved_dandi_path_for_root_level_asset(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes resolved dandi_path even when matched asset path is at dandiset root."""
    content_id = "0fbbca6a-0000-0000-0000-000000000002"
    asset_size_bytes = 4321
    root_asset_path = "sub-mouse01_ses-ses001_obj-raw.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01_ses-ses001_obj-raw/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-3333333_attempt-1/code/submit.sh"
    )

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

    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    attempt_path: AssetMetadata(
                        path=attempt_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="attempt-code-id",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    root_asset_path: AssetMetadata(
                        path=root_asset_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=asset_size_bytes,
                        content_id=content_id,
                    )
                },
            ),
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["asset_size_bytes"] == asset_size_bytes
    assert state_records[0]["dandi_path"] == root_asset_path


@pytest.mark.ai_generated
def test_write_queue_state_with_dandiset_directory_empty_when_no_attempts(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes an empty state file with no assets metadata."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
    ):
        write_queue_state(queue_directory=queue_dir)
    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    assert state_file.read_text() == ""


@pytest.mark.ai_generated
def test_write_queue_state_does_not_require_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """write_queue_state works when DANDI_API_KEY is not set."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    with (
        mock.patch.dict("os.environ", {}, clear=True),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
    ):
        write_queue_state(queue_directory=queue_dir)


@pytest.mark.ai_generated
def test_write_queue_state_with_dandiset_directory_includes_only_pending_in_waiting(tmp_path: pathlib.Path) -> None:
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

    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test-pipeline/"
            "version-v1.0_codebase-v0.3.0_params-abc1234_config-1111111_attempt-1/code/submit.sh": AssetMetadata(
                path=(
                    "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test-pipeline/"
                    "version-v1.0_codebase-v0.3.0_params-abc1234_config-1111111_attempt-1/code/submit.sh"
                ),
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=1,
                content_id="attempt-1",
            ),
            "derivatives/dandiset-001697/sub-mouse02/sub-mouse02_ecephys/pipeline-test-pipeline/"
            "version-v1.0_codebase-v0.3.0_params-abc1234_config-2222222_attempt-1/code/submit.sh": AssetMetadata(
                path=(
                    "derivatives/dandiset-001697/sub-mouse02/sub-mouse02_ecephys/pipeline-test-pipeline/"
                    "version-v1.0_codebase-v0.3.0_params-abc1234_config-2222222_attempt-1/code/submit.sh"
                ),
                date_modified="2025-01-02T00:00:00+00:00",
                content_size=1,
                content_id="attempt-2",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            "sub-mouse01/sub-mouse01_ecephys.nwb": AssetMetadata(
                path="sub-mouse01/sub-mouse01_ecephys.nwb",
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=11,
                content_id="0fbbca6a-0000-0000-0000-000000000001",
            ),
            "sub-mouse02/sub-mouse02_ecephys.nwb": AssetMetadata(
                path="sub-mouse02/sub-mouse02_ecephys.nwb",
                date_modified="2025-01-02T00:00:00+00:00",
                content_size=22,
                content_id="0fbbca6a-0000-0000-0000-000000000002",
            ),
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_lines = [line for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_lines) == 2
    state_records = [json.loads(line) for line in state_lines]
    assert {record["dandi_path"] for record in state_records} == {
        "sub-mouse01/sub-mouse01_ecephys.nwb",
        "sub-mouse02/sub-mouse02_ecephys.nwb",
    }


@pytest.mark.ai_generated
def test_write_queue_state_with_dandiset_directory_excludes_entries_with_submitted_markers(
    tmp_path: pathlib.Path,
) -> None:
    """write_queue_state output is independent of local submitted marker files."""
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
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test-pipeline/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-9999999_attempt-1/code/submit.sh"
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    attempt_path: AssetMetadata(
                        path=attempt_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=1,
                        content_id="attempt-1",
                    )
                },
            ),
        ),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    "sub-mouse01/sub-mouse01_ecephys.nwb": AssetMetadata(
                        path="sub-mouse01/sub-mouse01_ecephys.nwb",
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=11,
                        content_id="0fbbca6a-0000-0000-0000-000000000001",
                    )
                },
            ),
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_lines = [line for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_lines) == 1
    state_records = [json.loads(line) for line in state_lines]
    assert state_records[0]["dandi_path"] == "sub-mouse01/sub-mouse01_ecephys.nwb"


@pytest.mark.ai_generated
def test_write_queue_state_parses_codebase_field_from_new_format_path(tmp_path: pathlib.Path) -> None:
    """write_queue_state parses the _codebase- entity from new-format derivatives paths."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-aind+ephys/"
        "version-v1.1.1_codebase-v0.3.17_params-4af6a25_config-0d4bf36_attempt-1"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={
            "source-content-id": {
                "path": source_path,
                "contentSize": 500,
                "blobDateModified": "2026-05-24T09:00:00+00:00",
            },
            "code-content-id": {
                "path": f"{attempt_prefix}/code/submit.sh",
                "contentSize": 1,
                "dateModified": "2026-05-24T10:00:00+00:00",
            },
        },
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2026-05-24T09:00:00+00:00",
                content_size=500,
                content_id="source-content-id",
            ),
            f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                path=f"{attempt_prefix}/code/submit.sh",
                date_modified="2026-05-24T10:00:00+00:00",
                content_size=1,
                content_id="code-content-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2026-05-24T09:00:00+00:00",
                content_size=500,
                content_id="source-content-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
    assert state_entries[0]["version"] == "v1.1.1"
    assert state_entries[0]["params"] == "4af6a25"
    assert state_entries[0]["config"] == "0d4bf36"
    assert state_entries[0]["codebase"] == "v0.3.17"
    assert state_entries[0]["attempt"] == 1
    assert state_entries[0]["has_code"] is True


def test_write_queue_state_output_paths_empty_when_no_output(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes output_paths as an empty dict when has_output is False."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                path=f"{attempt_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
    assert state_entries[0]["has_output"] is False
    assert state_entries[0]["dataset_description_path"] == {}
    assert state_entries[0]["output_paths"] == {}


def test_write_queue_state_log_paths_empty_when_no_logs(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes log_paths as an empty dict when has_logs is False."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                path=f"{attempt_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
    assert state_entries[0]["has_logs"] is False
    assert state_entries[0]["log_paths"] == {}


def test_write_queue_state_output_paths_maps_asset_paths_to_blob_ids(tmp_path: pathlib.Path) -> None:
    """write_queue_state populates output_paths with all derivatives asset paths mapped to their blob IDs."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                path=f"{attempt_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
            f"{attempt_prefix}/derivatives/output.nwb": AssetMetadata(
                path=f"{attempt_prefix}/derivatives/output.nwb",
                date_modified="2024-01-01T00:01:00+00:00",
                content_size=100,
                content_id="output-blob-id-1",
            ),
            f"{attempt_prefix}/derivatives/extra.json": AssetMetadata(
                path=f"{attempt_prefix}/derivatives/extra.json",
                date_modified="2024-01-01T00:02:00+00:00",
                content_size=10,
                content_id="output-blob-id-2",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
    assert state_entries[0]["has_output"] is True
    assert state_entries[0]["output_paths"] == {
        f"{attempt_prefix}/derivatives/output.nwb": "output-blob-id-1",
        f"{attempt_prefix}/derivatives/extra.json": "output-blob-id-2",
    }


def test_write_queue_state_log_paths_map_asset_paths_to_blob_ids(tmp_path: pathlib.Path) -> None:
    """write_queue_state populates log_paths with log asset paths mapped to their blob IDs."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_prefix = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1"
    )
    metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            f"{attempt_prefix}/code/submit.sh": AssetMetadata(
                path=f"{attempt_prefix}/code/submit.sh",
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="code-id",
            ),
            f"{attempt_prefix}/dataset_description.json": AssetMetadata(
                path=f"{attempt_prefix}/dataset_description.json",
                date_modified="2024-01-01T00:00:30+00:00",
                content_size=10,
                content_id="dataset-description-id",
            ),
            f"{attempt_prefix}/logs/stdout.txt": AssetMetadata(
                path=f"{attempt_prefix}/logs/stdout.txt",
                date_modified="2024-01-01T00:01:00+00:00",
                content_size=100,
                content_id="log-blob-id-1",
            ),
            f"{attempt_prefix}/logs/stderr.txt": AssetMetadata(
                path=f"{attempt_prefix}/logs/stderr.txt",
                date_modified="2024-01-01T00:02:00+00:00",
                content_size=10,
                content_id="log-blob-id-2",
            ),
            f"{attempt_prefix}/logs/dataset_description.json": AssetMetadata(
                path=f"{attempt_prefix}/logs/dataset_description.json",
                date_modified="2024-01-01T00:03:00+00:00",
                content_size=10,
                content_id="ignored-log-id",
            ),
        },
    )
    upstream_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1234,
                content_id="source-id",
            )
        },
    )
    with (
        mock.patch("dandi_compute_code.queue._write_queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._write_queue_state._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 1
    assert state_entries[0]["has_logs"] is True
    assert state_entries[0]["dataset_description_path"] == {
        f"{attempt_prefix}/dataset_description.json": "dataset-description-id"
    }
    assert state_entries[0]["log_paths"] == {
        f"{attempt_prefix}/logs/stdout.txt": "log-blob-id-1",
        f"{attempt_prefix}/logs/stderr.txt": "log-blob-id-2",
    }


@pytest.mark.ai_generated
def test_queue_state_from_jsonl_preserves_dataset_description_path(tmp_path: pathlib.Path) -> None:
    """QueueState.from_jsonl preserves dataset_description_path entries."""
    state_file = tmp_path / "state.jsonl"
    state_file.write_text(
        json.dumps(
            {
                "dandiset_id": "001697",
                "dandi_path": "sub-mouse01/sub-mouse01_ecephys.nwb",
                "pipeline": "aind+ephys",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "attempt": 1,
                "codebase": "v0.3.0",
                "dataset_description_path": {
                    "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/"
                    "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1/"
                    "dataset_description.json": "dataset-description-id"
                },
            }
        )
        + "\n"
    )

    queue_state = QueueState.from_jsonl(state_file)

    assert len(queue_state) == 1
    assert queue_state.entries[0].dataset_description_path == {
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1/"
        "dataset_description.json": "dataset-description-id"
    }


def test_queue_state_from_jsonl_converts_legacy_dataset_description_path(
    tmp_path: pathlib.Path,
) -> None:
    """QueueState.from_jsonl converts legacy string dataset_description_path values."""
    state_file = tmp_path / "state.jsonl"
    state_file.write_text(
        json.dumps(
            {
                "dandiset_id": "001697",
                "dandi_path": "sub-mouse01/sub-mouse01_ecephys.nwb",
                "pipeline": "aind+ephys",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "attempt": 1,
                "codebase": "v0.3.0",
                "dataset_description_path": (
                    "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/"
                    "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1/"
                    "dataset_description.json"
                ),
            }
        )
        + "\n"
    )

    queue_state = QueueState.from_jsonl(state_file)

    assert len(queue_state) == 1
    assert queue_state.entries[0].dataset_description_path == {
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-1/"
        "dataset_description.json": None
    }


def test_queue_state_from_jsonl_rejects_invalid_dataset_description_type(
    tmp_path: pathlib.Path,
) -> None:
    """QueueState.from_jsonl raises when dataset_description_path has an invalid type."""
    state_file = tmp_path / "state.jsonl"
    state_file.write_text(
        json.dumps(
            {
                "dandiset_id": "001697",
                "dandi_path": "sub-mouse01/sub-mouse01_ecephys.nwb",
                "pipeline": "aind+ephys",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "attempt": 1,
                "codebase": "v0.3.0",
                "dataset_description_path": 123,
            }
        )
        + "\n"
    )

    with pytest.raises(TypeError, match="Expected mapping or string"):
        QueueState.from_jsonl(state_file)


def test_queue_state_from_jsonl_converts_null_dataset_description_path(
    tmp_path: pathlib.Path,
) -> None:
    """QueueState.from_jsonl converts null dataset_description_path values to empty dicts."""
    state_file = tmp_path / "state.jsonl"
    state_file.write_text(
        json.dumps(
            {
                "dandiset_id": "001697",
                "dandi_path": "sub-mouse01/sub-mouse01_ecephys.nwb",
                "pipeline": "aind+ephys",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "attempt": 1,
                "codebase": "v0.3.0",
                "dataset_description_path": None,
            }
        )
        + "\n"
    )

    queue_state = QueueState.from_jsonl(state_file)

    assert len(queue_state) == 1
    assert queue_state.entries[0].dataset_description_path == {}
