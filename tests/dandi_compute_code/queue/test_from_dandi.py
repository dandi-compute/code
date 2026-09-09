from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import QueueState

# QueueState.from_dandi derives queue state from DANDI assets.jsonld metadata fetched over
# the network. The conftest _no_real_dandi_fetch guard defaults that loader to empty; tests
# that need specific metadata override it with their own mock.patch. The assets metadata built
# in each test is the ground-truth input under test.


def _entries(state: QueueState) -> list[dict]:
    return [entry.to_dict() for entry in state]


@pytest.mark.ai_generated
def test_from_dandi_returns_empty_for_missing_metadata() -> None:
    """from_dandi returns an empty state when there is no assets metadata."""
    content_id_to_asset: dict[str, dict[str, object]] = {}
    with mock.patch(
        "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(content_id_to_asset=content_id_to_asset, path_to_asset_metadata={}),
    ):
        state = QueueState.from_dandi()
    assert len(state) == 0


@pytest.mark.ai_generated
def test_from_dandi_returns_all_ordered_pending_entries() -> None:
    """from_dandi returns ordered pending entries from metadata."""
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
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=attempt_metadata_by_path),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=source_metadata_by_path),
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 5
    assert all(
        entry["has_code"] and not entry["has_been_submitted"] and not entry["has_output"] and not entry["has_logs"]
        for entry in state_entries
    )


@pytest.mark.ai_generated
def test_from_dandi_includes_entries_with_submitted_markers() -> None:
    """from_dandi does not depend on local submitted marker files."""
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-default_config-def5678_attempt-1/code/submit.sh"
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
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
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
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
        state = QueueState.from_dandi()
    state_entries = _entries(state)
    assert len(state_entries) == 1


@pytest.mark.ai_generated
def test_from_dandi_submitted_marker_sets_has_been_submitted() -> None:
    """from_dandi sets has_been_submitted when code/submitted_date-* exists."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()
    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["has_code"] is True
    assert state_entries[0]["has_been_submitted"] is True
    assert state_entries[0]["has_output"] is False
    assert state_entries[0]["has_logs"] is False


@pytest.mark.ai_generated
def test_from_dandi_parses_attempt_fields_and_presence_flags_from_assets_paths() -> None:
    """from_dandi parses attempt metadata from derivatives asset paths."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
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
def test_from_dandi_resolves_dandi_path_for_nested_asset() -> None:
    """from_dandi writes the assets.jsonld-resolved source path."""
    content_id = "0fbbca6a-0000-0000-0000-000000000001"
    source_path = "sub-mouse01/sub-mouse01_ses-ses001_obj-raw.nwb"
    asset_size_bytes = 1234
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ses-ses001_obj-raw/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-2222222_attempt-1/code/submit.sh"
    )

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
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
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2025-01-01T00:00:00+00:00",
                        content_size=asset_size_bytes,
                        content_id=content_id,
                    )
                },
            ),
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["asset_size_bytes"] == asset_size_bytes
    assert state_entries[0]["dandi_path"] == source_path


@pytest.mark.ai_generated
def test_from_dandi_resolves_dandi_path_for_root_level_asset() -> None:
    """from_dandi writes the resolved dandi_path even when the matched asset path is at dandiset root."""
    content_id = "0fbbca6a-0000-0000-0000-000000000002"
    asset_size_bytes = 4321
    root_asset_path = "sub-mouse01_ses-ses001_obj-raw.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01_ses-ses001_obj-raw/"
        "pipeline-aind+ephys/version-v1.0_codebase-v0.3.0_params-abc1234_config-3333333_attempt-1/code/submit.sh"
    )

    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
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
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
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
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["asset_size_bytes"] == asset_size_bytes
    assert state_entries[0]["dandi_path"] == root_asset_path


@pytest.mark.ai_generated
def test_from_dandi_does_not_require_dandi_api_key() -> None:
    """from_dandi works when DANDI_API_KEY is not set."""
    with (
        mock.patch.dict("os.environ", {}, clear=True),
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
        ),
    ):
        QueueState.from_dandi()


@pytest.mark.ai_generated
def test_from_dandi_includes_all_entries_derived_from_metadata() -> None:
    """from_dandi returns all entries derived from assets metadata."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 2
    assert {record["dandi_path"] for record in state_entries} == {
        "sub-mouse01/sub-mouse01_ecephys.nwb",
        "sub-mouse02/sub-mouse02_ecephys.nwb",
    }


@pytest.mark.ai_generated
def test_from_dandi_is_independent_of_local_submitted_marker_files() -> None:
    """from_dandi output is independent of local submitted marker files."""
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test-pipeline/"
        "version-v1.0_codebase-v0.3.0_params-abc1234_config-9999999_attempt-1/code/submit.sh"
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
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
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
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
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["dandi_path"] == "sub-mouse01/sub-mouse01_ecephys.nwb"


@pytest.mark.ai_generated
def test_from_dandi_parses_codebase_field_from_new_format_path() -> None:
    """from_dandi parses the _codebase- entity from new-format derivatives paths."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["version"] == "v1.1.1"
    assert state_entries[0]["params"] == "4af6a25"
    assert state_entries[0]["config"] == "0d4bf36"
    assert state_entries[0]["codebase"] == "v0.3.17"
    assert state_entries[0]["attempt"] == 1
    assert state_entries[0]["has_code"] is True


@pytest.mark.ai_generated
def test_from_dandi_output_paths_empty_when_no_output() -> None:
    """from_dandi returns output_paths as an empty dict when has_output is False."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["has_output"] is False
    assert state_entries[0]["dataset_description_path"] == {}
    assert state_entries[0]["output_paths"] == {}


@pytest.mark.ai_generated
def test_from_dandi_log_paths_empty_when_no_logs() -> None:
    """from_dandi returns log_paths as an empty dict when has_logs is False."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["has_logs"] is False
    assert state_entries[0]["log_paths"] == {}


@pytest.mark.ai_generated
def test_from_dandi_output_paths_maps_asset_paths_to_blob_ids() -> None:
    """from_dandi populates output_paths with all derivatives asset paths mapped to their blob IDs."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["has_output"] is True
    assert state_entries[0]["output_paths"] == {
        f"{attempt_prefix}/derivatives/output.nwb": "output-blob-id-1",
        f"{attempt_prefix}/derivatives/extra.json": "output-blob-id-2",
    }


@pytest.mark.ai_generated
def test_from_dandi_log_paths_map_asset_paths_to_blob_ids() -> None:
    """from_dandi populates log_paths with log asset paths mapped to their blob IDs."""
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
        mock.patch("dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata", return_value=metadata),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=upstream_metadata,
        ),
    ):
        state = QueueState.from_dandi()

    state_entries = _entries(state)
    assert len(state_entries) == 1
    assert state_entries[0]["has_logs"] is True
    assert state_entries[0]["dataset_description_path"] == {
        f"{attempt_prefix}/dataset_description.json": "dataset-description-id"
    }
    assert state_entries[0]["log_paths"] == {
        f"{attempt_prefix}/logs/stdout.txt": "log-blob-id-1",
        f"{attempt_prefix}/logs/stderr.txt": "log-blob-id-2",
    }
