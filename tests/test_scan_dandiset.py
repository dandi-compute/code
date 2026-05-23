"""
Unit tests for scan_dandiset_directory and writing state/waiting files via refresh_queue_state.
(dandi_compute_code.dandiset).
"""

import json
import pathlib
import re
from collections.abc import Iterator
from typing import Any
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import scan_dandiset_directory
from dandi_compute_code.queue import refresh_queue_state

DEFAULT_TEST_CONTENT_ID = "00000000-0000-0000-0000-000000000000"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_attempt_dir(
    base: pathlib.Path,
    dandiset_id: str,
    subject: str,
    pipeline: str,
    version: str,
    params: str,
    config: str,
    attempt: int,
    *,
    session: str | None = None,
    with_code: bool = True,
    with_output: bool = False,
    with_logs: bool = False,
    logs_empty: bool = False,
    content_id: str | None = None,
) -> pathlib.Path:
    """
    Create a mock attempt directory inside a fake dandiset clone rooted at *base*.
    """
    parts = [
        base,
        "derivatives",
        f"dandiset-{dandiset_id}",
        f"sub-{subject}",
    ]
    if session is not None:
        parts.append(f"ses-{session}")
    parts += [
        f"pipeline-{pipeline}",
        f"version-{version}_params-{params}_config-{config}_attempt-{attempt}",
    ]
    attempt_dir = pathlib.Path(*parts)
    attempt_dir.mkdir(parents=True)
    if with_code:
        (attempt_dir / "code").mkdir()
        resolved_content_id = content_id if content_id is not None else DEFAULT_TEST_CONTENT_ID
        blob_prefix = resolved_content_id[:3]
        blob_subprefix = resolved_content_id[3:6]
        (attempt_dir / "code" / "submit.sh").write_text(
            (
                "#!/bin/bash\n"
                f'NWB_FILE_PATH="/orcd/data/dandi/001/s3dandiarchive/blobs/{blob_prefix}/{blob_subprefix}/{resolved_content_id}"\n'
            )
        )
    if with_output:
        (attempt_dir / "derivatives").mkdir()
    if with_logs:
        logs_dir = attempt_dir / "logs"
        logs_dir.mkdir()
        if not logs_empty:
            (logs_dir / "nextflow.log").write_text("log content")
    return attempt_dir


def _build_mapping_for_content_id(*, content_id: str, dandiset_id: str, asset_path: str) -> dict[str, dict[str, str]]:
    """
    Build a minimal content ID → unique dandiset path mapping payload.

    Parameters
    ----------
    content_id : str
        Content ID key in the mapping.
    dandiset_id : str
        Dandiset ID associated with the content ID.
    asset_path : str
        Unique mapped asset path for the content ID.

    Returns
    -------
    dict[str, dict[str, str]]
        Nested mapping of content ID to ``{dandiset_id: asset_path}``.
    """
    return {content_id: {dandiset_id: asset_path}}


# ---------------------------------------------------------------------------
# Tests for scan_dandiset_directory
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def mock_dandi_api_asset_lookup() -> Iterator[None]:
    """Prevent network calls during tests by defaulting DANDI lookup to no matches."""

    class _EmptyDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[Any]:
            return iter(())

    class _EmptyClient:
        def get_dandiset(self, dandiset_id: str) -> _EmptyDandiset:
            assert isinstance(dandiset_id, str) and dandiset_id
            return _EmptyDandiset()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_EmptyClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value={},
        ),
    ):
        yield


@pytest.mark.ai_generated
def test_scan_empty_when_no_derivatives(tmp_path: pathlib.Path) -> None:
    """Returns an empty list when there is no derivatives/ directory."""
    result = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert result == []


@pytest.mark.ai_generated
def test_scan_requires_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """scan_dandiset_directory requires DANDI_API_KEY to be set."""
    with mock.patch.dict("os.environ", {}, clear=True):
        with pytest.raises(AssertionError, match="DANDI_API_KEY"):
            scan_dandiset_directory(dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_scan_single_failed_attempt(tmp_path: pathlib.Path) -> None:
    """Single attempt with code/ but no derivatives/ is returned with correct fields."""
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
        with_output=False,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    r = records[0]
    assert r["dandiset_id"] == "000001"
    assert r["dandi_path"] == "sub-mouse01"
    assert r["content_id"] == DEFAULT_TEST_CONTENT_ID
    assert r["asset_size_bytes"] is None
    assert r["pipeline"] == "aind+ephys"
    assert r["version"] == "v1.0"
    assert r["params"] == "abc1234"
    assert r["config"] == "def5678"
    assert r["attempt"] == 1
    assert r["has_code"] is True
    assert r["has_output"] is False
    assert r["has_logs"] is False


@pytest.mark.ai_generated
def test_scan_single_successful_attempt(tmp_path: pathlib.Path) -> None:
    """Single attempt with both code/ and derivatives/ is returned with has_output=True."""
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
        with_output=True,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["has_code"] is True
    assert records[0]["has_output"] is True


@pytest.mark.ai_generated
def test_scan_has_logs_true_when_logs_dir_nonempty(tmp_path: pathlib.Path) -> None:
    """has_logs is True when a non-empty logs/ subdirectory exists."""
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_logs=True,
        logs_empty=False,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["has_logs"] is True


@pytest.mark.ai_generated
def test_scan_has_logs_false_when_logs_dir_empty(tmp_path: pathlib.Path) -> None:
    """has_logs is False when the logs/ directory exists but is empty."""
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_logs=True,
        logs_empty=True,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["has_logs"] is False


@pytest.mark.ai_generated
def test_scan_has_logs_false_when_no_logs_dir(tmp_path: pathlib.Path) -> None:
    """has_logs is False when no logs/ directory exists at all."""
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_logs=False,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["has_logs"] is False


@pytest.mark.ai_generated
def test_scan_with_session_level(tmp_path: pathlib.Path) -> None:
    """Session-level directories are parsed correctly."""
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        session="20230101",
        with_code=True,
        with_output=False,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["dandi_path"] == "sub-mouse01/ses-20230101"


@pytest.mark.ai_generated
def test_scan_parses_non_bids_entity_path_into_dandi_path(tmp_path: pathlib.Path) -> None:
    """scan_dandiset_directory stores the full path segment even when no sub/ses entities exist."""
    attempt_dir = (
        tmp_path
        / "derivatives"
        / "dandiset-000001"
        / "sample-slab01"
        / "slice-sliceA"
        / "pipeline-aind+ephys"
        / "version-v1.0_params-abc1234_config-def5678_attempt-1"
    )
    (attempt_dir / "code").mkdir(parents=True)
    (attempt_dir / "code" / "submit.sh").write_text(f'NWB_FILE_PATH="{DEFAULT_TEST_CONTENT_ID}"\n')

    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["dandi_path"] == "sample-slab01/slice-sliceA"


@pytest.mark.ai_generated
def test_scan_multiple_attempts_sorted(tmp_path: pathlib.Path) -> None:
    """Multiple attempts are returned sorted by (dandiset_id, subject, ..., attempt)."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 3)
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 2)
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert [r["attempt"] for r in records] == [1, 2, 3]


@pytest.mark.ai_generated
def test_scan_multiple_dandisets(tmp_path: pathlib.Path) -> None:
    """Attempts from multiple dandiset directories are all returned."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    _make_attempt_dir(tmp_path, "000002", "rat01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 2
    dandiset_ids = {r["dandiset_id"] for r in records}
    assert dandiset_ids == {"000001", "000002"}


@pytest.mark.ai_generated
def test_scan_ignores_non_dandiset_dirs(tmp_path: pathlib.Path) -> None:
    """Directories not starting with 'dandiset-' are ignored."""
    # Create a valid attempt
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    # Create a spurious directory that should be ignored
    spurious = tmp_path / "derivatives" / "other-stuff" / "sub-x" / "pipeline-y" / "version-z"
    spurious.mkdir(parents=True)
    (spurious / "params-a_config-b_attempt-1").mkdir()

    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_scan_includes_created_at_timestamp(tmp_path: pathlib.Path) -> None:
    """Each record includes a created_at field with a valid ISO 8601 UTC timestamp."""
    import datetime

    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    created_at = records[0]["created_at"]
    assert isinstance(created_at, str)
    # Must be parseable as an ISO 8601 datetime with timezone info
    dt = datetime.datetime.fromisoformat(created_at)
    assert dt.tzinfo is not None


@pytest.mark.ai_generated
def test_scan_job_completion_time_is_none_when_no_logs(tmp_path: pathlib.Path) -> None:
    """job_completion_time is None when the attempt has no logs directory."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_logs=False)
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["job_completion_time"] is None


@pytest.mark.ai_generated
def test_scan_job_completion_time_populated_from_dandi_date_modified(tmp_path: pathlib.Path) -> None:
    """job_completion_time is taken from the dateModified field of the first DANDI log asset."""
    expected_completion_time = "2024-06-15T12:30:00+00:00"
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    attempt_dir = _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
        with_logs=True,
    )
    expected_log_path = (attempt_dir / "logs" / "nextflow.log").relative_to(tmp_path).as_posix()

    class _FakeLogAsset:
        def get_raw_metadata(self) -> dict[str, Any]:
            return {"dateModified": expected_completion_time}

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[Any]:
            if path == expected_log_path:
                return iter([_FakeLogAsset()])
            return iter(())

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            return _FakeDandiset()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
            ),
        ),
    ):
        records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["job_completion_time"] == expected_completion_time


@pytest.mark.ai_generated
def test_scan_job_completion_time_is_none_with_warning_when_log_asset_not_found(tmp_path: pathlib.Path) -> None:
    """job_completion_time is None with a warning when no DANDI asset matches the log path."""
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    attempt_dir = _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
        with_logs=True,
    )
    expected_log_path = (attempt_dir / "logs" / "nextflow.log").relative_to(tmp_path).as_posix()

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[Any]:
            return iter(())

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            return _FakeDandiset()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        pytest.warns(
            UserWarning, match=f"Unable to resolve job_completion_time for log asset at {re.escape(expected_log_path)}"
        ),
    ):
        records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["job_completion_time"] is None


@pytest.mark.ai_generated
def test_scan_job_completion_time_is_none_with_warning_when_date_modified_missing(tmp_path: pathlib.Path) -> None:
    """job_completion_time is None with a warning when the log asset has no dateModified field."""
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    attempt_dir = _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
        with_logs=True,
    )
    expected_log_path = (attempt_dir / "logs" / "nextflow.log").relative_to(tmp_path).as_posix()

    class _FakeLogAsset:
        def get_raw_metadata(self) -> dict[str, Any]:
            return {}  # No dateModified field

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[Any]:
            if path == expected_log_path:
                return iter([_FakeLogAsset()])
            return iter(())

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            return _FakeDandiset()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        pytest.warns(
            UserWarning, match=f"Unable to resolve job_completion_time for log asset at {re.escape(expected_log_path)}"
        ),
    ):
        records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["job_completion_time"] is None


@pytest.mark.ai_generated
def test_scan_config_with_underscores(tmp_path: pathlib.Path) -> None:
    """Config IDs that contain underscores (e.g., 'abc_date-2024+01+01') are parsed correctly."""
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678_date-2024+01+01",
        1,
        with_code=True,
        with_output=False,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["config"] == "def5678_date-2024+01+01"


@pytest.mark.ai_generated
def test_scan_supports_legacy_version_subdirectory_layout(tmp_path: pathlib.Path) -> None:
    """Legacy attempt paths with a version subdirectory are still parsed."""
    legacy_attempt_dir = (
        tmp_path
        / "derivatives"
        / "dandiset-000001"
        / "sub-mouse01"
        / "pipeline-aind+ephys"
        / "version-v1.0"
        / "params-abc1234_config-def5678_attempt-1"
    )
    legacy_attempt_dir.mkdir(parents=True)
    (legacy_attempt_dir / "code").mkdir()
    (legacy_attempt_dir / "code" / "submit.sh").write_text(
        f'#!/bin/bash\nNWB_FILE_PATH="/orcd/data/dandi/001/s3dandiarchive/blobs/000/000/{DEFAULT_TEST_CONTENT_ID}"\n'
    )

    records = scan_dandiset_directory(dandiset_directory=tmp_path)

    assert len(records) == 1
    assert records[0]["version"] == "v1.0"
    assert records[0]["params"] == "abc1234"


@pytest.mark.ai_generated
def test_scan_parses_content_id_from_submission_script(tmp_path: pathlib.Path) -> None:
    """content_id is read from the NWB_FILE_PATH in code/submit.sh when present."""
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["content_id"] == content_id
    assert records[0]["asset_size_bytes"] is None


@pytest.mark.ai_generated
def test_scan_parses_asset_size_from_dandi_asset_lookup(tmp_path: pathlib.Path) -> None:
    """asset_size_bytes is read from DANDI metadata after matching blob ID to content_id."""
    asset_size_bytes = 123456789
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"

    class _FakeAsset:
        path = "sub-mouse01/sub-mouse01_ecephys.nwb"

        def __init__(self, metadata: dict) -> None:
            self._metadata = metadata

        def get_raw_metadata(self) -> dict:
            return self._metadata

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[_FakeAsset]:
            assert path == "sub-mouse01/sub-mouse01_ecephys.nwb"
            return iter(
                [
                    _FakeAsset(
                        {
                            "contentUrl": [
                                "https://api.dandiarchive.org/api/assets/download/",
                                f"https://dandiarchive.s3.amazonaws.com/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}",
                            ],
                            "contentSize": str(asset_size_bytes),
                        }
                    )
                ]
            )

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            assert dandiset_id == "000001"
            return _FakeDandiset()

    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
    )
    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
            ),
        ),
    ):
        records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["asset_size_bytes"] == asset_size_bytes


@pytest.mark.ai_generated
def test_scan_resolves_asset_size_with_session_subdirectory(tmp_path: pathlib.Path) -> None:
    """asset_size_bytes is resolved when dandi_path contains a ses- segment.

    When the attempt directory includes a session subdirectory (ses-<session>),
    the API lookup must use only the subject prefix and filter by the combined
    subject_session filename pattern ending in .nwb.
    """
    asset_size_bytes = 987654321
    content_id = "004fb230-da29-45cf-8d88-33e698f82dea"

    class _FakeAsset:
        path = "sub-CGM3/sub-CGM3_ses-CGM3_ecephys.nwb"

        def __init__(self, metadata: dict) -> None:
            self._metadata = metadata

        def get_raw_metadata(self) -> dict:
            return self._metadata

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[_FakeAsset]:
            assert path == "sub-CGM3/sub-CGM3_ses-CGM3_ecephys.nwb"
            return iter(
                [
                    _FakeAsset(
                        {
                            "contentUrl": [
                                "https://api.dandiarchive.org/api/assets/download/",
                                f"https://dandiarchive.s3.amazonaws.com/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}",
                            ],
                            "contentSize": asset_size_bytes,
                        }
                    )
                ]
            )

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            assert dandiset_id == "000001"
            return _FakeDandiset()

    _make_attempt_dir(
        tmp_path,
        "000001",
        "CGM3",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        session="CGM3",
        content_id=content_id,
    )
    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path="sub-CGM3/sub-CGM3_ses-CGM3_ecephys.nwb",
            ),
        ),
    ):
        records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["asset_size_bytes"] == asset_size_bytes
    assert records[0]["dandi_path"] == "sub-CGM3/sub-CGM3_ses-CGM3_ecephys.nwb"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("session", "asset_filename"),
    [
        # No session: dandi_path = "sub-SUB1", file is sub-SUB1/sub-SUB1_ecephys.nwb
        (None, "sub-SUB1/sub-SUB1_ecephys.nwb"),
        # Session with extra entity in filename: dandi_path = "sub-SUB1/ses-SES1",
        # file is sub-SUB1/sub-SUB1_ses-SES1_obj-OBJ1.nwb (ses- not the last filename entity)
        ("SES1", "sub-SUB1/sub-SUB1_ses-SES1_obj-OBJ1.nwb"),
    ],
)
def test_scan_resolves_asset_size_for_no_session_and_extra_entities(
    tmp_path: pathlib.Path,
    session: str | None,
    asset_filename: str,
) -> None:
    """asset_size_bytes is resolved for subject-only and extra-entity filename cases."""
    asset_size_bytes = 111222333
    content_id = "12345678-0000-0000-0000-000000000abc"

    class _FakeAsset:
        path = asset_filename

        def __init__(self, metadata: dict) -> None:
            self._metadata = metadata

        def get_raw_metadata(self) -> dict:
            return self._metadata

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[_FakeAsset]:
            assert path == asset_filename
            return iter(
                [
                    _FakeAsset(
                        {
                            "contentUrl": [
                                "https://api.dandiarchive.org/api/assets/download/",
                                f"https://dandiarchive.s3.amazonaws.com/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}",
                            ],
                            "contentSize": asset_size_bytes,
                        }
                    )
                ]
            )

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            assert dandiset_id == "000001"
            return _FakeDandiset()

    _make_attempt_dir(
        tmp_path,
        "000001",
        "SUB1",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        session=session,
        content_id=content_id,
    )
    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path=asset_filename,
            ),
        ),
    ):
        records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["asset_size_bytes"] == asset_size_bytes


@pytest.mark.ai_generated
def test_scan_uses_expected_dandi_api_url_for_dandiset(
    tmp_path: pathlib.Path,
) -> None:
    """scan_dandiset_directory uses the live API with token for standard dandisets."""
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    attempt_dir = _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
        with_logs=True,
    )
    expected_log_path = (attempt_dir / "logs" / "nextflow.log").relative_to(tmp_path).as_posix()

    class _FakeAsset:
        path = "sub-mouse01/sub-mouse01_ecephys.nwb"

        def get_raw_metadata(self) -> dict[str, Any]:
            return {
                "contentUrl": [
                    "https://api.dandiarchive.org/api/assets/download/",
                    f"https://dandiarchive.s3.amazonaws.com/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}",
                ],
                "contentSize": 123,
                "dateModified": "2024-06-15T12:30:00+00:00",
            }

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[Any]:
            if path in {"sub-mouse01/sub-mouse01_ecephys.nwb", expected_log_path}:
                return iter([_FakeAsset()])
            return iter(())

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            return _FakeDandiset()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ) as mock_client_ctor,
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
            ),
        ),
    ):
        scan_dandiset_directory(dandiset_directory=tmp_path)

    assert mock_client_ctor.call_count >= 2
    for call in mock_client_ctor.call_args_list:
        assert call.kwargs["token"] == "live-token"
        assert "api_url" not in call.kwargs


@pytest.mark.ai_generated
def test_scan_skips_sandbox_dandiset_directories(tmp_path: pathlib.Path) -> None:
    """scan_dandiset_directory omits attempt directories under the sandbox dandiset-214527 folder."""
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    _make_attempt_dir(
        tmp_path,
        "214527",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
    )
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
    )
    records = scan_dandiset_directory(dandiset_directory=tmp_path)
    dandiset_ids = [r["dandiset_id"] for r in records]
    assert "214527" not in dandiset_ids
    assert "000001" in dandiset_ids


@pytest.mark.ai_generated
def test_scan_raises_on_missing_nwb_file_path(tmp_path: pathlib.Path) -> None:
    """A missing NWB_FILE_PATH in submit.sh raises an error."""
    attempt_dir = _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
    )
    (attempt_dir / "code" / "submit.sh").write_text("#!/bin/bash\n")
    with pytest.raises(ValueError, match="Unable to determine content_id"):
        scan_dandiset_directory(dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_scan_raises_on_empty_content_id_in_nwb_file_path(tmp_path: pathlib.Path) -> None:
    """An NWB_FILE_PATH with an empty trailing path segment raises an error."""
    attempt_dir = _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
    )
    (attempt_dir / "code" / "submit.sh").write_text('#!/bin/bash\nNWB_FILE_PATH=""\n')
    with pytest.raises(ValueError, match="empty content_id"):
        scan_dandiset_directory(dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_scan_asset_size_lookup_falls_back_to_none_when_mapped_asset_path_has_no_match(
    tmp_path: pathlib.Path,
) -> None:
    """asset_size_bytes falls back to None when the mapped unique asset path has no API match."""
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[Any]:
            assert path == "sub-mouse01/sub-mouse01_ecephys.nwb"
            return iter(())

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            assert dandiset_id == "000001"
            return _FakeDandiset()

    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
    )
    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
            ),
        ),
    ):
        with pytest.warns(UserWarning, match="but found 0"):
            records = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert len(records) == 1
    assert records[0]["asset_size_bytes"] is None


# ---------------------------------------------------------------------------
# Tests for refresh_queue_state with dandiset_directory
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_refresh_queue_state_with_dandiset_directory_creates_valid_files(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes state.jsonl when scanning dandiset_directory."""
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
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
    refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)
    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    lines = [line for line in state_file.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "000001"
    assert record["content_id"] == content_id
    assert record["asset_size_bytes"] is None


@pytest.mark.ai_generated
def test_refresh_queue_state_writes_resolved_dandi_path_to_state(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes the API-resolved source path in state.jsonl when lookup succeeds."""
    content_id = "0fbbca6a-0000-0000-0000-000000000001"
    asset_size_bytes = 1234
    mapped_asset_path = "sub-mouse01/./sub-mouse01_ses-ses001_obj-raw.nwb"
    resolved_asset_path = "sub-mouse01/sub-mouse01_ses-ses001_obj-raw.nwb"

    class _FakeAsset:
        def __init__(self, path: str) -> None:
            self.path = path

        def get_raw_metadata(self) -> dict[str, object]:
            return {
                "contentUrl": [
                    "https://api.dandiarchive.org/api/assets/download/",
                    f"https://dandiarchive.s3.amazonaws.com/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}",
                ],
                "contentSize": asset_size_bytes,
            }

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[_FakeAsset]:
            assert path == mapped_asset_path
            return iter([_FakeAsset(resolved_asset_path)])

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            assert dandiset_id == "000001"
            return _FakeDandiset()

    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        session="ses001",
        content_id=content_id,
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
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path=mapped_asset_path,
            ),
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

    class _FakeAsset:
        def __init__(self, path: str) -> None:
            self.path = path

        def get_raw_metadata(self) -> dict[str, object]:
            return {
                "contentUrl": [
                    "https://api.dandiarchive.org/api/assets/download/",
                    f"https://dandiarchive.s3.amazonaws.com/blobs/{content_id[0:3]}/{content_id[3:6]}/{content_id}",
                ],
                "contentSize": asset_size_bytes,
            }

    class _FakeDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[_FakeAsset]:
            assert path == root_asset_path
            return iter([_FakeAsset(root_asset_path)])

    class _FakeClient:
        def get_dandiset(self, dandiset_id: str) -> _FakeDandiset:
            assert dandiset_id == "000001"
            return _FakeDandiset()

    _make_attempt_dir(
        tmp_path,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        session="ses001",
        content_id=content_id,
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
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "live-token"}),
        mock.patch(
            "dandi_compute_code.dandiset._create_dandi_api_client.dandi.dandiapi.DandiAPIClient",
            return_value=_FakeClient(),
        ),
        mock.patch(
            "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
            return_value=_build_mapping_for_content_id(
                content_id=content_id,
                dandiset_id="000001",
                asset_path=root_asset_path,
            ),
        ),
    ):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["asset_size_bytes"] == asset_size_bytes
    assert state_records[0]["dandi_path"] == root_asset_path


@pytest.mark.ai_generated
def test_refresh_queue_state_with_dandiset_directory_empty_when_no_attempts(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes an empty state file with no attempts."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)
    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    assert state_file.read_text() == ""


@pytest.mark.ai_generated
def test_refresh_queue_state_requires_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state requires DANDI_API_KEY to be set."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    with mock.patch.dict("os.environ", {}, clear=True):
        with pytest.raises(AssertionError, match="DANDI_API_KEY"):
            refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_refresh_queue_state_with_dandiset_directory_includes_only_pending_in_waiting(tmp_path: pathlib.Path) -> None:
    """state.jsonl contains all scanned entries from the refreshed dandiset scan."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "test-pipeline", "v1.0", "abc1234", "def5678", 1, with_code=True)
    _make_attempt_dir(
        tmp_path,
        "000002",
        "mouse02",
        "test-pipeline",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
        with_output=True,
    )

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

    refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_lines = [line for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_lines) == 2
    state_records = [json.loads(line) for line in state_lines]
    assert {record["dandiset_id"] for record in state_records} == {"000001", "000002"}


@pytest.mark.ai_generated
def test_refresh_queue_state_with_dandiset_directory_excludes_entries_with_submitted_markers(
    tmp_path: pathlib.Path,
) -> None:
    """refresh_queue_state keeps submitted-marker entries in state.jsonl."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "test-pipeline", "v1.0", "abc1234", "def5678", 1, with_code=True)
    submitted_attempt_dir = _make_attempt_dir(
        tmp_path,
        "000002",
        "mouse02",
        "test-pipeline",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
    )
    (submitted_attempt_dir / "code" / ".submitted").touch()

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
    refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_lines = [line for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_lines) == 2
    state_records = [json.loads(line) for line in state_lines]
    assert {record["dandiset_id"] for record in state_records} == {"000001", "000002"}


# ---------------------------------------------------------------------------
# Tests for the CLI command
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_cli_queue_refresh_with_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh --dandiset scans and writes state.jsonl."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0+abc1234+def5678"],
                        "params_priority": ["default"],
                        "max_fail_per_dandiset": 3,
                    }
                }
            }
        )
    )
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    _make_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
    )
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "refresh", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        env={"DANDI_API_KEY": "test-key"},
    )
    assert result.exit_code == 0, result.output
    assert (queue_dir / "state.jsonl").exists()
    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["dandiset_id"] == "000001"
    assert state_records[0]["content_id"] == content_id


@pytest.mark.ai_generated
def test_cli_queue_refresh_requires_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh requires --dandiset."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    entry = {
        "dandiset_id": "000001",
        "dandi_path": "sub-mouse01",
        "pipeline": "aind+ephys",
        "version": "v1.0",
        "params": "abc1234",
        "config": "def5678",
        "attempt": 1,
        "has_code": True,
        "has_output": False,
        "has_logs": False,
        "created_at": "2024-01-01T00:00:00+00:00",
    }
    (queue_dir / "state.jsonl").write_text(json.dumps(entry) + "\n")
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0+abc1234+def5678"],
                        "params_priority": ["default"],
                        "max_fail_per_dandiset": 3,
                    }
                }
            }
        )
    )
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "refresh", "--queue", str(queue_dir)],
    )
    assert result.exit_code != 0
    assert "Missing option '--dandiset'" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize("dandi_api_key", [None, ""])
def test_cli_queue_refresh_requires_dandi_api_key(tmp_path: pathlib.Path, dandi_api_key: str | None) -> None:
    """dandicompute queue refresh fails immediately when DANDI_API_KEY is missing."""
    dandiset_dir = tmp_path / "dandiset_directory"
    dandiset_dir.mkdir()
    queue_dir = tmp_path / "queue_directory"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))

    runner = CliRunner()
    with mock.patch.dict("os.environ", {}, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
            env={} if dandi_api_key is None else {"DANDI_API_KEY": dandi_api_key},
        )
    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_fails_without_queue_config(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh --dandiset fails when queue_config.json is missing."""
    dandiset_dir = tmp_path / "dandiset_directory"
    dandiset_dir.mkdir()
    queue_dir = tmp_path / "queue_directory"
    queue_dir.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "refresh", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        env={"DANDI_API_KEY": "test-key"},
    )
    assert result.exit_code != 0
    assert "queue_config.json" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_required_queue_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh requires --queue."""
    dandiset_dir = tmp_path / "dandiset_directory"
    dandiset_dir.mkdir()
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["queue", "refresh", "--dandiset", str(dandiset_dir)])
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output
