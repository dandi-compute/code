"""
Unit tests for the queue processing module (dandi_compute_code.queue).

Each test is flagged with the `ai_generated` pytest marker and exercises the
core helper functions using an in-memory, temporary queue directory rather
than touching the network or a SLURM cluster.
"""

import gzip
import importlib.resources
import json
import logging
import os
import pathlib
from collections.abc import Iterator
from datetime import datetime
from typing import Any
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import write_queue_state
from dandi_compute_code.queue._aggregate_queue_statistics import aggregate_queue_statistics
from dandi_compute_code.queue._attempt_dir_candidates import _attempt_dir_candidates
from dandi_compute_code.queue._clean_unsubmitted_capsules import clean_unsubmitted_capsules
from dandi_compute_code.queue._count_running_aind_ephys_pipeline_jobs import _count_running_aind_ephys_pipeline_jobs
from dandi_compute_code.queue._globals import TEST_QUEUE_CONTENT_ID
from dandi_compute_code.queue._load_queue_config import _load_queue_config
from dandi_compute_code.queue._prepare_queue import prepare_queue
from dandi_compute_code.queue._process_queue import process_queue
from dandi_compute_code.queue._resolve_params_key_to_id import _resolve_params_key_to_id
from dandi_compute_code.queue._submit_next import _submit_next
from dandi_compute_code.queue._version_matches import _version_matches

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#: Consolidated queue config used by all tests that need a queue directory.
#: Schema: top-level "pipelines" key; pipeline names are plain (no prefix);
#: version_priority / params_priority are flat lists of plain names;
#: max_attempts_per_asset and asset_overrides live directly on the pipeline object.
_EXAMPLE_QUEUE_CONFIG = {
    "pipelines": {
        "test": {
            "version_priority": ["v1.0"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 2,
            "asset_overrides": {"asset-aaa": 1},
            "max_fail_per_dandiset": 2,
        }
    }
}

_ISSUE_EXAMPLE_QUEUE_CONFIG = {
    "pipelines": {
        "aind+ephys": {
            "version_priority": ["v1.1.1+b268fd2"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 1,
            "asset_overrides": {"048d1ee9-83b7-491f-8f02-1ca615b1d455": None},
            "max_fail_per_dandiset": 10,
        }
    }
}


@pytest.fixture(autouse=True)
def mock_scan_dandi_api_asset_lookup() -> Iterator[None]:
    """Prevent network calls from scan-time asset lookup during queue tests."""

    class _EmptyDandiset:
        def get_assets_with_path_prefix(self, path: str) -> Iterator[Any]:
            assert isinstance(path, str)
            assert path.startswith("sub-") or path.startswith("derivatives/")
            return iter(())

    class _EmptyClient:
        def get_dandiset(self, dandiset_id: str) -> _EmptyDandiset:
            assert isinstance(dandiset_id, str)
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


def _get_default_params_id() -> str:
    params_registry_path = importlib.resources.files("dandi_compute_code.aind_ephys_pipeline").joinpath(
        "registries/registered_params.json"
    )
    return json.loads(params_registry_path.read_text())["default"]["md5"][:7]


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Build a minimal but realistic queue directory under *tmp_path*.

    Structure
    ---------
    queue/
        queue_config.json   (single consolidated config for all pipelines/versions/params)
    """
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    (queue_dir / "queue_config.json").write_text(json.dumps(_EXAMPLE_QUEUE_CONFIG))

    return queue_dir


def _write_jsonl(file_path: pathlib.Path, entries: list[dict]) -> None:
    file_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


def _read_jsonl(file_path: pathlib.Path) -> list[dict]:
    return [json.loads(line) for line in file_path.read_text().splitlines() if line.strip()]


def _mock_urlopen_response(payload: object) -> mock.MagicMock:
    mock_response = mock.MagicMock()
    mock_response.read.return_value = gzip.compress(json.dumps(payload).encode())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False
    return mock_response


def _build_assets_metadata(*, content_id: str, asset_path: str, content_size: int) -> AssetsJsonldMetadata:
    """Build minimal assets metadata containing one source NWB asset."""
    return AssetsJsonldMetadata(
        content_id_to_asset={
            content_id: {
                "path": asset_path,
                "contentSize": content_size,
                "blobDateModified": "2025-01-01T00:00:00+00:00",
            }
        },
        path_to_asset_metadata={
            asset_path: AssetMetadata(
                path=asset_path,
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=content_size,
                content_id=content_id,
            )
        },
    )


def _make_state_entry(
    *,
    dandiset_id: str = "000001",
    dandi_path: str | None = None,
    subject: str = "mouse01",
    session: str | None = None,
    pipeline: str = "test",
    version: str = "v1.0",
    params: str = "default",
    config: str = "abc123",
    attempt: int = 1,
    has_code: bool = True,
    has_output: bool = False,
    has_logs: bool = False,
    created_at: str = "2024-01-01T00:00:00+00:00",
) -> dict:
    """Build a minimal state.jsonl entry."""
    normalized_dandi_path = dandi_path or "/".join(
        filter(None, [f"sub-{subject}", f"ses-{session}" if session else None])
    )
    return {
        "dandiset_id": dandiset_id,
        "dandi_path": normalized_dandi_path,
        "pipeline": pipeline,
        "version": version,
        "params": params,
        "config": config,
        "attempt": attempt,
        "has_code": has_code,
        "has_output": has_output,
        "has_logs": has_logs,
        "created_at": created_at,
    }


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dandi_path", "relative_prefix"),
    [
        ("sub-mouse01", pathlib.Path("derivatives/dandiset-000001/sub-mouse01/pipeline-test")),
        ("sub-mouse01/ses-01", pathlib.Path("derivatives/dandiset-000001/sub-mouse01/ses-01/pipeline-test")),
    ],
)
def test_attempt_dir_candidates_constructs_both_layouts(
    dandi_path: str,
    relative_prefix: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """_attempt_dir_candidates returns both flat and legacy attempt directory paths."""
    entry = _make_state_entry(
        dandiset_id="000001",
        dandi_path=dandi_path,
        pipeline="test",
        version="v1.0",
        params="abc1234",
        config="def5678",
        attempt=2,
    )

    flat_path, legacy_path = _attempt_dir_candidates(base_dir=tmp_path, entry=entry)

    assert flat_path == tmp_path / relative_prefix / "version-v1.0_params-abc1234_config-def5678_attempt-2"
    assert legacy_path == tmp_path / relative_prefix / "version-v1.0/params-abc1234_config-def5678_attempt-2"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("entry", "expected_exception", "expected_message"),
    [
        (
            {
                "dandiset_id": "000001",
                "subject": "mouse01",
                "session": "01",
                "pipeline": "test",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "attempt": 2,
            },
            ValueError,
            r"Entry has invalid dandi_path field \(missing\)",
        ),
        (
            {
                "dandiset_id": "000001",
                "dandi_path": "",
                "pipeline": "test",
                "version": "v1.0",
                "params": "abc1234",
                "config": "def5678",
                "attempt": 2,
            },
            ValueError,
            r"Entry has invalid dandi_path field \(empty\)",
        ),
    ],
)
def test_attempt_dir_candidates_requires_valid_dandi_path(
    entry: dict,
    expected_exception: type[Exception],
    expected_message: str,
    tmp_path: pathlib.Path,
) -> None:
    """_attempt_dir_candidates requires a valid dandi_path value."""
    with pytest.raises(expected_exception, match=expected_message):
        _attempt_dir_candidates(base_dir=tmp_path, entry=entry)


def _make_attempt_dir_with_script(
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
    with_script: bool = True,
) -> pathlib.Path:
    """
    Create a fake attempt directory with an optional submit.sh under *base*.

    Returns the attempt directory path.
    """
    attempt_dir = base / "derivatives" / f"dandiset-{dandiset_id}" / f"sub-{subject}"
    if session:
        attempt_dir = attempt_dir / f"ses-{session}"
    attempt_dir = (
        attempt_dir / f"pipeline-{pipeline}" / f"version-{version}_params-{params}_config-{config}_attempt-{attempt}"
    )
    attempt_dir.mkdir(parents=True)
    code_dir = attempt_dir / "code"
    code_dir.mkdir()
    if with_script:
        (code_dir / "submit.sh").write_text("#!/bin/bash\necho hello\n")
    return attempt_dir


# ---------------------------------------------------------------------------
# Tests for _resolve_params_key_to_id
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_aind_ephys_default() -> None:
    """_resolve_params_key_to_id returns the 7-char hash for a known aind+ephys key."""
    result = _resolve_params_key_to_id("aind+ephys", "default")
    assert result == _get_default_params_id()


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_unknown_pipeline_returns_key() -> None:
    """_resolve_params_key_to_id returns the key unchanged for an unknown pipeline."""
    result = _resolve_params_key_to_id("unknown-pipeline", "default")
    assert result == "default"


@pytest.mark.ai_generated
def test_resolve_params_key_to_id_already_hash_passthrough() -> None:
    """_resolve_params_key_to_id returns the value unchanged if it is already an ID (not a registered key)."""
    result = _resolve_params_key_to_id("aind+ephys", "98fd947")
    # '98fd947' is not a registered key name, so it is returned as-is
    assert result == "98fd947"


# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_version_matches_exact() -> None:
    """_version_matches returns True for exact match."""
    assert _version_matches("v1.1.1+b268fd2", "v1.1.1+b268fd2") is True


@pytest.mark.ai_generated
def test_version_matches_with_code_hash_suffix() -> None:
    """_version_matches returns True when state version has an extra code-repo commit hash suffix."""
    assert _version_matches("v1.1.1+b268fd2+abcdef1", "v1.1.1+b268fd2") is True


@pytest.mark.ai_generated
def test_version_matches_rejects_non_hex_suffix() -> None:
    """_version_matches returns False when the extra suffix is not a hex hash."""
    assert _version_matches("v1.1.1+b268fd2+notahex", "v1.1.1+b268fd2") is False


@pytest.mark.ai_generated
def test_version_matches_rejects_different_version() -> None:
    """_version_matches returns False when the version base is different."""
    assert _version_matches("v1.1.0+b268fd2", "v1.1.1+b268fd2") is False


# ---------------------------------------------------------------------------
# Tests for _count_running_aind_ephys_pipeline_jobs
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_count_running_aind_ephys_pipeline_jobs_counts_exact_name_matches() -> None:
    """Counts only exact AIND-Ephys-Pipeline job names from squeue output."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(
            stdout="JOBNAME\nAIND-Ephys-Pipeline\nAIND-Ephys-Pipeline\nother_job\nAIND_ephys_job\n",
            stderr="",
        )
        assert _count_running_aind_ephys_pipeline_jobs() == 2


@pytest.mark.ai_generated
def test_count_running_aind_ephys_pipeline_jobs_returns_zero_when_no_exact_match() -> None:
    """Returns zero when squeue output has no exact AIND-Ephys-Pipeline names."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(stdout="JOBNAME\nsome_other_job\nAIND\n", stderr="")
        assert _count_running_aind_ephys_pipeline_jobs() == 0


# ---------------------------------------------------------------------------
# Tests for _submit_next
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_submit_next_raises_when_state_file_is_absent(tmp_path: pathlib.Path) -> None:
    """_submit_next raises when state.jsonl is absent."""
    queue_dir = _make_queue_dir(tmp_path)

    with pytest.raises(FileNotFoundError, match="State file not found"):
        _submit_next(queue_directory=queue_dir, datalad_directory=tmp_path)


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_max_submissions_less_than_one(tmp_path: pathlib.Path) -> None:
    """_submit_next returns False for invalid max_submissions before reading state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)
    result = _submit_next(
        queue_directory=queue_dir,
        datalad_directory=tmp_path,
        dandiset_directory=tmp_path,
        max_submissions=0,
    )
    assert result is False


@pytest.mark.ai_generated
def test_submit_next_logs_and_returns_false_when_state_file_is_empty(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """_submit_next logs and returns False when state.jsonl is empty."""
    queue_dir = _make_queue_dir(tmp_path)
    (queue_dir / "state.jsonl").write_text("")

    with caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._submit_next"):
        result = _submit_next(queue_directory=queue_dir, datalad_directory=tmp_path)

    assert result is False
    assert any("No pending entries in" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_no_eligible_entries(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """_submit_next returns False when all ordered entries already have submitted markers."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    first_entry = _make_state_entry(dandiset_id="000001")
    second_entry = _make_state_entry(dandiset_id="000002", subject="mouse02")
    _write_jsonl(queue_dir / "state.jsonl", [first_entry, second_entry])
    first_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    second_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    (first_attempt_dir / "code" / "submitted").touch()
    (second_attempt_dir / "code" / "submitted").touch()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._submit_next"),
        mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit,
    ):
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is False
    mock_submit.assert_not_called()
    assert any("No eligible pending entries available for submission" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_submit_next_submits_first_pending_entry_in_state_order(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first pending entry in state.jsonl order."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True
    mock_submit.assert_called_once()
    assert "submit.sh" in str(mock_submit.call_args.kwargs["script_file_path"])


@pytest.mark.ai_generated
def test_submit_next_raised_when_script_missing(tmp_path: pathlib.Path) -> None:
    """_submit_next raises FileNotFoundError when the submit.sh for the first entry does not exist."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(dandiset_id="000001", pipeline="test", version="v1.0", params="default")
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    # Deliberately do NOT create the attempt directory / submit.sh

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        with pytest.raises(FileNotFoundError, match="Submit script not found"):
            _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)


@pytest.mark.ai_generated
def test_submit_next_found_flat_attempt_directory_under_scanned_dandi_path(tmp_path: pathlib.Path) -> None:
    """_submit_next can submit from flat attempt layout even when dandi_path points to source data."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(
        dandiset_id="001849",
        dandi_path="sourcedata",
        pipeline="test",
        version="v1.1.1+b268fd2+a66c8df",
        params="4af6a25",
        config="0d4bf36_date-2026+05+21",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    actual_attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "pipeline-test"
        / "version-v1.1.1+b268fd2+a66c8df_params-4af6a25_config-0d4bf36_date-2026+05+21_attempt-1"
    )
    (actual_attempt_dir / "code").mkdir(parents=True)
    script_file_path = actual_attempt_dir / "code" / "submit.sh"
    script_file_path.write_text("#!/bin/bash\necho hello\n")

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True
    mock_submit.assert_called_once_with(script_file_path=script_file_path)


@pytest.mark.ai_generated
def test_submit_next_uses_session_in_path_when_present(tmp_path: pathlib.Path) -> None:
    """_submit_next constructs the correct path when the entry has a session field."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        session="ses01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        session="ses01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True
    assert mock_submit.called


@pytest.mark.ai_generated
def test_submit_next_leaves_state_jsonl_unchanged_after_submission(tmp_path: pathlib.Path) -> None:
    """_submit_next submits without mutating state.jsonl contents."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry1 = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    entry2 = _make_state_entry(
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry1, entry2])

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)

    remaining = _read_jsonl(queue_dir / "state.jsonl")
    assert remaining == [entry1, entry2]


@pytest.mark.ai_generated
def test_submit_next_creates_submitted_marker_file(tmp_path: pathlib.Path) -> None:
    """_submit_next creates a submitted marker next to code/submit.sh."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)

    assert (attempt_dir / "code" / "submitted").exists()


@pytest.mark.ai_generated
def test_submit_next_submits_top_two_eligible_entries(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first two eligible entries by default."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    first_entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    second_entry = _make_state_entry(
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    third_entry = _make_state_entry(
        dandiset_id="000003",
        subject="mouse03",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [first_entry, second_entry, third_entry])
    first_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    second_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    third_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000003",
        subject="mouse03",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)

    assert mock_submit.call_count == 2
    assert mock_submit.call_args_list == [
        mock.call(script_file_path=first_attempt_dir / "code" / "submit.sh"),
        mock.call(script_file_path=second_attempt_dir / "code" / "submit.sh"),
    ]
    assert (first_attempt_dir / "code" / "submitted").exists()
    assert (second_attempt_dir / "code" / "submitted").exists()
    assert not (third_attempt_dir / "code" / "submitted").exists()


@pytest.mark.ai_generated
def test_submit_next_deduplicates_same_attempt_directory(tmp_path: pathlib.Path) -> None:
    """_submit_next submits a given attempt directory only once even if state has duplicate entries."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(dandiset_id="000001")
    _write_jsonl(queue_dir / "state.jsonl", [entry, dict(entry)])
    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
        )

    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=attempt_dir / "code" / "submit.sh")
    assert (attempt_dir / "code" / "submitted").exists()


@pytest.mark.ai_generated
def test_submit_next_submits_next_entry_when_first_has_submitted_marker(tmp_path: pathlib.Path) -> None:
    """_submit_next skips entries with code/submitted markers and submits the next one."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    first_entry = _make_state_entry(dandiset_id="000001")
    second_entry = _make_state_entry(dandiset_id="000002", subject="mouse02")
    _write_jsonl(queue_dir / "state.jsonl", [first_entry, second_entry])

    first_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    (first_attempt_dir / "code" / "submitted").touch()
    second_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
        )

    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=second_attempt_dir / "code" / "submit.sh")
    remaining = _read_jsonl(queue_dir / "state.jsonl")
    assert remaining == [first_entry, second_entry]


@pytest.mark.ai_generated
def test_submit_next_only_submits_entries_pending_in_state_jsonl(tmp_path: pathlib.Path) -> None:
    """_submit_next only submits entries listed as pending in state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    _write_jsonl(
        queue_dir / "state.jsonl",
        [
            _make_state_entry(dandiset_id="000001", has_code=True, has_output=True, has_logs=False),
            _make_state_entry(dandiset_id="000002", subject="mouse02", has_code=True, has_output=False, has_logs=False),
        ],
    )
    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    eligible_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
        )

    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=eligible_attempt_dir / "code" / "submit.sh")
    remaining = _read_jsonl(queue_dir / "state.jsonl")
    assert len(remaining) == 2


# ---------------------------------------------------------------------------
# Tests for process_queue (top-level orchestration)
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_process_queue_handles_empty_scan_when_waiting_file_missing(tmp_path: pathlib.Path) -> None:
    """process_queue raises when state.jsonl is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="State file not found"):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )


@pytest.mark.ai_generated
def test_process_queue_refreshes_state_when_empty(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    """process_queue logs and returns when state.jsonl is empty."""
    queue_dir = _make_queue_dir(tmp_path)
    (queue_dir / "state.jsonl").write_text("")
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._process_queue"),
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_not_called()
    assert any("No entries in" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_process_queue_skips_refresh_when_state_non_empty(tmp_path: pathlib.Path) -> None:
    """process_queue runs without warning when state.jsonl already has entries."""
    queue_dir = _make_queue_dir(tmp_path)
    entry = _make_state_entry()
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_skips_when_lock_is_already_held(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """process_queue skips submission when another process holds the lock."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._process_queue"),
        mock.patch(
            "dandi_compute_code.queue._process_queue.fcntl.flock",
            side_effect=BlockingIOError,
        ),
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs") as mock_running,
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    assert any("Skipping queue processing: lock already held" in record.message for record in caplog.records)
    mock_running.assert_not_called()
    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue requests two submissions when no AIND jobs are running."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=dandiset_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=2,
    )


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue does not submit when two AIND-Ephys-Pipeline jobs already run."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=2),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_submits_one_when_one_job_running(tmp_path: pathlib.Path) -> None:
    """process_queue requests one submission when exactly one AIND-Ephys-Pipeline job is running."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=1),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=dandiset_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=1,
    )


@pytest.mark.ai_generated
def test_process_queue_passes_datalad_directory_to_submit_next_when_matching_dandiset(
    tmp_path: pathlib.Path,
) -> None:
    """process_queue forwards datalad_directory to _submit_next when idle."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(
            queue_directory=queue_dir,
            dandiset_directory=dandiset_dir,
            datalad_directory=dandiset_dir,
        )

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=dandiset_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=2,
    )


@pytest.mark.ai_generated
def test_process_queue_passes_datalad_directory_to_submit_next(tmp_path: pathlib.Path) -> None:
    """process_queue forwards explicit datalad_directory to _submit_next when idle."""
    queue_dir = _make_queue_dir(tmp_path)
    _write_jsonl(queue_dir / "state.jsonl", [_make_state_entry()])
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()
    datalad_dir = tmp_path / "datalad"
    datalad_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next", return_value=True) as mock_submit,
    ):
        process_queue(queue_directory=queue_dir, dandiset_directory=dandiset_dir, datalad_directory=datalad_dir)

    mock_submit.assert_called_once_with(
        queue_directory=queue_dir,
        datalad_directory=datalad_dir,
        dandiset_directory=dandiset_dir,
        max_submissions=2,
    )


# ---------------------------------------------------------------------------
# Tests for order_queue and write_queue_state
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_order_queue_raises_when_queue_config_missing(tmp_path: pathlib.Path) -> None:
    """write_queue_state raises FileNotFoundError when queue_config.json is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="queue_config.json"):
        write_queue_state(queue_directory=queue_dir)


@pytest.mark.ai_generated
def test_load_queue_config_validates_issue_example_schema(tmp_path: pathlib.Path) -> None:
    """Issue-provided queue config validates against the LinkML schema."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps(_ISSUE_EXAMPLE_QUEUE_CONFIG))

    loaded = _load_queue_config(queue_directory=queue_dir)

    assert loaded == _ISSUE_EXAMPLE_QUEUE_CONFIG


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

    with (
        mock.patch("dandi_compute_code.queue._refresh_queue.scan_dandiset_directory", return_value=[]),
        pytest.raises(ValueError, match="LinkML validation failed"),
    ):
        write_queue_state(queue_directory=queue_dir)


@pytest.mark.ai_generated
def test_prepare_queue_raises_when_queue_config_fails_linkml_validation(tmp_path: pathlib.Path) -> None:
    """prepare_queue raises when queue_config violates LinkML constraints."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    invalid_queue_config = {
        "pipelines": {
            # Violates schema minimum_value: 0 constraint.
            "test": {"version_priority": ["v1.0"], "params_priority": ["default"], "max_fail_per_dandiset": -1}
        }
    }
    (queue_dir / "queue_config.json").write_text(json.dumps(invalid_queue_config))

    with pytest.raises(ValueError, match="LinkML validation failed"):
        prepare_queue(queue_directory=queue_dir, content_ids=[])


@pytest.mark.ai_generated
def test_write_queue_state_writes_empty_files_for_missing_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes an empty state file when dandiset directory does not exist."""
    queue_dir = _make_queue_dir(tmp_path)

    write_queue_state(queue_directory=queue_dir)

    assert (queue_dir / "state.jsonl").read_text() == ""


@pytest.mark.ai_generated
def test_order_queue_writes_waiting_jsonl_from_state_entries(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes scanned entries to state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)

    entries = [
        _make_state_entry(dandiset_id="000001", has_code=True, has_output=False, has_logs=False),
        # Already has output (kept in state.jsonl for authoritative tracking)
        _make_state_entry(dandiset_id="000002", has_code=True, has_output=True, has_logs=False),
    ]
    with mock.patch("dandi_compute_code.queue._refresh_queue.scan_dandiset_directory", return_value=entries):
        write_queue_state(queue_directory=queue_dir)

    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    state_entries = _read_jsonl(state_file)
    assert len(state_entries) == 2
    assert {entry["dandiset_id"] for entry in state_entries} == {"000001", "000002"}


@pytest.mark.ai_generated
def test_write_queue_state_writes_all_ordered_pending_entries(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes all scanned entries to state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)

    entries = [
        _make_state_entry(dandiset_id=f"00000{i}", has_code=True, has_output=False, has_logs=False) for i in range(1, 6)
    ]
    with mock.patch("dandi_compute_code.queue._refresh_queue.scan_dandiset_directory", return_value=entries):
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 5


@pytest.mark.ai_generated
def test_write_queue_state_excludes_entries_with_submitted_markers(tmp_path: pathlib.Path) -> None:
    """write_queue_state writes submitted-marker entries into state.jsonl unchanged."""
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
        write_queue_state(queue_directory=queue_dir)

    state_entries = _read_jsonl(queue_dir / "state.jsonl")
    assert len(state_entries) == 2
    assert {entry["dandiset_id"] for entry in state_entries} == {"000001", "000002"}


@pytest.mark.ai_generated
@pytest.mark.ai_generated
def test_aggregate_queue_statistics_writes_queue_stats_json(tmp_path: pathlib.Path) -> None:
    """aggregate_queue_statistics writes queue_stats.json with byte and timeline aggregates."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry_with_output = _make_state_entry(has_output=True, has_logs=True)
    entry_with_output["asset_size_bytes"] = 120
    entry_without_output = _make_state_entry(dandiset_id="000002", subject="mouse02", has_output=False, has_logs=True)
    entry_without_output["asset_size_bytes"] = 999
    _write_jsonl(queue_dir / "state.jsonl", [entry_with_output, entry_without_output])

    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    logs_dir = attempt_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "timeline.html").write_text("""<script>
window.data = {
  "processes": [
    {"label": "step_one (step-one)", "times": [{"label": "1m 5s / 1 GB"}]},
    {"label": "step_two (step-two)", "times": [{"label": "30s / 500 MB"}, {"label": "2m / 1 GB"}]}
  ]
};
</script>""")

    stats = aggregate_queue_statistics(queue_directory=queue_dir, dandiset_directory=dandiset_dir)

    queue_stats_file = queue_dir / "queue_stats.json"
    assert queue_stats_file.exists()
    assert stats["state_entry_count"] == 2
    assert stats["successful_asset_bytes_total"] == 120
    assert stats["timeline_files_processed"] == 1
    assert datetime.fromisoformat(stats["generated_at"])
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(65.0)
    assert stats["job_step_wall_time_seconds"]["step_two"] == pytest.approx(150.0)
    assert json.loads(queue_stats_file.read_text()) == stats


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_skips_invalid_timeline_html(tmp_path: pathlib.Path) -> None:
    """aggregate_queue_statistics ignores timeline files with malformed embedded JSON."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(has_output=True, has_logs=True)
    entry["asset_size_bytes"] = 120
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    logs_dir = attempt_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "timeline.html").write_text("<script>window.data = {invalid json};</script>")

    stats = aggregate_queue_statistics(queue_directory=queue_dir, dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 0
    assert stats["job_step_wall_time_seconds"] == {}


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_found_timeline_via_fallback_attempt_resolution(tmp_path: pathlib.Path) -> None:
    """aggregate_queue_statistics finds timeline files when state dandi_path differs from on-disk attempt path."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(
        dandiset_id="001849",
        dandi_path="sourcedata",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+a66c8df",
        params="4af6a25",
        config="0d4bf36_date-2026+05+21",
        has_output=True,
        has_logs=True,
    )
    entry["asset_size_bytes"] = 120
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+a66c8df_params-4af6a25_config-0d4bf36_date-2026+05+21_attempt-1"
    )
    logs_dir = attempt_dir / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "timeline.html").write_text("""<script>
window.data = {
  "processes": [
    {"label": "step_one (step-one)", "times": [{"label": "1s / 1 GB"}]}
  ]
};
</script>""")

    stats = aggregate_queue_statistics(queue_directory=queue_dir, dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 1
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Helpers for _submit_next tests
# ---------------------------------------------------------------------------


def _make_attempt_dir(
    base: pathlib.Path,
    dandiset_id: str,
    version: str,
    params_id: str,
    config_id: str,
    attempt_number: int,
    *,
    with_code: bool = True,
    with_output: bool = False,
    with_logs: bool = False,
) -> pathlib.Path:
    """
    Create a mock attempt directory inside a fake 001697 clone rooted at *base*.

    A directory path

    ``derivatives/dandiset-{dandiset_id}/sub-test/pipeline-aind+ephys/``
    ``version-{version}_params-{params_id}_config-{config_id}_attempt-{attempt_number}/``

    is created.  *with_code*, *with_output*, and *with_logs* control whether the
    ``code/``, ``derivatives/``, and ``logs/`` subdirectories are created.  When
    *with_logs* is True a sentinel file is written inside ``logs/``.
    """
    attempt_dir = (
        base
        / "derivatives"
        / f"dandiset-{dandiset_id}"
        / "sub-test"
        / "pipeline-aind+ephys"
        / f"version-{version}_params-{params_id}_config-{config_id}_attempt-{attempt_number}"
    )
    attempt_dir.mkdir(parents=True)
    if with_code:
        (attempt_dir / "code").mkdir()
    if with_output:
        (attempt_dir / "derivatives").mkdir()
    if with_logs:
        logs_dir = attempt_dir / "logs"
        logs_dir.mkdir()
        (logs_dir / "run.log").write_text("job output\n")
    return attempt_dir


# ---------------------------------------------------------------------------
# Tests for _submit_next simplification behavior
# ---------------------------------------------------------------------------

#: params_id and config_id used when building fake attempt directories.
_FAKE_PARAMS_ID = "abc1234"
_FAKE_CONFIG_ID = "def5678"


@pytest.mark.ai_generated
def test_submit_next_submits_first_entry_directly(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first ordered state entry directly."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"

    # Create 2 failure attempt dirs for the dandiset (== max_fail_per_dandiset=2)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1, with_logs=True)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 2, with_logs=True)

    # state.jsonl: first entry will be submitted directly.
    _write_jsonl(
        queue_dir / "state.jsonl",
        [
            _make_state_entry(dandiset_id="000001", version="v1.0"),
            _make_state_entry(dandiset_id="000002", version="v1.0"),
        ],
    )

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        result = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
            max_submissions=1,
        )

    assert result is True


@pytest.mark.ai_generated
def test_submit_next_submits_first_entry_with_existing_failure_dirs(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first ordered state entry directly even if failure dirs exist."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"

    # Only 1 failure (< max_fail_per_dandiset=2) → entry should be submitted
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1, with_logs=True)

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    # Create the submit script so _submit_next can proceed
    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True


# ---------------------------------------------------------------------------
# Tests for prepare_queue
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_prepare_queue_calls_prepare_for_each_qualifying_asset(tmp_path: pathlib.Path) -> None:
    """prepare_queue calls prepare_aind_ephys_job for every qualifying content ID."""
    queue_dir = _make_queue_dir(tmp_path)

    qualifying_ids = ["asset-bbb", "asset-ccc"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_unique_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir)

    assert mock_prepare.call_count == 2
    prepared_ids = {call.kwargs["content_id"] for call in mock_prepare.call_args_list}
    assert prepared_ids == {"asset-bbb", "asset-ccc"}


@pytest.mark.ai_generated
def test_prepare_queue_skips_when_failures_reach_max(tmp_path: pathlib.Path) -> None:
    """prepare_queue skips assets for dandisets whose failure count reaches max_fail_per_dandiset."""
    queue_dir = _make_queue_dir(tmp_path)
    # Write 2 failure entries to state.jsonl (== max_fail_per_dandiset from _EXAMPLE_QUEUE_CONFIG).
    failure_entries = [
        {
            **_make_state_entry(
                dandiset_id="000001",
                pipeline="test",
                version="v1.0",
                attempt=1,
                has_code=True,
                has_logs=True,
                has_output=False,
            ),
            "content_id": "asset-aaa",
        },
        {
            **_make_state_entry(
                dandiset_id="000001",
                pipeline="test",
                version="v1.0",
                attempt=2,
                has_code=True,
                has_logs=True,
                has_output=False,
            ),
            "content_id": "asset-aaa",
        },
        {
            **_make_state_entry(
                dandiset_id="000002",
                pipeline="test",
                version="v1.0",
                attempt=1,
                has_code=True,
                has_logs=False,
                has_output=False,
            ),
            "content_id": "asset-bbb",
        },
    ]
    _write_jsonl(queue_dir / "state.jsonl", failure_entries)

    qualifying_ids = ["asset-aaa", "asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_unique_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir)

    assert mock_prepare.call_count == 1
    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-bbb"]


@pytest.mark.ai_generated
def test_prepare_queue_strips_commit_suffix_from_version(tmp_path: pathlib.Path) -> None:
    """prepare_queue passes the version without its trailing commit-hash to prepare_aind_ephys_job."""
    queue_dir = _make_queue_dir(tmp_path)
    queue_config = json.loads((queue_dir / "queue_config.json").read_text())
    queue_config["pipelines"]["test"]["version_priority"] = ["v1.1.0+abcdef0"]
    (queue_dir / "queue_config.json").write_text(json.dumps(queue_config))

    qualifying_ids = ["asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_unique_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir)

    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["pipeline_version"] == "v1.1.0"


@pytest.mark.ai_generated
def test_prepare_queue_passes_optional_args_through(tmp_path: pathlib.Path) -> None:
    """prepare_queue forwards optional args to prepare_aind_ephys_job."""
    queue_dir = _make_queue_dir(tmp_path)

    fake_pipeline_dir = tmp_path / "pipeline"
    fake_pipeline_dir.mkdir()

    qualifying_ids = ["asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_unique_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(
            queue_directory=queue_dir,
            pipeline_directory=fake_pipeline_dir,
            config_key="mit+engaging+revision-1",
        )

    assert mock_prepare.call_count == 1
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["pipeline_directory"] == fake_pipeline_dir
    assert call_kwargs["config_key"] == "mit+engaging+revision-1"


@pytest.mark.ai_generated
def test_prepare_queue_limit_stops_after_n_assets(tmp_path: pathlib.Path) -> None:
    """prepare_queue stops after preparing exactly limit assets when limit is set."""
    queue_dir = _make_queue_dir(tmp_path)

    qualifying_ids = ["asset-aaa", "asset-bbb", "asset-ccc"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_unique_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir, limit=2)

    assert mock_prepare.call_count == 2


@pytest.mark.ai_generated
def test_prepare_queue_limit_samples_uniformly_over_dandisets(tmp_path: pathlib.Path) -> None:
    """prepare_queue interleaves qualifying assets so --limit is not biased by asset-rich Dandisets."""
    queue_dir = _make_queue_dir(tmp_path)
    qualifying_ids = ["asset-a1", "asset-a2", "asset-b1"]
    content_id_mapping = {
        "asset-a1": {"000001": "sub-a1/sub-a1_ecephys.nwb"},
        "asset-a2": {"000001": "sub-a2/sub-a2_ecephys.nwb"},
        "asset-b1": {"000002": "sub-b1/sub-b1_ecephys.nwb"},
    }

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_unique_dandiset_path",
            return_value=content_id_mapping,
        ),
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling.random.shuffle",
            side_effect=lambda items: None,
        ) as mock_shuffle,
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir, limit=2)

    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-a1", "asset-b1"]
    assert mock_shuffle.call_count == 3
    assert {tuple(group) for group in mock_shuffle.call_args_list[0].args[0]} == {
        ("asset-a1", "asset-a2"),
        ("asset-b1",),
    }
    assert set(mock_shuffle.call_args_list[1].args[0]) == {"asset-a1", "asset-a2"}
    assert set(mock_shuffle.call_args_list[2].args[0]) == {"asset-b1"}


@pytest.mark.ai_generated
def test_prepare_queue_uses_explicit_content_ids_when_provided(tmp_path: pathlib.Path) -> None:
    """prepare_queue uses provided content_ids directly and skips the network fetch."""
    queue_dir = _make_queue_dir(tmp_path)
    explicit_ids = ["explicit-asset-001"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        prepare_queue(queue_directory=queue_dir, content_ids=explicit_ids)

    mock_urlopen.assert_not_called()
    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["content_id"] == "explicit-asset-001"


@pytest.mark.ai_generated
def test_cli_prepare_test_calls_prepare_queue_with_test_content_id(tmp_path: pathlib.Path) -> None:
    """dandicompute prepare aind --test calls prepare_queue with the known test content ID."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.prepare_queue") as mock_prepare_queue,
    ):
        result = runner.invoke(_dandicompute_group, ["prepare", "aind", "--test", "--queue", str(queue_dir)])

    assert result.exit_code == 0
    mock_prepare_queue.assert_called_once_with(
        queue_directory=queue_dir,
        content_ids=[TEST_QUEUE_CONTENT_ID],
        pipeline_directory=None,
        config_key="default",
    )


@pytest.mark.ai_generated
def test_cli_prepare_test_passes_config_key(tmp_path: pathlib.Path) -> None:
    """dandicompute prepare aind --test forwards --config to prepare_queue."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.prepare_queue") as mock_prepare_queue,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["prepare", "aind", "--test", "--queue", str(queue_dir), "--config", "mit+engaging+revision-1"],
        )

    assert result.exit_code == 0
    mock_prepare_queue.assert_called_once_with(
        queue_directory=queue_dir,
        content_ids=[TEST_QUEUE_CONTENT_ID],
        pipeline_directory=None,
        config_key="mit+engaging+revision-1",
    )


@pytest.mark.ai_generated
def test_cli_prepare_test_required_queue_directory() -> None:
    """dandicompute prepare aind --test requires --queue."""
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, ["prepare", "aind", "--test"])
    assert result.exit_code != 0
    assert "--queue is required when using --test" in result.output


@pytest.mark.ai_generated
def test_cli_aind_prepare_passes_config_key() -> None:
    """dandicompute prepare aind forwards --config to prepare_aind_ephys_job."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_prepare.return_value = pathlib.Path("/tmp/submit.sh")
        result = runner.invoke(
            _dandicompute_group,
            [
                "prepare",
                "aind",
                "--id",
                "abc123",
                "--version",
                "v1.2.3",
                "--config",
                "mit+engaging+revision-1",
            ],
        )

    assert result.exit_code == 0
    assert mock_prepare.call_args.kwargs["config_key"] == "mit+engaging+revision-1"


# ---------------------------------------------------------------------------
# Tests for clean_unsubmitted_capsules
# ---------------------------------------------------------------------------


def _make_full_attempt_dir(
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
) -> pathlib.Path:
    """Create a mock attempt directory with optional code/, derivatives/, and logs/."""
    parts = [base, "derivatives", f"dandiset-{dandiset_id}", f"sub-{subject}"]
    if session is not None:
        parts.append(f"ses-{session}")
    parts += [
        f"pipeline-{pipeline}",
        f"version-{version}_params-{params}_config-{config}_attempt-{attempt}",
    ]
    attempt_dir = pathlib.Path(*parts)
    attempt_dir.mkdir(parents=True)
    if with_code:
        code_dir = attempt_dir / "code"
        code_dir.mkdir()
        (code_dir / "submit.sh").write_text(
            f"#!/bin/bash\nNWB_FILE_PATH=s3://dandi/mock/{dandiset_id}-{subject}-{attempt}\n"
        )
    if with_output:
        (attempt_dir / "derivatives").mkdir()
    if with_logs:
        logs_dir = attempt_dir / "logs"
        logs_dir.mkdir()
        (logs_dir / "run.log").write_text("job output\n")
    return attempt_dir


def _make_full_attempt_dir_legacy_nested(
    *,
    base: pathlib.Path,
    dandiset_id: str,
    subject: str,
    pipeline: str,
    version: str,
    params: str,
    config: str,
    attempt: int,
    session: str | None = None,
    with_code: bool = True,
    with_output: bool = False,
    with_logs: bool = False,
) -> pathlib.Path:
    """Create a mock attempt dir in legacy layout: pipeline/version/params_config_attempt."""
    parts = [base, "derivatives", f"dandiset-{dandiset_id}", f"sub-{subject}"]
    if session is not None:
        parts.append(f"ses-{session}")
    parts += [
        f"pipeline-{pipeline}",
        f"version-{version}",
        f"params-{params}_config-{config}_attempt-{attempt}",
    ]
    attempt_dir = pathlib.Path(*parts)
    attempt_dir.mkdir(parents=True)
    if with_code:
        code_dir = attempt_dir / "code"
        code_dir.mkdir()
        (code_dir / "submit.sh").write_text(
            f"#!/bin/bash\nNWB_FILE_PATH=s3://dandi/mock/{dandiset_id}-{subject}-{attempt}\n"
        )
    if with_output:
        (attempt_dir / "derivatives").mkdir()
    if with_logs:
        logs_dir = attempt_dir / "logs"
        logs_dir.mkdir()
        (logs_dir / "run.log").write_text("job output\n")
    return attempt_dir


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_raises_without_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules raises RuntimeError when DANDI_API_KEY is not set."""
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            clean_unsubmitted_capsules(dandiset_directory=tmp_path, queue_directory=tmp_path)


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_queued_directories(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes capsule dirs that are queued (code, no logs, no output)."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    mock_run.assert_called_once_with(
        ["dandi", "delete", str(queued_dir)],
        input=b"y\n",
        check=True,
    )


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_output(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules does not remove capsules that already have output."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    completed_dir = _make_full_attempt_dir(
        dandiset_dir,
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

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []
    assert completed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_logs(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules does not remove capsules that have logs (already run)."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    failed_dir = _make_full_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
        with_logs=True,
    )

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []
    assert failed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_ignores_dataset_description_in_logs(
    tmp_path: pathlib.Path,
) -> None:
    """clean_unsubmitted_capsules removes a capsule whose logs/ dir contains only dataset_description.json.

    When prepare_job uploads an empty logs/ directory to DANDI, a dataset_description.json
    metadata file may be added inside it. That file must not be treated as evidence of
    actual job logs. The capsule should still qualify for cleaning.
    """
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    # Simulate a logs/ directory that contains only a dataset_description.json metadata file.
    logs_dir = queued_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "dataset_description.json").write_text("{}\n")

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_submitted_marker(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules does not remove capsules with a code/submitted marker."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )

    (queued_dir / "code" / "submitted").touch()

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []
    assert queued_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_returns_empty_list_when_nothing_queued(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules returns an empty list when there are no queued capsules."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_handles_session_in_path(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules correctly handles attempt dirs with a session component."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        session="ses001",
        with_code=True,
    )

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_empty_parent_directories(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes empty pipeline/version dirs after last capsule removal."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir,
        "001371",
        "S25",
        "aind+ephys",
        "v1.1.1+b268fd2",
        "abc1234",
        "def5678",
        1,
        session="S25-210913",
        with_code=True,
    )
    version_dir = queued_dir.parent
    pipeline_dir = version_dir.parent

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not version_dir.exists()
    assert not pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_keeps_non_empty_parent_directories(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules keeps pipeline/version dirs when a sibling attempt remains."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    remaining_dir = _make_full_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        2,
        with_code=True,
        with_output=True,
    )
    version_dir = queued_dir.parent
    pipeline_dir = version_dir.parent

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert remaining_dir.exists()
    assert version_dir.exists()
    assert pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_legacy_nested_layout(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes queued capsules in the legacy nested layout."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir_legacy_nested(
        base=dandiset_dir,
        dandiset_id="001371",
        subject="S25",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2",
        params="abc1234",
        config="def5678",
        attempt=1,
        session="S25-210913",
        with_code=True,
    )
    version_dir = queued_dir.parent
    pipeline_dir = version_dir.parent

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert not version_dir.exists()
    assert not pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_only_queued_not_submitted(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules only removes queued capsules, leaving submitted ones intact."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    # Queued (should be removed)
    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    # Submitted marker exists – should be kept
    submitted_dir = _make_full_attempt_dir(
        dandiset_dir, "000002", "mouse02", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    (submitted_dir / "code" / "submitted").touch()

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert submitted_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removed_entry_via_fallback_attempt_resolution(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes queued entry when dandi_path differs from on-disk attempt path."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+a66c8df_params-4af6a25_config-0d4bf36_date-2026+05+21_attempt-1"
    )
    (attempt_dir / "code").mkdir(parents=True)
    (attempt_dir / "code" / "submit.sh").write_text("#!/bin/bash\necho hello\n")
    state_entry = _make_state_entry(
        dandiset_id="001849",
        dandi_path="sourcedata",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+a66c8df",
        params="4af6a25",
        config="0d4bf36_date-2026+05+21",
        has_code=True,
        has_logs=False,
        has_output=False,
    )

    with (
        mock.patch(
            "dandi_compute_code.queue._clean_unsubmitted_capsules.scan_dandiset_directory", return_value=[state_entry]
        ),
        mock.patch("subprocess.run") as mock_run,
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
    ):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [attempt_dir]
    assert not attempt_dir.exists()
    mock_run.assert_called_once_with(["dandi", "delete", str(attempt_dir)], input=b"y\n", check=True)


@pytest.mark.ai_generated
def test_cli_queue_clean_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute queue clean delegates to clean_unsubmitted_capsules and reports removed paths."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    fake_removed = [dandiset_dir / "derivatives" / "dandiset-000001" / "sub-mouse01" / "attempt-1"]
    runner = CliRunner()

    with mock.patch(
        "dandi_compute_code._cli._dandicompute_group.clean_unsubmitted_capsules", return_value=fake_removed
    ) as mock_clean:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "clean",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
            ],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    mock_clean.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Cleaned 1 unsubmitted capsule" in result.output


@pytest.mark.ai_generated
def test_cli_queue_clean_reports_nothing_found(tmp_path: pathlib.Path) -> None:
    """dandicompute queue clean reports when no unsubmitted capsules are found."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()

    with mock.patch("dandi_compute_code._cli._dandicompute_group.clean_unsubmitted_capsules", return_value=[]):
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "clean",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
            ],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    assert "No unsubmitted capsules found" in result.output


@pytest.mark.ai_generated
def test_cli_queue_stats_calls_helper_and_reports_output(tmp_path: pathlib.Path) -> None:
    """dandicompute queue stats delegates to aggregate_queue_statistics."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch(
        "dandi_compute_code._cli._dandicompute_group.aggregate_queue_statistics",
        return_value={"successful_asset_bytes_total": 0},
    ) as mock_stats:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "stats",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_stats.assert_called_once_with(
        queue_directory=queue_dir,
        dandiset_directory=dandiset_dir,
        output_file_name="queue_stats.json",
    )
    assert "Wrote queue aggregate statistics" in result.output


@pytest.mark.ai_generated
def test_cli_issues_dump_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute issues dump delegates to dump_issues and reports output."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch("dandi_compute_code._cli._dandicompute_group.dump_issues", return_value=[]) as mock_dump:
        result = runner.invoke(
            _dandicompute_group,
            [
                "issues",
                "dump",
                "--directory",
                str(dandiset_dir),
                "--queue",
                str(queue_dir),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_dump.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Wrote issue dump" in result.output


@pytest.mark.ai_generated
def test_cli_issues_summarize_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute issues summarize delegates to summarize_issues and reports output."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch("dandi_compute_code._cli._dandicompute_group.summarize_issues", return_value={}) as mock_summarize:
        result = runner.invoke(
            _dandicompute_group,
            [
                "issues",
                "summarize",
                "--directory",
                str(dandiset_dir),
                "--queue",
                str(queue_dir),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_summarize.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Wrote issue summary" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "subcommand",
    [
        "clean",
        "stats",
        "process",
    ],
)
def test_cli_queue_subcommands_required_queue_directory(tmp_path: pathlib.Path, subcommand: str) -> None:
    """Queue clean/process commands require --queue."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    args = ["queue", subcommand, "--dandiset", str(dandiset_dir)]
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, args)
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output


@pytest.mark.ai_generated
def test_cli_queue_prepare_required_queue_directory() -> None:
    """Queue prepare command requires --queue."""
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, ["queue", "prepare"])
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_requires_datalad_directory(tmp_path: pathlib.Path) -> None:
    """Queue process command requires --datalad."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        )
    assert result.exit_code != 0
    assert "Missing option '--datalad'" in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_passes_explicit_datalad_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue process forwards --datalad to process_queue."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    datalad_dir = tmp_path / "datalad"
    datalad_dir.mkdir()
    runner = CliRunner()

    with mock.patch("dandi_compute_code._cli._dandicompute_group.process_queue") as mock_process:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "process",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
                "--datalad",
                str(datalad_dir),
            ],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    mock_process.assert_called_once_with(
        queue_directory=queue_dir,
        dandiset_directory=dandiset_dir,
        datalad_directory=datalad_dir,
    )


@pytest.mark.ai_generated
def test_cli_queue_process_failed_when_submit_script_missing(tmp_path: pathlib.Path) -> None:
    """dandicompute queue process exits non-zero when a queued submit script is missing."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    _write_jsonl(
        queue_dir / "state.jsonl",
        [_make_state_entry(dandiset_id="000001", pipeline="test", version="v1.0", params="default")],
    )
    runner = CliRunner()
    datalad_dir = tmp_path / "datalad"
    datalad_dir.mkdir()
    with mock.patch("dandi_compute_code.queue._process_queue._count_running_aind_ephys_pipeline_jobs", return_value=0):
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "process",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
                "--datalad",
                str(datalad_dir),
            ],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code != 0
    assert result.exception is not None
    assert "Submit script not found" in str(result.exception)
