"""
Unit tests for the queue processing module (dandi_compute_code.queue).

Each test is flagged with the `ai_generated` pytest marker and exercises the
core helper functions using an in-memory, temporary queue directory rather
than touching the network or a SLURM cluster.
"""

import collections
import gzip
import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue._process_queue import (
    _build_processing_order,
    _count_dandiset_failures,
    _determine_running,
    _fetch_counts,
    _submit_next,
    order_queue,
    prepare_queue,
    process_queue,
)

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


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Build a minimal but realistic queue directory under *tmp_path*.

    Structure
    ---------
    queue/
        submitted.jsonl
        queue_config.json   (single consolidated config for all pipelines/versions/params)
    """
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    (queue_dir / "submitted.jsonl").write_text("")
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


def _make_state_entry(
    *,
    dandiset_id: str = "000001",
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
    return {
        "dandiset_id": dandiset_id,
        "subject": subject,
        "session": session,
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
        attempt_dir
        / f"pipeline-{pipeline}"
        / f"version-{version}"
        / f"params-{params}_config-{config}_attempt-{attempt}"
    )
    attempt_dir.mkdir(parents=True)
    code_dir = attempt_dir / "code"
    code_dir.mkdir()
    if with_script:
        (code_dir / "submit.sh").write_text("#!/bin/bash\necho hello\n")
    return attempt_dir


# ---------------------------------------------------------------------------
# Tests for _fetch_counts
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_fetch_counts_empty_file(tmp_path: pathlib.Path) -> None:
    """_fetch_counts returns an empty Counter when the file is empty."""
    jsonl_file = tmp_path / "submitted.jsonl"
    jsonl_file.write_text("")

    result = _fetch_counts(
        file_path=jsonl_file,
        pipeline="test",
        version="v1.0",
        params="default",
    )

    assert result == collections.Counter()


@pytest.mark.ai_generated
def test_fetch_counts_matches_pipeline(tmp_path: pathlib.Path) -> None:
    """_fetch_counts only counts entries that match pipeline/version/params."""
    jsonl_file = tmp_path / "submitted.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"},
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"},
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"},
            # Different pipeline – should NOT be counted
            {"pipeline": "other", "version": "v1.0", "params": "default", "content_id": "asset-aaa"},
            # Different params – should NOT be counted
            {"pipeline": "test", "version": "v1.0", "params": "other", "content_id": "asset-bbb"},
        ],
    )

    result = _fetch_counts(
        file_path=jsonl_file,
        pipeline="test",
        version="v1.0",
        params="default",
    )

    assert result["asset-aaa"] == 2
    assert result["asset-bbb"] == 1
    assert result["asset-ccc"] == 0


@pytest.mark.ai_generated
def test_fetch_counts_ignores_blank_lines(tmp_path: pathlib.Path) -> None:
    """_fetch_counts gracefully skips blank lines in the JSONL file."""
    jsonl_file = tmp_path / "submitted.jsonl"
    jsonl_file.write_text(
        "\n"
        + json.dumps({"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-ccc"})
        + "\n\n"
    )

    result = _fetch_counts(
        file_path=jsonl_file,
        pipeline="test",
        version="v1.0",
        params="default",
    )

    assert result["asset-ccc"] == 1


# ---------------------------------------------------------------------------
# Tests for _build_processing_order
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_build_processing_order_empty_state() -> None:
    """_build_processing_order returns an empty list when state_entries is empty."""
    result = _build_processing_order(state_entries=[], queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert result == []


@pytest.mark.ai_generated
def test_build_processing_order_filters_out_entries_with_output() -> None:
    """_build_processing_order excludes entries that already have output."""
    entries = [
        _make_state_entry(has_code=True, has_output=True, has_logs=False),
        _make_state_entry(has_code=True, has_output=False, has_logs=False, dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_filters_out_entries_with_logs() -> None:
    """_build_processing_order excludes entries that already have logs (running or failed)."""
    entries = [
        _make_state_entry(has_code=True, has_output=False, has_logs=True),
        _make_state_entry(has_code=True, has_output=False, has_logs=False, dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_filters_out_entries_without_code() -> None:
    """_build_processing_order excludes entries that have no code directory yet."""
    entries = [
        _make_state_entry(has_code=False, has_output=False, has_logs=False),
        _make_state_entry(has_code=True, has_output=False, has_logs=False, dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_respects_version_priority() -> None:
    """_build_processing_order returns entries for higher-priority versions first."""
    config = {
        "pipelines": {
            "test": {
                "version_priority": ["v2.0", "v1.0"],
                "params_priority": ["default"],
            }
        }
    }
    entries = [
        _make_state_entry(version="v1.0", dandiset_id="000001"),
        _make_state_entry(version="v2.0", dandiset_id="000001"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=config)
    assert len(result) == 2
    assert result[0]["version"] == "v2.0"
    assert result[1]["version"] == "v1.0"


@pytest.mark.ai_generated
def test_build_processing_order_respects_params_priority() -> None:
    """_build_processing_order iterates params in params_priority order for each dandiset."""
    config = {
        "pipelines": {
            "test": {
                "version_priority": ["v1.0"],
                "params_priority": ["fast", "slow"],
            }
        }
    }
    entries = [
        _make_state_entry(params="slow", dandiset_id="000001"),
        _make_state_entry(params="fast", dandiset_id="000001"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=config)
    assert len(result) == 2
    assert result[0]["params"] == "fast"
    assert result[1]["params"] == "slow"


@pytest.mark.ai_generated
def test_build_processing_order_sorts_dandisets_by_created_at() -> None:
    """_build_processing_order processes dandiset instances in earliest-created-first order."""
    entries = [
        _make_state_entry(dandiset_id="000002", created_at="2024-01-02T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000001", created_at="2024-01-01T00:00:00+00:00"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 2
    assert result[0]["dandiset_id"] == "000001"
    assert result[1]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_zipper_all_params_per_dandiset_before_next() -> None:
    """All params for a dandiset instance appear consecutively before the next dandiset."""
    config = {
        "pipelines": {
            "test": {
                "version_priority": ["v1.0"],
                "params_priority": ["p1", "p2"],
            }
        }
    }
    # Two dandisets, two params each
    entries = [
        _make_state_entry(dandiset_id="000001", params="p1", created_at="2024-01-01T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000001", params="p2", created_at="2024-01-01T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000002", params="p1", created_at="2024-01-02T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000002", params="p2", created_at="2024-01-02T00:00:00+00:00"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=config)
    assert len(result) == 4
    ids = [(e["dandiset_id"], e["params"]) for e in result]
    # Expect: 000001/p1, 000001/p2, 000002/p1, 000002/p2
    assert ids == [("000001", "p1"), ("000001", "p2"), ("000002", "p1"), ("000002", "p2")]


@pytest.mark.ai_generated
def test_build_processing_order_ignores_unknown_pipeline() -> None:
    """_build_processing_order ignores state entries whose pipeline is not in queue_config."""
    entries = [
        _make_state_entry(pipeline="unknown", dandiset_id="000001"),
        _make_state_entry(pipeline="test", dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


# ---------------------------------------------------------------------------
# Tests for _determine_running
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_determine_running_true_when_aind_job_present() -> None:
    """_determine_running returns True when an AIND job appears in squeue output."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(stdout="JOBNAME\nAIND_ephys_job\nother_job\n", stderr="")
        assert _determine_running() is True


@pytest.mark.ai_generated
def test_determine_running_false_when_no_aind_jobs() -> None:
    """_determine_running returns False when no AIND jobs are in squeue output."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(stdout="JOBNAME\nsome_other_job\n", stderr="")
        assert _determine_running() is False


# ---------------------------------------------------------------------------
# Tests for _submit_next
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_no_waiting_file(tmp_path: pathlib.Path) -> None:
    """_submit_next returns False when waiting.jsonl is absent from the queue directory."""
    queue_dir = _make_queue_dir(tmp_path)

    result = _submit_next(cwd=queue_dir, dandiset_directory=tmp_path)

    assert result is False


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_no_pending_entries(tmp_path: pathlib.Path) -> None:
    """_submit_next returns False when waiting.jsonl is empty."""
    queue_dir = _make_queue_dir(tmp_path)

    # Empty waiting.jsonl → nothing to submit
    (queue_dir / "waiting.jsonl").write_text("")

    result = _submit_next(cwd=queue_dir, dandiset_directory=tmp_path)

    assert result is False


@pytest.mark.ai_generated
def test_submit_next_submits_first_entry_in_order(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first pending entry from waiting.jsonl."""
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
    _write_jsonl(queue_dir / "waiting.jsonl", [entry])

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

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert result is True
    submitted_command = mock_run.call_args.args[0]
    assert "submit" in submitted_command
    assert "--script" in submitted_command


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_script_missing(tmp_path: pathlib.Path) -> None:
    """_submit_next returns False when the submit.sh for the first entry does not exist."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(dandiset_id="000001", pipeline="test", version="v1.0", params="default")
    _write_jsonl(queue_dir / "waiting.jsonl", [entry])
    # Deliberately do NOT create the attempt directory / submit.sh

    with mock.patch("subprocess.run"):
        result = _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert result is False


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
    _write_jsonl(queue_dir / "waiting.jsonl", [entry])

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

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert result is True


@pytest.mark.ai_generated
def test_submit_next_pops_submitted_entry_from_waiting_jsonl(tmp_path: pathlib.Path) -> None:
    """_submit_next removes the submitted entry from waiting.jsonl."""
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
    _write_jsonl(queue_dir / "waiting.jsonl", [entry1, entry2])

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

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    remaining = _read_jsonl(queue_dir / "waiting.jsonl")
    assert len(remaining) == 1
    assert remaining[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_submit_next_appends_submitted_entry_to_submitted_jsonl(tmp_path: pathlib.Path) -> None:
    """_submit_next appends the submitted entry to submitted.jsonl."""
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
    _write_jsonl(queue_dir / "waiting.jsonl", [entry])
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

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    submitted_entries = _read_jsonl(queue_dir / "submitted.jsonl")
    assert len(submitted_entries) == 1
    assert submitted_entries[0]["dandiset_id"] == "000001"


# ---------------------------------------------------------------------------
# Tests for process_queue (top-level orchestration)
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_process_queue_raises_for_wrong_dir_name(tmp_path: pathlib.Path) -> None:
    """process_queue raises ValueError when the directory is not named 'queue'."""
    wrong_dir = tmp_path / "not_queue"
    wrong_dir.mkdir()

    with pytest.raises(ValueError, match="must be 'queue'"):
        process_queue(cwd=wrong_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_process_queue_raises_when_waiting_file_missing(tmp_path: pathlib.Path) -> None:
    """process_queue raises FileNotFoundError when waiting.jsonl is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="waiting.jsonl"):
        process_queue(cwd=queue_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue calls _submit_next when no AIND jobs are running."""
    queue_dir = _make_queue_dir(tmp_path)
    (queue_dir / "waiting.jsonl").write_text("")
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=False),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    mock_submit.assert_called_once_with(cwd=queue_dir, dandiset_directory=dandiset_dir)


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue does NOT call _submit_next when AIND jobs are currently running."""
    queue_dir = _make_queue_dir(tmp_path)
    (queue_dir / "waiting.jsonl").write_text("")
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=True),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    mock_submit.assert_not_called()


@pytest.mark.ai_generated
def test_process_queue_passes_dandiset_directory_to_submit_next(tmp_path: pathlib.Path) -> None:
    """process_queue forwards dandiset_directory to _submit_next."""
    queue_dir = _make_queue_dir(tmp_path)
    (queue_dir / "waiting.jsonl").write_text("")
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    with (
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=False),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    mock_submit.assert_called_once_with(cwd=queue_dir, dandiset_directory=dandiset_dir)


# ---------------------------------------------------------------------------
# Tests for order_queue
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_order_queue_raises_for_wrong_dir_name(tmp_path: pathlib.Path) -> None:
    """order_queue raises ValueError when the directory is not named 'queue'."""
    wrong_dir = tmp_path / "not_queue"
    wrong_dir.mkdir()

    with pytest.raises(ValueError, match="must be 'queue'"):
        order_queue(cwd=wrong_dir)


@pytest.mark.ai_generated
def test_order_queue_raises_when_state_file_missing(tmp_path: pathlib.Path) -> None:
    """order_queue raises FileNotFoundError when state.jsonl is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))

    with pytest.raises(FileNotFoundError, match="state.jsonl"):
        order_queue(cwd=queue_dir)


@pytest.mark.ai_generated
def test_order_queue_writes_waiting_jsonl_from_state_entries(tmp_path: pathlib.Path) -> None:
    """order_queue writes waiting.jsonl containing only pending (prepared-but-unrun) entries."""
    queue_dir = _make_queue_dir(tmp_path)

    entries = [
        _make_state_entry(dandiset_id="000001", has_code=True, has_output=False, has_logs=False),
        # Already has output – should NOT appear in waiting.jsonl
        _make_state_entry(dandiset_id="000002", has_code=True, has_output=True, has_logs=False),
    ]
    _write_jsonl(queue_dir / "state.jsonl", entries)

    order_queue(cwd=queue_dir)

    waiting_file = queue_dir / "waiting.jsonl"
    assert waiting_file.exists()
    waiting_entries = _read_jsonl(waiting_file)
    assert len(waiting_entries) == 1
    assert waiting_entries[0]["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_order_queue_limit_truncates_waiting_jsonl(tmp_path: pathlib.Path) -> None:
    """order_queue respects the limit parameter and truncates waiting.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)

    entries = [
        _make_state_entry(dandiset_id=f"00000{i}", has_code=True, has_output=False, has_logs=False) for i in range(1, 6)
    ]
    _write_jsonl(queue_dir / "state.jsonl", entries)

    order_queue(cwd=queue_dir, limit=2)

    waiting_entries = _read_jsonl(queue_dir / "waiting.jsonl")
    assert len(waiting_entries) == 2


# ---------------------------------------------------------------------------
# Tests for _count_dandiset_failures
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
    ``version-{version}/params-{params_id}_config-{config_id}_attempt-{attempt_number}/``

    is created.  *with_code*, *with_output*, and *with_logs* control whether the
    ``code/``, ``derivatives/``, and ``logs/`` subdirectories are created.  When
    *with_logs* is True a sentinel file is written inside ``logs/`` so it is
    treated as non-empty by :func:`_count_dandiset_failures`.
    """
    attempt_dir = (
        base
        / "derivatives"
        / f"dandiset-{dandiset_id}"
        / "sub-test"
        / "pipeline-aind+ephys"
        / f"version-{version}"
        / f"params-{params_id}_config-{config_id}_attempt-{attempt_number}"
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


@pytest.mark.ai_generated
def test_count_dandiset_failures_returns_zero_when_no_derivatives_dir(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures returns 0 when the derivatives directory does not exist."""
    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 0


@pytest.mark.ai_generated
def test_count_dandiset_failures_counts_failed_attempts(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures counts directories with code/ + non-empty logs/ but no output/ as failures."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_logs=True)
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 2, with_logs=True)
    # Successful run – must NOT be counted
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 3, with_output=True)
    # Pending entry (no logs) – must NOT be counted
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 4)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 2


@pytest.mark.ai_generated
def test_count_dandiset_failures_ignores_different_version(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures ignores attempt directories under a different version."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_logs=True)
    # Different version – should NOT be counted
    _make_attempt_dir(tmp_path, "000001", "v2.0", "abc1234", "def5678", 1, with_logs=True)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 1


@pytest.mark.ai_generated
def test_count_dandiset_failures_counts_all_params_config_combos(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures counts failures across all params/config combinations for the given version."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_logs=True)
    # Different params_id – also counted (no filtering by params/config)
    _make_attempt_dir(tmp_path, "000001", "v1.0", "zzz9999", "def5678", 1, with_logs=True)
    # Different config_id – also counted
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "yyy8888", 1, with_logs=True)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 3


@pytest.mark.ai_generated
def test_count_dandiset_failures_counts_across_all_dandisets(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures counts failures across all source dandisets for the given version."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_logs=True)
    # Different dandiset_id – also counted (no per-dandiset filtering)
    _make_attempt_dir(tmp_path, "000002", "v1.0", "abc1234", "def5678", 1, with_logs=True)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 2


# ---------------------------------------------------------------------------
# Tests for _submit_next with max_fail_per_dandiset
# ---------------------------------------------------------------------------

#: params_id and config_id used when building fake attempt directories.
_FAKE_PARAMS_ID = "abc1234"
_FAKE_CONFIG_ID = "def5678"


@pytest.mark.ai_generated
def test_submit_next_skips_all_entries_when_total_failures_exceed_max(tmp_path: pathlib.Path) -> None:
    """_submit_next skips all entries when the total failure count reaches max_fail_per_dandiset."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"

    # Create 2 failure attempt dirs for the dandiset (== max_fail_per_dandiset=2)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1, with_logs=True)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 2, with_logs=True)

    # waiting.jsonl: two pending entries – all should be skipped due to failure cap
    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [
            _make_state_entry(dandiset_id="000001", version="v1.0"),
            _make_state_entry(dandiset_id="000002", version="v1.0"),
        ],
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert result is False


@pytest.mark.ai_generated
def test_submit_next_allows_entry_when_dandiset_failures_below_max(tmp_path: pathlib.Path) -> None:
    """_submit_next does NOT skip an entry when total failure count is below the limit."""
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
    _write_jsonl(queue_dir / "waiting.jsonl", [entry])

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

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert result is True


# ---------------------------------------------------------------------------
# Tests for prepare_queue
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_prepare_queue_raises_for_wrong_dir_name(tmp_path: pathlib.Path) -> None:
    """prepare_queue raises ValueError when the directory is not named 'queue'."""
    wrong_dir = tmp_path / "not_queue"
    wrong_dir.mkdir()

    with pytest.raises(ValueError, match="must be 'queue'"):
        prepare_queue(cwd=wrong_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_prepare_queue_calls_prepare_for_each_qualifying_asset(tmp_path: pathlib.Path) -> None:
    """prepare_queue calls prepare_aind_ephys_job for every qualifying content ID."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    qualifying_ids = ["asset-bbb", "asset-ccc"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._process_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert mock_prepare.call_count == 2
    prepared_ids = {call.kwargs["content_id"] for call in mock_prepare.call_args_list}
    assert prepared_ids == {"asset-bbb", "asset-ccc"}


@pytest.mark.ai_generated
def test_prepare_queue_respects_max_attempts(tmp_path: pathlib.Path) -> None:
    """prepare_queue skips content IDs that have already reached max_attempts_per_asset."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    # asset-aaa has an override limit of 1; pre-populate submitted with one entry for it.
    _write_jsonl(
        queue_dir / "submitted.jsonl",
        [{"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"}],
    )

    qualifying_ids = ["asset-aaa", "asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._process_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    prepared_ids = {call.kwargs["content_id"] for call in mock_prepare.call_args_list}
    assert "asset-aaa" not in prepared_ids
    assert "asset-bbb" in prepared_ids


@pytest.mark.ai_generated
def test_prepare_queue_skips_when_failures_reach_max(tmp_path: pathlib.Path) -> None:
    """prepare_queue skips all assets when the failure count reaches max_fail_per_dandiset."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    # Create 2 failed attempt dirs (== max_fail_per_dandiset from _EXAMPLE_QUEUE_CONFIG).
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1, with_logs=True)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 2, with_logs=True)

    qualifying_ids = ["asset-bbb", "asset-ccc"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._process_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    mock_prepare.assert_not_called()


@pytest.mark.ai_generated
def test_prepare_queue_strips_commit_suffix_from_version(tmp_path: pathlib.Path) -> None:
    """prepare_queue passes the version without its trailing commit-hash to prepare_aind_ephys_job."""
    queue_dir = _make_queue_dir(tmp_path)
    queue_config = json.loads((queue_dir / "queue_config.json").read_text())
    queue_config["pipelines"]["test"]["version_priority"] = ["v1.1.0+abcdef0"]
    (queue_dir / "queue_config.json").write_text(json.dumps(queue_config))
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    qualifying_ids = ["asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._process_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["pipeline_version"] == "v1.1.0"


@pytest.mark.ai_generated
def test_prepare_queue_passes_optional_args_through(tmp_path: pathlib.Path) -> None:
    """prepare_queue forwards pipeline_directory and config_file_path to prepare_aind_ephys_job."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    fake_pipeline_dir = tmp_path / "pipeline"
    fake_pipeline_dir.mkdir()
    fake_config = tmp_path / "test.config"
    fake_config.write_text("config")

    qualifying_ids = ["asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._process_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(
            cwd=queue_dir,
            dandiset_directory=dandiset_dir,
            pipeline_directory=fake_pipeline_dir,
            config_file_path=fake_config,
        )

    assert mock_prepare.call_count == 1
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["pipeline_directory"] == fake_pipeline_dir
    assert call_kwargs["config_file_path"] == fake_config
