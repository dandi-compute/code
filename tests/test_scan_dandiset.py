"""
Unit tests for scan_dandiset_directory and writing state and waiting queue files.
(dandi_compute_code.dandiset).
"""

import json
import pathlib

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import scan_dandiset_directory, write_state_and_waiting_jsonl

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
    if with_output:
        (attempt_dir / "derivatives").mkdir()
    if with_logs:
        logs_dir = attempt_dir / "logs"
        logs_dir.mkdir()
        if not logs_empty:
            (logs_dir / "nextflow.log").write_text("log content")
    return attempt_dir


# ---------------------------------------------------------------------------
# Tests for scan_dandiset_directory
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_scan_empty_when_no_derivatives(tmp_path: pathlib.Path) -> None:
    """Returns an empty list when there is no derivatives/ directory."""
    result = scan_dandiset_directory(dandiset_directory=tmp_path)
    assert result == []


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
    assert r["subject"] == "mouse01"
    assert r["session"] is None
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
    assert records[0]["session"] == "20230101"
    assert records[0]["subject"] == "mouse01"


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

    records = scan_dandiset_directory(dandiset_directory=tmp_path)

    assert len(records) == 1
    assert records[0]["version"] == "v1.0"
    assert records[0]["params"] == "abc1234"


# ---------------------------------------------------------------------------
# Tests for write_state_and_waiting_jsonl
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_write_state_and_waiting_jsonl_creates_valid_files(tmp_path: pathlib.Path) -> None:
    """write_state_and_waiting_jsonl writes state.jsonl and waiting.jsonl."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
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
    write_state_and_waiting_jsonl(dandiset_directory=tmp_path, queue_directory=queue_dir)
    state_file = queue_dir / "state.jsonl"
    waiting_file = queue_dir / "waiting.jsonl"
    assert state_file.exists()
    assert waiting_file.exists()
    lines = [line for line in state_file.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_write_state_and_waiting_jsonl_empty_when_no_attempts(tmp_path: pathlib.Path) -> None:
    """write_state_and_waiting_jsonl writes empty state and waiting files with no attempts."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))
    write_state_and_waiting_jsonl(dandiset_directory=tmp_path, queue_directory=queue_dir)
    state_file = queue_dir / "state.jsonl"
    waiting_file = queue_dir / "waiting.jsonl"
    assert state_file.exists()
    assert waiting_file.exists()
    assert state_file.read_text() == ""
    assert waiting_file.read_text() == ""


@pytest.mark.ai_generated
def test_write_state_and_waiting_jsonl_includes_only_pending_in_waiting(tmp_path: pathlib.Path) -> None:
    """waiting.jsonl contains only pending entries from the newly written state."""
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

    write_state_and_waiting_jsonl(dandiset_directory=tmp_path, queue_directory=queue_dir)

    waiting_file = queue_dir / "waiting.jsonl"
    assert waiting_file.exists()
    waiting_lines = [line for line in waiting_file.read_text().splitlines() if line.strip()]
    assert len(waiting_lines) == 1
    waiting_record = json.loads(waiting_lines[0])
    assert waiting_record["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_write_state_and_waiting_jsonl_prunes_last_submitted_entries_with_output_or_logs(
    tmp_path: pathlib.Path,
) -> None:
    """write_state_and_waiting_jsonl removes entries from last_submitted.jsonl when logs/output are present."""
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
    _make_attempt_dir(
        tmp_path,
        "000003",
        "mouse03",
        "test-pipeline",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
        with_logs=True,
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
    (queue_dir / "last_submitted.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "dandiset_id": "000001",
                        "subject": "mouse01",
                        "session": None,
                        "pipeline": "test-pipeline",
                        "version": "v1.0",
                        "params": "abc1234",
                        "config": "def5678",
                        "attempt": 1,
                    }
                ),
                json.dumps(
                    {
                        "dandiset_id": "000002",
                        "subject": "mouse02",
                        "session": None,
                        "pipeline": "test-pipeline",
                        "version": "v1.0",
                        "params": "abc1234",
                        "config": "def5678",
                        "attempt": 1,
                    }
                ),
                json.dumps(
                    {
                        "dandiset_id": "000003",
                        "subject": "mouse03",
                        "session": None,
                        "pipeline": "test-pipeline",
                        "version": "v1.0",
                        "params": "abc1234",
                        "config": "def5678",
                        "attempt": 1,
                    }
                ),
            ]
        )
        + "\n"
    )

    write_state_and_waiting_jsonl(dandiset_directory=tmp_path, queue_directory=queue_dir)

    remaining = [
        json.loads(line) for line in (queue_dir / "last_submitted.jsonl").read_text().splitlines() if line.strip()
    ]
    assert len(remaining) == 1
    assert remaining[0]["dandiset_id"] == "000001"


# ---------------------------------------------------------------------------
# Tests for the CLI command
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_cli_dandiset_scan_to_stdout(tmp_path: pathlib.Path) -> None:
    """dandicompute queue scan prints JSONL to stdout when no --output given."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["queue", "scan", "--directory", str(tmp_path)])
    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_cli_dandiset_scan_to_file(tmp_path: pathlib.Path) -> None:
    """dandicompute queue scan writes JSONL to a file when --output is given."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    out = tmp_path / "out.jsonl"
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "scan", "--directory", str(tmp_path), "--output", str(out)],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()
    lines = [line for line in out.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_cli_dandiset_scan_state_output_requires_queue_config(tmp_path: pathlib.Path) -> None:
    """CLI returns a clear error when writing state.jsonl outside a queue directory."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    out = tmp_path / "state.jsonl"
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "scan", "--directory", str(tmp_path), "--output", str(out)],
    )
    assert result.exit_code != 0
    assert "ensure queue_config.json exists in the parent directory" in result.output
