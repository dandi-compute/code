"""
Unit tests for scan_dandiset_directory and write_scan_jsonl
(dandi_compute_code.dandiset).
"""

import json
import pathlib

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import scan_dandiset_directory, write_scan_jsonl

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
        f"version-{version}",
        f"params-{params}_config-{config}_attempt-{attempt}",
    ]
    attempt_dir = pathlib.Path(*parts)
    attempt_dir.mkdir(parents=True)
    if with_code:
        (attempt_dir / "code").mkdir()
    if with_output:
        (attempt_dir / "output").mkdir()
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
    """Single attempt with code/ but no output/ is returned with correct fields."""
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


@pytest.mark.ai_generated
def test_scan_single_successful_attempt(tmp_path: pathlib.Path) -> None:
    """Single attempt with both code/ and output/ is returned with has_output=True."""
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


# ---------------------------------------------------------------------------
# Tests for write_scan_jsonl
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_write_scan_jsonl_creates_valid_file(tmp_path: pathlib.Path) -> None:
    """write_scan_jsonl produces a readable JSONL file with correct content."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    out = tmp_path / "scan.jsonl"
    write_scan_jsonl(dandiset_directory=tmp_path, output_file=out)
    assert out.exists()
    lines = [line for line in out.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_write_scan_jsonl_empty_when_no_attempts(tmp_path: pathlib.Path) -> None:
    """write_scan_jsonl writes an empty file when there are no attempts."""
    out = tmp_path / "scan.jsonl"
    write_scan_jsonl(dandiset_directory=tmp_path, output_file=out)
    assert out.exists()
    assert out.read_text() == ""


# ---------------------------------------------------------------------------
# Tests for the CLI command
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_cli_dandiset_scan_to_stdout(tmp_path: pathlib.Path) -> None:
    """dandicompute dandiset scan prints JSONL to stdout when no --output given."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["dandiset", "scan", "--directory", str(tmp_path)])
    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_cli_dandiset_scan_to_file(tmp_path: pathlib.Path) -> None:
    """dandicompute dandiset scan writes JSONL to a file when --output is given."""
    _make_attempt_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1)
    out = tmp_path / "out.jsonl"
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["dandiset", "scan", "--directory", str(tmp_path), "--output", str(out)],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()
    lines = [line for line in out.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dandiset_id"] == "000001"
