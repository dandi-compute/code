"""
Unit tests for delete_dandiset_version and the ``dandicompute delete version`` CLI command.
"""

import pathlib
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.dandiset import delete_dandiset_version, scan_version_directories

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_version_dir(
    base: pathlib.Path,
    dandiset_id: str,
    subject: str,
    pipeline: str,
    version: str,
    *,
    session: str | None = None,
) -> pathlib.Path:
    """Create a mock ``version-{version}`` directory under a fake dandiset clone."""
    parts: list[pathlib.Path | str] = [
        base,
        "derivatives",
        f"dandiset-{dandiset_id}",
        f"sub-{subject}",
    ]
    if session is not None:
        parts.append(f"ses-{session}")
    parts += [f"pipeline-{pipeline}", f"version-{version}"]
    version_dir = pathlib.Path(*parts)
    version_dir.mkdir(parents=True)
    # Add a file inside so rmtree has something to remove
    (version_dir / "params-abc_config-def_attempt-1").mkdir()
    return version_dir


# ---------------------------------------------------------------------------
# Tests for delete_dandiset_version
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_delete_returns_empty_when_no_derivatives(tmp_path: pathlib.Path) -> None:
    """Returns an empty list when there is no derivatives/ directory."""
    result = delete_dandiset_version(dandiset_directory=tmp_path, version="v1.0")
    assert result == []


@pytest.mark.ai_generated
def test_delete_returns_empty_when_version_not_found(tmp_path: pathlib.Path) -> None:
    """Returns an empty list when no directories match the requested version."""
    _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v2.0")
    with patch("subprocess.run") as mock_run:
        result = delete_dandiset_version(dandiset_directory=tmp_path, version="v1.0")
    assert result == []
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_delete_single_version_dir(tmp_path: pathlib.Path) -> None:
    """delete_dandiset_version calls subprocess.run and removes the local directory."""
    version_dir = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    assert version_dir.is_dir()

    with patch("subprocess.run") as mock_run:
        result = delete_dandiset_version(dandiset_directory=tmp_path, version="v1.0")

    assert result == [version_dir]
    mock_run.assert_called_once_with(
        ["dandi", "delete", str(version_dir)],
        input=b"y\n",
        check=True,
    )
    assert not version_dir.exists()


@pytest.mark.ai_generated
def test_delete_multiple_version_dirs(tmp_path: pathlib.Path) -> None:
    """All matching version directories across multiple subjects are deleted."""
    dir1 = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    dir2 = _make_version_dir(tmp_path, "000001", "mouse02", "aind+ephys", "v1.0")

    with patch("subprocess.run") as mock_run:
        result = delete_dandiset_version(dandiset_directory=tmp_path, version="v1.0")

    assert set(result) == {dir1, dir2}
    assert mock_run.call_count == 2
    assert not dir1.exists()
    assert not dir2.exists()


@pytest.mark.ai_generated
def test_delete_ignores_other_versions(tmp_path: pathlib.Path) -> None:
    """Only the requested version is deleted; other versions are left intact."""
    target = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    other = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v2.0")

    with patch("subprocess.run"):
        result = delete_dandiset_version(dandiset_directory=tmp_path, version="v1.0")

    assert result == [target]
    assert not target.exists()
    assert other.exists()


@pytest.mark.ai_generated
def test_delete_with_session_level(tmp_path: pathlib.Path) -> None:
    """Session-level directory structure is handled correctly."""
    version_dir = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0", session="20230101")
    with patch("subprocess.run") as mock_run:
        result = delete_dandiset_version(dandiset_directory=tmp_path, version="v1.0")

    assert result == [version_dir]
    mock_run.assert_called_once_with(
        ["dandi", "delete", str(version_dir)],
        input=b"y\n",
        check=True,
    )
    assert not version_dir.exists()


@pytest.mark.ai_generated
def test_delete_ignores_non_dandiset_dirs(tmp_path: pathlib.Path) -> None:
    """Directories not starting with 'dandiset-' are ignored."""
    target = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    # Create a spurious version-v1.0 dir outside a dandiset- directory
    spurious = tmp_path / "derivatives" / "other-stuff" / "sub-x" / "pipeline-y" / "version-v1.0"
    spurious.mkdir(parents=True)

    with patch("subprocess.run") as mock_run:
        result = delete_dandiset_version(dandiset_directory=tmp_path, version="v1.0")

    assert result == [target]
    assert mock_run.call_count == 1
    assert spurious.exists()


# ---------------------------------------------------------------------------
# Tests for the CLI command
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_cli_delete_version_aborts_on_no_confirmation(tmp_path: pathlib.Path) -> None:
    """dandicompute delete version aborts when the user does not confirm."""
    _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    runner = CliRunner()
    with patch("subprocess.run") as mock_run:
        result = runner.invoke(
            _dandicompute_group,
            ["delete", "version", "--directory", str(tmp_path), "--version", "v1.0"],
            input="n\n",
        )
    assert result.exit_code != 0
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_cli_delete_version_deletes_on_confirmation(tmp_path: pathlib.Path) -> None:
    """dandicompute delete version deletes the directory when the user confirms."""
    version_dir = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    runner = CliRunner()
    with patch("subprocess.run"):
        result = runner.invoke(
            _dandicompute_group,
            ["delete", "version", "--directory", str(tmp_path), "--version", "v1.0"],
            input="y\n",
        )
    assert result.exit_code == 0, result.output
    assert "Deleted 1 version directory" in result.output
    assert not version_dir.exists()


@pytest.mark.ai_generated
def test_cli_delete_version_reports_none_found(tmp_path: pathlib.Path) -> None:
    """dandicompute delete version reports when no directories match (no prompt shown)."""
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["delete", "version", "--directory", str(tmp_path), "--version", "v99.0"],
    )
    assert result.exit_code == 0, result.output
    assert "No 'version-v99.0' directories found" in result.output


@pytest.mark.ai_generated
def test_cli_delete_version_prompt_shows_count(tmp_path: pathlib.Path) -> None:
    """The confirmation prompt includes the count of directories to be deleted."""
    _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    _make_version_dir(tmp_path, "000001", "mouse02", "aind+ephys", "v1.0")
    runner = CliRunner()
    with patch("subprocess.run"):
        result = runner.invoke(
            _dandicompute_group,
            ["delete", "version", "--directory", str(tmp_path), "--version", "v1.0"],
            input="n\n",
        )
    assert "2" in result.output
    assert "version-v1.0" in result.output


@pytest.mark.ai_generated
def test_cli_delete_version_hash_suffixed_version(tmp_path: pathlib.Path) -> None:
    """Version strings with appended commit hashes are handled correctly."""
    version_str = "v1.0.0+fixes+20abeb6"
    version_dir = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", version_str)
    runner = CliRunner()
    with patch("subprocess.run"):
        result = runner.invoke(
            _dandicompute_group,
            ["delete", "version", "--directory", str(tmp_path), "--version", version_str],
            input="y\n",
        )
    assert result.exit_code == 0, result.output
    assert "Deleted 1 version directory" in result.output
    assert not version_dir.exists()


@pytest.mark.ai_generated
def test_cli_delete_version_plural_message(tmp_path: pathlib.Path) -> None:
    """Output message uses plural form when more than one directory is deleted."""
    _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    _make_version_dir(tmp_path, "000001", "mouse02", "aind+ephys", "v1.0")
    runner = CliRunner()
    with patch("subprocess.run"):
        result = runner.invoke(
            _dandicompute_group,
            ["delete", "version", "--directory", str(tmp_path), "--version", "v1.0"],
            input="y\n",
        )
    assert result.exit_code == 0, result.output
    assert "Deleted 2 version directories" in result.output


# ---------------------------------------------------------------------------
# Tests for scan_version_directories
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_scan_version_directories_returns_empty_when_no_derivatives(tmp_path: pathlib.Path) -> None:
    """Returns an empty list when there is no derivatives/ directory."""
    assert scan_version_directories(dandiset_directory=tmp_path, version="v1.0") == []


@pytest.mark.ai_generated
def test_scan_version_directories_finds_matching_dirs(tmp_path: pathlib.Path) -> None:
    """Returns all matching version dirs, ignoring other versions."""
    target = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0")
    _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v2.0")
    result = scan_version_directories(dandiset_directory=tmp_path, version="v1.0")
    assert result == [target]


@pytest.mark.ai_generated
def test_scan_version_directories_hash_suffixed_version(tmp_path: pathlib.Path) -> None:
    """Exact hash-suffixed version string is resolved correctly."""
    version_str = "v1.1.2+abcd123+def4567"
    target = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", version_str)
    result = scan_version_directories(dandiset_directory=tmp_path, version=version_str)
    assert result == [target]


@pytest.mark.ai_generated
def test_scan_version_directories_base_version_matches_suffixed(tmp_path: pathlib.Path) -> None:
    """Specifying the base version matches all hash-suffixed variants."""
    exact = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0.0")
    suffixed1 = _make_version_dir(tmp_path, "000001", "mouse02", "aind+ephys", "v1.0.0+fixes+20abeb6")
    suffixed2 = _make_version_dir(tmp_path, "000002", "mouse01", "aind+ephys", "v1.0.0+abcd123+def4567")
    # A different base version should NOT be matched
    _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0.1")
    result = scan_version_directories(dandiset_directory=tmp_path, version="v1.0.0")
    assert set(result) == {exact, suffixed1, suffixed2}


@pytest.mark.ai_generated
def test_cli_delete_version_base_matches_suffixed(tmp_path: pathlib.Path) -> None:
    """CLI: specifying the base version deletes exact and hash-suffixed variants."""
    exact = _make_version_dir(tmp_path, "000001", "mouse01", "aind+ephys", "v1.0.0")
    suffixed = _make_version_dir(tmp_path, "000001", "mouse02", "aind+ephys", "v1.0.0+fixes+20abeb6")
    runner = CliRunner()
    with patch("subprocess.run"):
        result = runner.invoke(
            _dandicompute_group,
            ["delete", "version", "--directory", str(tmp_path), "--version", "v1.0.0"],
            input="y\n",
        )
    assert result.exit_code == 0, result.output
    assert "Deleted 2 version directories" in result.output
    assert not exact.exists()
    assert not suffixed.exists()
