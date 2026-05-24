# ruff: noqa: F821
from . import _process_queue_test_cases as _support

globals().update(
    {
        name: value
        for name, value in vars(_support).items()
        if not name.startswith("__") and not name.startswith("test_")
    }
)

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
