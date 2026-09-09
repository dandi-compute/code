import pathlib

import pytest

from dandi_compute_code.queue import JobEntry


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dandi_path", "relative_prefix"),
    [
        ("sub-mouse01", pathlib.Path("derivatives/dandiset-000001/sub-mouse01/pipeline-test")),
        ("sub-mouse01/ses-01", pathlib.Path("derivatives/dandiset-000001/sub-mouse01/ses-01/pipeline-test")),
        (
            "sourcedata/aind-sample.nwb",
            pathlib.Path("derivatives/dandiset-000001/sourcedata/aind-sample/pipeline-test"),
        ),
    ],
)
def test_attempt_dir_candidates_constructs_both_layouts(
    dandi_path: str,
    relative_prefix: pathlib.Path,
    tmp_path: pathlib.Path,
) -> None:
    """JobEntry.attempt_dir_candidates returns both flat and legacy attempt directory paths."""
    entry = {
        "dandiset_id": "000001",
        "dandi_path": dandi_path,
        "pipeline": "test",
        "version": "v1.0",
        "params": "abc1234",
        "config": "def5678",
        "attempt": 2,
        "codebase": "v0.3.0",
    }

    flat_path, legacy_path = JobEntry.from_dict(entry).attempt_dir_candidates(tmp_path)

    assert (
        flat_path == tmp_path / relative_prefix / "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-2"
    )
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
                "codebase": "v0.3.0",
            },
            # A JobEntry always carries dandi_path, so a missing key fails at construction.
            KeyError,
            r"dandi_path",
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
                "codebase": "v0.3.0",
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
    """JobEntry.attempt_dir_candidates requires a valid dandi_path value."""
    with pytest.raises(expected_exception, match=expected_message):
        JobEntry.from_dict(entry).attempt_dir_candidates(tmp_path)


@pytest.mark.ai_generated
def test_attempt_dir_candidates_includes_codebase_in_flat_path(tmp_path: pathlib.Path) -> None:
    """JobEntry.attempt_dir_candidates includes the _codebase- segment in the flat path."""
    entry = {
        "dandiset_id": "000001",
        "dandi_path": "sub-mouse01",
        "pipeline": "test",
        "version": "v1.1.1",
        "params": "4af6a25",
        "config": "0d4bf36",
        "attempt": 1,
        "codebase": "v0.3.17",
    }
    flat_path, legacy_path = JobEntry.from_dict(entry).attempt_dir_candidates(tmp_path)

    expected_prefix = tmp_path / "derivatives" / "dandiset-000001" / "sub-mouse01" / "pipeline-test"
    assert flat_path == expected_prefix / "version-v1.1.1_codebase-v0.3.17_params-4af6a25_config-0d4bf36_attempt-1"
    assert legacy_path == expected_prefix / "version-v1.1.1" / "params-4af6a25_config-0d4bf36_attempt-1"
