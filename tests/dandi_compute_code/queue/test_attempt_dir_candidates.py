# ruff: noqa: F821
import importlib.util as _importlib_util
import pathlib as _pathlib

from dandi_compute_code.queue import JobEntry

_spec = _importlib_util.spec_from_file_location(
    "_process_queue_test_cases",
    _pathlib.Path(__file__).with_name("_process_queue_test_cases.py"),
)
assert _spec is not None
assert _spec.loader is not None
_support = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_support)

globals().update(
    {
        name: value
        for name, value in vars(_support).items()
        if not name.startswith("__") and not name.startswith("test_")
    }
)


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
    entry = JobEntry.from_dict(
        _make_state_entry(
            dandiset_id="000001",
            dandi_path=dandi_path,
            pipeline="test",
            version="v1.0",
            params="abc1234",
            config="def5678",
            attempt=2,
        )
    )

    flat_path, legacy_path = entry.attempt_dir_candidates(tmp_path)

    assert (
        flat_path == tmp_path / relative_prefix / "version-v1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-2"
    )
    assert legacy_path == tmp_path / relative_prefix / "version-v1.0/params-abc1234_config-def5678_attempt-2"


@pytest.mark.ai_generated
def test_attempt_dir_candidates_requires_non_empty_dandi_path(tmp_path: pathlib.Path) -> None:
    """JobEntry.attempt_dir_candidates rejects an empty dandi_path value."""
    entry = JobEntry.from_dict(
        {
            "dandiset_id": "000001",
            "dandi_path": "",
            "pipeline": "test",
            "version": "v1.0",
            "params": "abc1234",
            "config": "def5678",
            "codebase": "v0.3.0",
            "attempt": 2,
        }
    )

    with pytest.raises(ValueError, match=r"Entry has invalid dandi_path field \(empty\)"):
        entry.attempt_dir_candidates(tmp_path)


@pytest.mark.ai_generated
def test_attempt_dir_candidates_includes_codebase_in_flat_path(tmp_path: pathlib.Path) -> None:
    """attempt_dir_candidates includes the _codebase- segment in the flat path."""
    entry = JobEntry.from_dict(
        _make_state_entry(
            dandiset_id="000001",
            dandi_path="sub-mouse01",
            pipeline="test",
            version="v1.1.1",
            params="4af6a25",
            config="0d4bf36",
            codebase="v0.3.17",
            attempt=1,
        )
    )
    flat_path, legacy_path = entry.attempt_dir_candidates(tmp_path)

    expected_prefix = tmp_path / "derivatives" / "dandiset-000001" / "sub-mouse01" / "pipeline-test"
    assert flat_path == expected_prefix / "version-v1.1.1_codebase-v0.3.17_params-4af6a25_config-0d4bf36_attempt-1"
    assert legacy_path == expected_prefix / "version-v1.1.1" / "params-4af6a25_config-0d4bf36_attempt-1"
