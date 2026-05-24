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
