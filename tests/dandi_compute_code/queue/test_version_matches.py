# ruff: noqa: F821
import importlib.util as _importlib_util
import pathlib as _pathlib

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
def test_version_matches_exact() -> None:
    """_version_matches returns True for exact match."""
    assert _version_matches("v1.1.1", "v1.1.1") is True


@pytest.mark.ai_generated
def test_version_matches_rejects_different_version() -> None:
    """_version_matches returns False when the version base is different."""
    assert _version_matches("v1.1.0", "v1.1.1") is False
