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
