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
def test_load_queue_config_validates_issue_example_schema(tmp_path: pathlib.Path) -> None:
    """Issue-provided queue config validates against the LinkML schema."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps(_ISSUE_EXAMPLE_QUEUE_CONFIG))

    loaded = _load_queue_config(queue_directory=queue_dir)

    assert loaded == _ISSUE_EXAMPLE_QUEUE_CONFIG
