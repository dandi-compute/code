import hashlib
import json
import pathlib

import pytest

_DEFAULT_PARAMS_PATH = "name-custom+preprocessing+default_version-1+2+2.json"


@pytest.mark.ai_generated
def test_registered_configs_md5_matches_files() -> None:
    """registered_configs.json entries point to existing files with matching MD5 hashes."""
    repository_root = pathlib.Path(__file__).resolve().parents[3]
    pipeline_dir = repository_root / "src" / "dandi_compute_code" / "aind_ephys_pipeline"
    registry_path = pipeline_dir / "registries" / "registered_configs.json"
    registry = json.loads(registry_path.read_text())

    assert "default" in registry
    for config_entry in registry.values():
        config_path = pipeline_dir / "configs" / config_entry["path"]
        assert config_path.exists()
        assert hashlib.md5(config_path.read_bytes()).hexdigest() == config_entry["md5"]  # noqa: S324


@pytest.mark.ai_generated
def test_registered_default_params_matches_custom_preprocessing_file() -> None:
    """Verify that registered_params.json default entry points to the custom preprocessing file."""
    repository_root = pathlib.Path(__file__).resolve().parents[3]
    pipeline_dir = repository_root / "src" / "dandi_compute_code" / "aind_ephys_pipeline"
    registry_path = pipeline_dir / "registries" / "registered_params.json"
    registry = json.loads(registry_path.read_text())

    default_entry = registry["default"]
    assert default_entry["path"] == _DEFAULT_PARAMS_PATH
    params_path = pipeline_dir / "params" / default_entry["path"]
    assert params_path.exists()
    assert hashlib.md5(params_path.read_bytes()).hexdigest() == default_entry["md5"]  # noqa: S324
