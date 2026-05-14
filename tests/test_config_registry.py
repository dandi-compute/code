import hashlib
import json
import pathlib

import pytest


@pytest.mark.ai_generated
def test_registered_configs_md5_matches_files() -> None:
    """registered_configs.json entries point to existing files with matching MD5 hashes."""
    pipeline_dir = pathlib.Path(__file__).parent.parent / "src" / "dandi_compute_code" / "aind_ephys_pipeline"
    registry_path = pipeline_dir / "registries" / "registered_configs.json"
    registry = json.loads(registry_path.read_text())

    assert "default" in registry
    for config_entry in registry.values():
        config_path = pipeline_dir / "configs" / config_entry["path"]
        assert config_path.exists()
        assert hashlib.md5(config_path.read_bytes()).hexdigest() == config_entry["md5"]  # noqa: S324
