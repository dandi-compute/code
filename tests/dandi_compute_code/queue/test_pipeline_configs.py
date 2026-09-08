import json
import pathlib

import pytest

from dandi_compute_code.queue import QueueState
from dandi_compute_code.queue._globals import _PACKAGED_PIPELINE_CONFIGS_PATH


@pytest.mark.ai_generated
def test_packaged_pipeline_configs_file_exists_and_validates() -> None:
    """The pipeline_configs.json packaged with this repo exists and validates."""
    assert _PACKAGED_PIPELINE_CONFIGS_PATH.exists()
    loaded = QueueState.load_queue_config()
    assert "pipelines" in loaded
    assert loaded["pipelines"]


@pytest.mark.ai_generated
def test_load_queue_config_falls_back_to_packaged_default_when_directory_has_no_config(
    tmp_path: pathlib.Path,
) -> None:
    """load_queue_config uses the packaged pipeline_configs.json when --queue has neither file."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    loaded = QueueState.load_queue_config(queue_directory=queue_dir)

    assert loaded == json.loads(_PACKAGED_PIPELINE_CONFIGS_PATH.read_text())


@pytest.mark.ai_generated
def test_load_queue_config_prefers_pipeline_configs_json_in_queue_directory(tmp_path: pathlib.Path) -> None:
    """load_queue_config prefers a queue-directory pipeline_configs.json over the packaged default."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    override_config = {"pipelines": {"custom": {"version_priority": ["v9.9"], "params_priority": ["default"]}}}
    (queue_dir / "pipeline_configs.json").write_text(json.dumps(override_config))

    loaded = QueueState.load_queue_config(queue_directory=queue_dir)

    assert loaded == override_config


@pytest.mark.ai_generated
def test_load_queue_config_prefers_pipeline_configs_json_over_legacy_queue_config_json(
    tmp_path: pathlib.Path,
) -> None:
    """load_queue_config prefers pipeline_configs.json over a legacy queue_config.json in the same directory."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    new_config = {"pipelines": {"new": {"version_priority": ["v1"], "params_priority": ["default"]}}}
    legacy_config = {"pipelines": {"legacy": {"version_priority": ["v0"], "params_priority": ["default"]}}}
    (queue_dir / "pipeline_configs.json").write_text(json.dumps(new_config))
    (queue_dir / "queue_config.json").write_text(json.dumps(legacy_config))

    loaded = QueueState.load_queue_config(queue_directory=queue_dir)

    assert loaded == new_config


@pytest.mark.ai_generated
def test_load_queue_config_still_supports_legacy_queue_config_json(tmp_path: pathlib.Path) -> None:
    """load_queue_config still reads a legacy queue_config.json when pipeline_configs.json is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    legacy_config = {"pipelines": {"legacy": {"version_priority": ["v0"], "params_priority": ["default"]}}}
    (queue_dir / "queue_config.json").write_text(json.dumps(legacy_config))

    loaded = QueueState.load_queue_config(queue_directory=queue_dir)

    assert loaded == legacy_config
