import json
import pathlib

import pytest

from dandi_compute_code.queue import QueueState

_ISSUE_EXAMPLE_QUEUE_CONFIG = {
    "pipelines": {
        "aind+ephys": {
            "version_priority": ["v1.1.1"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 1,
            "asset_overrides": {"048d1ee9-83b7-491f-8f02-1ca615b1d455": None},
            "max_fail_per_dandiset": 10,
        }
    }
}


@pytest.mark.ai_generated
def test_load_queue_config_validates_issue_example_schema(tmp_path: pathlib.Path) -> None:
    """Issue-provided queue config validates against the LinkML schema."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps(_ISSUE_EXAMPLE_QUEUE_CONFIG))

    loaded = QueueState.load_queue_config(queue_directory=queue_dir)

    assert loaded == _ISSUE_EXAMPLE_QUEUE_CONFIG
