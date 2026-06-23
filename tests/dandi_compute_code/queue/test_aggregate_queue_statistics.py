import json
import pathlib
from collections.abc import Callable
from datetime import datetime

import pytest

from dandi_compute_code.queue import QueueState, aggregate_queue_statistics

_TIMELINE_TWO_STEPS = """<script>
window.data = {
  "processes": [
    {"label": "step_one (step-one)", "times": [{"label": "1m 5s / 1 GB"}]},
    {"label": "step_two (step-two)", "times": [{"label": "30s / 500 MB"}, {"label": "2m / 1 GB"}]}
  ]
};
</script>"""

_TIMELINE_ONE_STEP = """<script>
window.data = {
  "processes": [
    {"label": "step_one (step-one)", "times": [{"label": "1s / 1 GB"}]}
  ]
};
</script>"""


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_writes_queue_stats_json(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """aggregate_queue_statistics writes queue_stats.json with byte and timeline aggregates."""
    state_file = install_state_file(queue_directory=queue_directory, name="aggregate_two_attempts.jsonl")
    dandiset_dir = tmp_path / "dandiset"

    successful_entry = QueueState.from_jsonl(state_file).entries[0]
    attempt_dir = create_attempt_directory(base_dir=dandiset_dir, entry=successful_entry, with_logs=True)
    (attempt_dir / "logs" / "timeline.html").write_text(_TIMELINE_TWO_STEPS)

    stats = aggregate_queue_statistics(queue_directory=queue_directory, dandiset_directory=dandiset_dir)

    queue_stats_file = queue_directory / "queue_stats.json"
    assert queue_stats_file.exists()
    assert stats["state_entry_count"] == 2
    assert stats["successful_asset_bytes_total"] == 120
    assert stats["timeline_files_processed"] == 1
    assert datetime.fromisoformat(stats["generated_at"])
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(65.0)
    assert stats["job_step_wall_time_seconds"]["step_two"] == pytest.approx(150.0)
    assert json.loads(queue_stats_file.read_text()) == stats


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_skips_invalid_timeline_html(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    create_attempt_directory: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """aggregate_queue_statistics ignores timeline files with malformed embedded JSON."""
    state_file = install_state_file(queue_directory=queue_directory, name="aggregate_single_successful.jsonl")
    dandiset_dir = tmp_path / "dandiset"

    entry = QueueState.from_jsonl(state_file).entries[0]
    attempt_dir = create_attempt_directory(base_dir=dandiset_dir, entry=entry, with_logs=True)
    (attempt_dir / "logs" / "timeline.html").write_text("<script>window.data = {invalid json};</script>")

    stats = aggregate_queue_statistics(queue_directory=queue_directory, dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 0
    assert stats["job_step_wall_time_seconds"] == {}


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_found_timeline_via_fallback_attempt_resolution(
    queue_directory: pathlib.Path,
    install_state_file: Callable[..., pathlib.Path],
    tmp_path: pathlib.Path,
) -> None:
    """aggregate_queue_statistics finds timeline files when state dandi_path differs from on-disk attempt path."""
    install_state_file(queue_directory=queue_directory, name="aggregate_fallback_sourcedata.jsonl")
    dandiset_dir = tmp_path / "dandiset"

    # The on-disk attempt lives under sub-mouse01 while the state dandi_path is "sourcedata",
    # so the attempt directory must be located via fallback resolution rather than the recorded path.
    attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-mouse01"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+a66c8df_codebase-v0.3.0_params-4af6a25_config-0d4bf36_attempt-1"
    )
    logs_dir = attempt_dir / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "timeline.html").write_text(_TIMELINE_ONE_STEP)

    stats = aggregate_queue_statistics(queue_directory=queue_directory, dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 1
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(1.0)
