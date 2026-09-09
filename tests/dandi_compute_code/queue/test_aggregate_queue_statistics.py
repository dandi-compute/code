import json
import pathlib
from datetime import datetime
from unittest import mock

import pytest
from testing_utilities import create_attempt_directory

from dandi_compute_code.queue import QueueState

_EXAMPLE_TIMELINE_REPORTS = pathlib.Path(__file__).parent / "example_timeline_reports"
_JOB_CAPSULES_DANDISET_ID = "001697"


@pytest.fixture
def timeline_two_steps() -> str:
    """Nextflow timeline report with two process steps."""
    return (_EXAMPLE_TIMELINE_REPORTS / "two_steps.html").read_text()


@pytest.fixture
def timeline_one_step() -> str:
    """Nextflow timeline report with a single process step."""
    return (_EXAMPLE_TIMELINE_REPORTS / "one_step.html").read_text()


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_publishes_queue_stats_json(
    example_queue_state: QueueState, tmp_path: pathlib.Path, timeline_two_steps: str
) -> None:
    """aggregate_statistics publishes queue_stats.json with byte and timeline aggregates."""
    dandiset_dir = tmp_path / "dandiset"

    # sub-successful is the only entry with both output and a known source-asset size.
    attempt_dir = create_attempt_directory(
        base_dir=dandiset_dir, entry=example_queue_state.entry_for(dandi_path="sub-successful"), with_logs=True
    )
    (attempt_dir / "logs" / "timeline.html").write_text(timeline_two_steps)

    with mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file") as mock_write_file:
        stats = example_queue_state.aggregate_statistics(dandiset_directory=dandiset_dir)

    assert stats["state_entry_count"] == len(example_queue_state)
    assert stats["successful_asset_bytes_total"] == 120
    assert stats["timeline_files_processed"] == 1
    assert datetime.fromisoformat(stats["generated_at"])
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(65.0)
    assert stats["job_step_wall_time_seconds"]["step_two"] == pytest.approx(150.0)

    mock_write_file.assert_called_once()
    call_kwargs = mock_write_file.call_args.kwargs
    assert call_kwargs["dandiset_id"] == _JOB_CAPSULES_DANDISET_ID
    assert call_kwargs["relative_path"] == "derivatives/queue_stats.json"
    assert json.loads(call_kwargs["content"]) == stats


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_skips_invalid_timeline_html(
    example_queue_state: QueueState, tmp_path: pathlib.Path
) -> None:
    """aggregate_statistics ignores timeline files with malformed embedded JSON."""
    dandiset_dir = tmp_path / "dandiset"

    attempt_dir = create_attempt_directory(
        base_dir=dandiset_dir, entry=example_queue_state.entry_for(dandi_path="sub-successful"), with_logs=True
    )
    (attempt_dir / "logs" / "timeline.html").write_text("<script>window.data = {invalid json};</script>")

    with mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file"):
        stats = example_queue_state.aggregate_statistics(dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 0
    assert stats["job_step_wall_time_seconds"] == {}


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_found_timeline_via_fallback_attempt_resolution(
    example_queue_state: QueueState, tmp_path: pathlib.Path, timeline_one_step: str
) -> None:
    """aggregate_statistics finds timeline files when state dandi_path differs from on-disk attempt path."""
    dandiset_dir = tmp_path / "dandiset"

    # The "sourcedata" entry's on-disk attempt lives under sub-mouse01, so its timeline
    # must be located via fallback resolution rather than the recorded dandi_path.
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
    (logs_dir / "timeline.html").write_text(timeline_one_step)

    with mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file"):
        stats = example_queue_state.aggregate_statistics(dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 1
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(1.0)


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_forwards_dandiset_id_and_relative_path(
    example_queue_state: QueueState, tmp_path: pathlib.Path
) -> None:
    """aggregate_statistics forwards dandiset_id/relative_path/processing_directory/test to write_dandiset_file."""
    dandiset_dir = tmp_path / "dandiset"
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()

    with mock.patch("dandi_compute_code.queue._queue_state.write_dandiset_file") as mock_write_file:
        example_queue_state.aggregate_statistics(
            dandiset_directory=dandiset_dir,
            dandiset_id="000123",
            relative_path="derivatives/custom_stats.json",
            processing_directory=processing_dir,
            test=True,
        )

    mock_write_file.assert_called_once_with(
        dandiset_id="000123",
        relative_path="derivatives/custom_stats.json",
        content=mock.ANY,
        processing_directory=processing_dir,
        test=True,
    )
