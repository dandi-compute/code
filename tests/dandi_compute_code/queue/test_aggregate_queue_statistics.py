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
def test_aggregate_queue_statistics_writes_queue_stats_json(tmp_path: pathlib.Path) -> None:
    """aggregate_queue_statistics writes queue_stats.json with byte and timeline aggregates."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry_with_output = _make_state_entry(has_output=True, has_logs=True)
    entry_with_output["asset_size_bytes"] = 120
    entry_without_output = _make_state_entry(dandiset_id="000002", subject="mouse02", has_output=False, has_logs=True)
    entry_without_output["asset_size_bytes"] = 999
    _write_jsonl(queue_dir / "state.jsonl", [entry_with_output, entry_without_output])

    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    logs_dir = attempt_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "timeline.html").write_text("""<script>
window.data = {
  "processes": [
    {"label": "step_one (step-one)", "times": [{"label": "1m 5s / 1 GB"}]},
    {"label": "step_two (step-two)", "times": [{"label": "30s / 500 MB"}, {"label": "2m / 1 GB"}]}
  ]
};
</script>""")

    stats = aggregate_queue_statistics(queue_directory=queue_dir, dandiset_directory=dandiset_dir)

    queue_stats_file = queue_dir / "queue_stats.json"
    assert queue_stats_file.exists()
    assert stats["state_entry_count"] == 2
    assert stats["successful_asset_bytes_total"] == 120
    assert stats["timeline_files_processed"] == 1
    assert datetime.fromisoformat(stats["generated_at"])
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(65.0)
    assert stats["job_step_wall_time_seconds"]["step_two"] == pytest.approx(150.0)
    assert json.loads(queue_stats_file.read_text()) == stats


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_skips_invalid_timeline_html(tmp_path: pathlib.Path) -> None:
    """aggregate_queue_statistics ignores timeline files with malformed embedded JSON."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(has_output=True, has_logs=True)
    entry["asset_size_bytes"] = 120
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    logs_dir = attempt_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "timeline.html").write_text("<script>window.data = {invalid json};</script>")

    stats = aggregate_queue_statistics(queue_directory=queue_dir, dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 0
    assert stats["job_step_wall_time_seconds"] == {}


@pytest.mark.ai_generated
def test_aggregate_queue_statistics_found_timeline_via_fallback_attempt_resolution(tmp_path: pathlib.Path) -> None:
    """aggregate_queue_statistics finds timeline files when state dandi_path differs from on-disk attempt path."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(
        dandiset_id="001849",
        dandi_path="sourcedata",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+a66c8df",
        params="4af6a25",
        config="0d4bf36",
        has_output=True,
        has_logs=True,
    )
    entry["asset_size_bytes"] = 120
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+a66c8df_codebase-v0.3.0_params-4af6a25_config-0d4bf36_attempt-1"
    )
    logs_dir = attempt_dir / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "timeline.html").write_text("""<script>
window.data = {
  "processes": [
    {"label": "step_one (step-one)", "times": [{"label": "1s / 1 GB"}]}
  ]
};
</script>""")

    stats = aggregate_queue_statistics(queue_directory=queue_dir, dandiset_directory=dandiset_dir)

    assert stats["timeline_files_processed"] == 1
    assert stats["job_step_wall_time_seconds"]["step_one"] == pytest.approx(1.0)
