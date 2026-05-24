# ruff: noqa: F821
from . import _process_queue_test_cases as _support

globals().update(
    {
        name: value
        for name, value in vars(_support).items()
        if not name.startswith("__") and not name.startswith("test_")
    }
)

@pytest.mark.ai_generated
def test_order_queue_raises_when_queue_config_missing(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state raises FileNotFoundError when queue_config.json is absent."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="queue_config.json"):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_order_queue_writes_waiting_jsonl_from_state_entries(tmp_path: pathlib.Path) -> None:
    """refresh_queue_state writes scanned entries to state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)

    entries = [
        _make_state_entry(dandiset_id="000001", has_code=True, has_output=False, has_logs=False),
        # Already has output (kept in state.jsonl for authoritative tracking)
        _make_state_entry(dandiset_id="000002", has_code=True, has_output=True, has_logs=False),
    ]
    with mock.patch("dandi_compute_code.queue._refresh_queue.scan_dandiset_directory", return_value=entries):
        refresh_queue_state(queue_directory=queue_dir, dandiset_directory=tmp_path)

    state_file = queue_dir / "state.jsonl"
    assert state_file.exists()
    state_entries = _read_jsonl(state_file)
    assert len(state_entries) == 2
    assert {entry["dandiset_id"] for entry in state_entries} == {"000001", "000002"}


@pytest.mark.ai_generated
def test_order_queue_returns_ordered_pending_entries_only() -> None:
    """order_queue returns pending entries without writing files."""
    state_entries = [
        _make_state_entry(dandiset_id="000001", has_code=True, has_output=False, has_logs=False),
        _make_state_entry(dandiset_id="000002", has_code=True, has_output=True, has_logs=False),
    ]
    queue_config = {"pipelines": {"test": {"version_priority": ["v1.0"], "params_priority": ["default"]}}}

    ordered = order_queue(state_entries=state_entries, queue_config=queue_config)

    assert len(ordered) == 1
    assert ordered[0]["dandiset_id"] == "000001"


@pytest.mark.ai_generated
def test_order_queue_respects_limit_parameter() -> None:
    """order_queue truncates ordered entries when limit is provided."""
    state_entries = [
        _make_state_entry(dandiset_id=f"00000{i}", has_code=True, has_output=False, has_logs=False) for i in range(1, 6)
    ]
    queue_config = {"pipelines": {"test": {"version_priority": ["v1.0"], "params_priority": ["default"]}}}

    ordered = order_queue(state_entries=state_entries, queue_config=queue_config, limit=2)

    assert len(ordered) == 2
