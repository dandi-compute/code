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
def test_build_processing_order_empty_state() -> None:
    """_build_processing_order returns an empty list when state_entries is empty."""
    result = _build_processing_order(state_entries=[], queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert result == []


@pytest.mark.ai_generated
def test_build_processing_order_filters_out_entries_with_output() -> None:
    """_build_processing_order excludes entries that already have output."""
    entries = [
        _make_state_entry(has_code=True, has_output=True, has_logs=False),
        _make_state_entry(has_code=True, has_output=False, has_logs=False, dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_filters_out_entries_with_logs() -> None:
    """_build_processing_order excludes entries that already have logs (running or failed)."""
    entries = [
        _make_state_entry(has_code=True, has_output=False, has_logs=True),
        _make_state_entry(has_code=True, has_output=False, has_logs=False, dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_filters_out_entries_without_code() -> None:
    """_build_processing_order excludes entries that have no code directory yet."""
    entries = [
        _make_state_entry(has_code=False, has_output=False, has_logs=False),
        _make_state_entry(has_code=True, has_output=False, has_logs=False, dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_respects_version_priority() -> None:
    """_build_processing_order returns entries for higher-priority versions first."""
    config = {
        "pipelines": {
            "test": {
                "version_priority": ["v2.0", "v1.0"],
                "params_priority": ["default"],
            }
        }
    }
    entries = [
        _make_state_entry(version="v1.0", dandiset_id="000001"),
        _make_state_entry(version="v2.0", dandiset_id="000001"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=config)
    assert len(result) == 2
    assert result[0]["version"] == "v2.0"
    assert result[1]["version"] == "v1.0"


@pytest.mark.ai_generated
def test_build_processing_order_respects_params_priority() -> None:
    """_build_processing_order iterates params in params_priority order for each dandiset."""
    config = {
        "pipelines": {
            "test": {
                "version_priority": ["v1.0"],
                "params_priority": ["fast", "slow"],
            }
        }
    }
    entries = [
        _make_state_entry(params="slow", dandiset_id="000001"),
        _make_state_entry(params="fast", dandiset_id="000001"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=config)
    assert len(result) == 2
    assert result[0]["params"] == "fast"
    assert result[1]["params"] == "slow"


@pytest.mark.ai_generated
def test_build_processing_order_sorts_dandisets_by_created_at() -> None:
    """_build_processing_order processes dandiset instances in earliest-created-first order."""
    entries = [
        _make_state_entry(dandiset_id="000002", created_at="2024-01-02T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000001", created_at="2024-01-01T00:00:00+00:00"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 2
    assert result[0]["dandiset_id"] == "000001"
    assert result[1]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_zipper_all_params_per_dandiset_before_next() -> None:
    """All params for a dandiset instance appear consecutively before the next dandiset."""
    config = {
        "pipelines": {
            "test": {
                "version_priority": ["v1.0"],
                "params_priority": ["p1", "p2"],
            }
        }
    }
    # Two dandisets, two params each
    entries = [
        _make_state_entry(dandiset_id="000001", params="p1", created_at="2024-01-01T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000001", params="p2", created_at="2024-01-01T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000002", params="p1", created_at="2024-01-02T00:00:00+00:00"),
        _make_state_entry(dandiset_id="000002", params="p2", created_at="2024-01-02T00:00:00+00:00"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=config)
    assert len(result) == 4
    ids = [(e["dandiset_id"], e["params"]) for e in result]
    # Expect: 000001/p1, 000001/p2, 000002/p1, 000002/p2
    assert ids == [("000001", "p1"), ("000001", "p2"), ("000002", "p1"), ("000002", "p2")]


@pytest.mark.ai_generated
def test_build_processing_order_ignores_unknown_pipeline() -> None:
    """_build_processing_order ignores state entries whose pipeline is not in queue_config."""
    entries = [
        _make_state_entry(pipeline="unknown", dandiset_id="000001"),
        _make_state_entry(pipeline="test", dandiset_id="000002"),
    ]
    result = _build_processing_order(state_entries=entries, queue_config=_EXAMPLE_QUEUE_CONFIG)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_build_processing_order_resolves_aind_ephys_params_key_to_id() -> None:
    """_build_processing_order matches state entries whose params is a hash ID when queue_config uses the key name."""
    config = {
        "pipelines": {
            "aind+ephys": {
                "version_priority": ["v1.1.1+b268fd2"],
                "params_priority": ["default"],
            }
        }
    }
    entry = _make_state_entry(
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2",
        params=_get_default_params_id(),
        dandiset_id="000233",
    )
    result = _build_processing_order(state_entries=[entry], queue_config=config)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000233"


@pytest.mark.ai_generated
def test_build_processing_order_matches_new_style_version_with_code_hash() -> None:
    """_build_processing_order matches state entries with a code-repo commit hash appended to the version."""
    # Simulates state entries produced after the change that appends the first 7 chars
    # of the dandi-compute/code repo commit hash to the version directory name.
    config = {
        "pipelines": {
            "aind+ephys": {
                "version_priority": ["v1.1.1+b268fd2"],
                "params_priority": ["default"],
            }
        }
    }
    entry = _make_state_entry(
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+abcdef1",
        params=_get_default_params_id(),
        dandiset_id="000233",
    )
    result = _build_processing_order(state_entries=[entry], queue_config=config)
    assert len(result) == 1
    assert result[0]["dandiset_id"] == "000233"
