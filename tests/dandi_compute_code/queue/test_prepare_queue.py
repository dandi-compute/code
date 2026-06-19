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
def test_prepare_queue_raises_when_queue_config_fails_linkml_validation(tmp_path: pathlib.Path) -> None:
    """prepare_queue raises when queue_config violates LinkML constraints."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    invalid_queue_config = {
        "pipelines": {
            # Violates schema minimum_value: 0 constraint.
            "test": {"version_priority": ["v1.0"], "params_priority": ["default"], "max_fail_per_dandiset": -1}
        }
    }
    (queue_dir / "queue_config.json").write_text(json.dumps(invalid_queue_config))

    with pytest.raises(ValueError, match="LinkML validation failed"):
        prepare_queue(queue_directory=queue_dir, content_ids=[])


@pytest.mark.ai_generated
def test_prepare_queue_calls_prepare_for_each_qualifying_asset(tmp_path: pathlib.Path) -> None:
    """prepare_queue calls prepare_aind_ephys_job for every qualifying content ID."""
    queue_dir = _make_queue_dir(tmp_path)

    qualifying_ids = ["asset-bbb", "asset-ccc"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir)

    assert mock_prepare.call_count == 2
    prepared_ids = {call.kwargs["content_id"] for call in mock_prepare.call_args_list}
    assert prepared_ids == {"asset-bbb", "asset-ccc"}


@pytest.mark.ai_generated
def test_prepare_queue_skips_when_failures_reach_max(tmp_path: pathlib.Path) -> None:
    """prepare_queue skips assets for dandisets whose failure count reaches max_fail_per_dandiset."""
    queue_dir = _make_queue_dir(tmp_path)
    # Write 2 failure entries to state.jsonl (== max_fail_per_dandiset from _EXAMPLE_QUEUE_CONFIG).
    failure_entries = [
        {
            **_make_state_entry(
                dandiset_id="000001",
                pipeline="test",
                version="v1.0",
                attempt=1,
                has_code=True,
                has_logs=True,
                has_output=False,
            ),
            "content_id": "asset-aaa",
        },
        {
            **_make_state_entry(
                dandiset_id="000001",
                pipeline="test",
                version="v1.0",
                attempt=2,
                has_code=True,
                has_logs=True,
                has_output=False,
            ),
            "content_id": "asset-aaa",
        },
        {
            **_make_state_entry(
                dandiset_id="000002",
                pipeline="test",
                version="v1.0",
                attempt=1,
                has_code=True,
                has_logs=False,
                has_output=False,
            ),
            "content_id": "asset-bbb",
        },
    ]
    _write_jsonl(queue_dir / "state.jsonl", failure_entries)

    qualifying_ids = ["asset-aaa", "asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir)

    assert mock_prepare.call_count == 1
    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-bbb"]


@pytest.mark.ai_generated
def test_prepare_queue_passes_optional_args_through(tmp_path: pathlib.Path) -> None:
    """prepare_queue forwards optional args to prepare_aind_ephys_job."""
    queue_dir = _make_queue_dir(tmp_path)

    fake_pipeline_dir = tmp_path / "pipeline"
    fake_pipeline_dir.mkdir()

    qualifying_ids = ["asset-bbb"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(
            queue_directory=queue_dir,
            pipeline_directory=fake_pipeline_dir,
            config_key="mit+engaging+revision-1",
        )

    assert mock_prepare.call_count == 1
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["pipeline_directory"] == fake_pipeline_dir
    assert call_kwargs["config_key"] == "mit+engaging+revision-1"


@pytest.mark.ai_generated
def test_prepare_queue_limit_stops_after_n_assets(tmp_path: pathlib.Path) -> None:
    """prepare_queue stops after preparing exactly limit assets when limit is set."""
    queue_dir = _make_queue_dir(tmp_path)

    qualifying_ids = ["asset-aaa", "asset-bbb", "asset-ccc"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir, limit=2)

    assert mock_prepare.call_count == 2


@pytest.mark.ai_generated
def test_prepare_queue_limit_samples_uniformly_over_dandisets(tmp_path: pathlib.Path) -> None:
    """prepare_queue interleaves qualifying assets so --limit is not biased by asset-rich Dandisets."""
    queue_dir = _make_queue_dir(tmp_path)
    qualifying_ids = ["asset-a1", "asset-a2", "asset-b1"]
    content_id_mapping = {
        "asset-a1": {"000001": "sub-a1/sub-a1_ecephys.nwb"},
        "asset-a2": {"000001": "sub-a2/sub-a2_ecephys.nwb"},
        "asset-b1": {"000002": "sub-b1/sub-b1_ecephys.nwb"},
    }

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling._load_content_id_to_usage_dandiset_path",
            return_value=content_id_mapping,
        ),
        mock.patch(
            "dandi_compute_code.queue._order_content_ids_for_uniform_dandiset_sampling.random.shuffle",
            side_effect=lambda items: None,
        ) as mock_shuffle,
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        prepare_queue(queue_directory=queue_dir, limit=2)

    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-a1", "asset-b1"]
    assert mock_shuffle.call_count == 3
    assert {tuple(group) for group in mock_shuffle.call_args_list[0].args[0]} == {
        ("asset-a1", "asset-a2"),
        ("asset-b1",),
    }
    assert set(mock_shuffle.call_args_list[1].args[0]) == {"asset-a1", "asset-a2"}
    assert set(mock_shuffle.call_args_list[2].args[0]) == {"asset-b1"}


@pytest.mark.ai_generated
def test_prepare_queue_uses_explicit_content_ids_when_provided(tmp_path: pathlib.Path) -> None:
    """prepare_queue uses provided content_ids directly and skips the network fetch."""
    queue_dir = _make_queue_dir(tmp_path)
    explicit_ids = ["explicit-asset-001"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        prepare_queue(queue_directory=queue_dir, content_ids=explicit_ids)

    mock_urlopen.assert_not_called()
    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["content_id"] == "explicit-asset-001"
