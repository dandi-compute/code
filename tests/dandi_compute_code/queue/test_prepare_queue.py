import gzip
import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue import QueueState

# prepare_queue reaches three external boundaries that cannot run in CI: the packaged
# pipeline configuration (mocked here so tests are not coupled to its real contents), the
# qualifying-content-ids download (urllib), and the per-asset job preparation
# (prepare_aind_ephys_job). All three are mocked here; everything else runs for real.

_TEST_QUEUE_CONFIG = {
    "pipelines": {
        "test": {
            "version_priority": ["v1.0"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 2,
            "asset_overrides": {"asset-aaa": 1},
            "max_fail_per_dandiset": 2,
        }
    }
}


def _mock_urlopen_response(qualifying_content_ids: list[str]) -> mock.MagicMock:
    """Build a mock response matching the real ``{content_id: qualifies}``-per-line JSONL format."""
    mock_response = mock.MagicMock()
    jsonl = "\n".join(json.dumps({content_id: True}) for content_id in qualifying_content_ids)
    mock_response.read.return_value = gzip.compress(jsonl.encode())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False
    return mock_response


@pytest.mark.ai_generated
def test_prepare_queue_calls_prepare_for_each_qualifying_asset() -> None:
    """prepare_queue calls prepare_aind_ephys_job for every qualifying content ID."""
    qualifying_ids = ["asset-bbb", "asset-ccc"]

    with (
        mock.patch("dandi_compute_code.queue._queue_state._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._queue_state.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        QueueState.prepare()

    assert mock_prepare.call_count == 2
    prepared_ids = {call.kwargs["content_id"] for call in mock_prepare.call_args_list}
    assert prepared_ids == {"asset-bbb", "asset-ccc"}


@pytest.mark.ai_generated
def test_prepare_queue_skips_when_failures_reach_max(example_queue_state: QueueState) -> None:
    """prepare_queue skips assets for dandisets whose failure count reaches max_fail_per_dandiset."""
    # The example queue records repeated failures for dandiset 000001 (reaching
    # max_fail_per_dandiset) mapped to asset-aaa, and a fresh asset in 000002 mapped to asset-bbb.
    qualifying_ids = ["asset-aaa", "asset-bbb"]

    with (
        mock.patch("dandi_compute_code.queue._queue_state._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("dandi_compute_code.queue._queue_state.QueueState.from_dandi", return_value=example_queue_state),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._queue_state.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        QueueState.prepare()

    assert mock_prepare.call_count == 1
    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-bbb"]


@pytest.mark.ai_generated
def test_prepare_queue_passes_optional_args_through(tmp_path: pathlib.Path) -> None:
    """prepare_queue forwards optional args to prepare_aind_ephys_job."""
    fake_pipeline_dir = tmp_path / "pipeline"
    fake_pipeline_dir.mkdir()
    qualifying_ids = ["asset-bbb"]

    with (
        mock.patch("dandi_compute_code.queue._queue_state._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._queue_state.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        QueueState.prepare(
            pipeline_directory=fake_pipeline_dir,
            config_key="mit+engaging+revision-1",
        )

    assert mock_prepare.call_count == 1
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["pipeline_directory"] == fake_pipeline_dir
    assert call_kwargs["config_key"] == "mit+engaging+revision-1"


@pytest.mark.ai_generated
def test_prepare_queue_limit_stops_after_n_assets() -> None:
    """prepare_queue stops after preparing exactly limit assets when limit is set."""
    qualifying_ids = ["asset-aaa", "asset-bbb", "asset-ccc"]

    with (
        mock.patch("dandi_compute_code.queue._queue_state._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._queue_state.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        QueueState.prepare(limit=2)

    assert mock_prepare.call_count == 2


@pytest.mark.ai_generated
def test_prepare_queue_limit_samples_uniformly_over_dandisets() -> None:
    """prepare_queue interleaves qualifying assets so --limit is not biased by asset-rich Dandisets."""
    qualifying_ids = ["asset-a1", "asset-a2", "asset-b1"]
    content_id_mapping = {
        "asset-a1": {"000001": "sub-a1/sub-a1_ecephys.nwb"},
        "asset-a2": {"000001": "sub-a2/sub-a2_ecephys.nwb"},
        "asset-b1": {"000002": "sub-b1/sub-b1_ecephys.nwb"},
    }

    with (
        mock.patch("dandi_compute_code.queue._queue_state._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value=content_id_mapping,
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils.random.shuffle",
            side_effect=lambda items: None,
        ) as mock_shuffle,
        mock.patch("dandi_compute_code.queue._queue_state.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = _mock_urlopen_response(qualifying_ids)
        QueueState.prepare(limit=2)

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
def test_prepare_queue_excludes_non_qualifying_content_ids() -> None:
    """QueueState.prepare only prepares content IDs whose remote cache entry is True.

    Regression test for the real cache format: each JSONL line is a
    ``{content_id: qualifies}`` object covering every content ID that qualifies for the
    (looser) LFP cache, not just the ones that qualify for the AIND pipeline.
    """
    mock_response = mock.MagicMock()
    jsonl = "\n".join(
        json.dumps({content_id: qualifies}) for content_id, qualifies in [("asset-bbb", True), ("asset-ccc", False)]
    )
    mock_response.read.return_value = gzip.compress(jsonl.encode())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False

    with (
        mock.patch("dandi_compute_code.queue._queue_state._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_content_id_to_usage_dandiset_path",
            return_value={},
        ),
        mock.patch("dandi_compute_code.queue._queue_state.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_urlopen.return_value = mock_response
        QueueState.prepare()

    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["content_id"] == "asset-bbb"


@pytest.mark.ai_generated
def test_prepare_queue_uses_explicit_content_ids_when_provided() -> None:
    """prepare_queue uses provided content_ids directly and skips the network fetch."""
    explicit_ids = ["explicit-asset-001"]

    with (
        mock.patch("dandi_compute_code.queue._queue_state._load_queue_config", return_value=_TEST_QUEUE_CONFIG),
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._queue_state.prepare_aind_ephys_job") as mock_prepare,
    ):
        QueueState.prepare(content_ids=explicit_ids)

    mock_urlopen.assert_not_called()
    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["content_id"] == "explicit-asset-001"
