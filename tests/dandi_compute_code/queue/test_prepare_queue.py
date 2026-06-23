import gzip
import json
import pathlib
from collections.abc import Callable
from unittest import mock

import pytest

from dandi_compute_code.queue import prepare_queue

# prepare_queue reaches two external boundaries that cannot run in CI: the
# qualifying-content-ids download (urllib) and the per-asset job preparation
# (prepare_aind_ephys_job). Both are mocked here; everything else runs for real.


def _mock_urlopen_response(payload: list) -> mock.MagicMock:
    mock_response = mock.MagicMock()
    jsonl = "\n".join(json.dumps(item) for item in payload)
    mock_response.read.return_value = gzip.compress(jsonl.encode())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False
    return mock_response


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
def test_prepare_queue_calls_prepare_for_each_qualifying_asset(queue_directory: pathlib.Path) -> None:
    """prepare_queue calls prepare_aind_ephys_job for every qualifying content ID."""
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
        prepare_queue(queue_directory=queue_directory)

    assert mock_prepare.call_count == 2
    prepared_ids = {call.kwargs["content_id"] for call in mock_prepare.call_args_list}
    assert prepared_ids == {"asset-bbb", "asset-ccc"}


@pytest.mark.ai_generated
def test_prepare_queue_skips_when_failures_reach_max(
    queue_directory: pathlib.Path,
    copy_state_file: Callable[..., pathlib.Path],
) -> None:
    """prepare_queue skips assets for dandisets whose failure count reaches max_fail_per_dandiset."""
    # The example queue records repeated failures for dandiset 000001 (reaching
    # max_fail_per_dandiset) mapped to asset-aaa, and a fresh asset in 000002 mapped to asset-bbb.
    copy_state_file(queue_directory=queue_directory)
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
        prepare_queue(queue_directory=queue_directory)

    assert mock_prepare.call_count == 1
    prepared_ids = [call.kwargs["content_id"] for call in mock_prepare.call_args_list]
    assert prepared_ids == ["asset-bbb"]


@pytest.mark.ai_generated
def test_prepare_queue_passes_optional_args_through(queue_directory: pathlib.Path, tmp_path: pathlib.Path) -> None:
    """prepare_queue forwards optional args to prepare_aind_ephys_job."""
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
            queue_directory=queue_directory,
            pipeline_directory=fake_pipeline_dir,
            config_key="mit+engaging+revision-1",
        )

    assert mock_prepare.call_count == 1
    call_kwargs = mock_prepare.call_args.kwargs
    assert call_kwargs["pipeline_directory"] == fake_pipeline_dir
    assert call_kwargs["config_key"] == "mit+engaging+revision-1"


@pytest.mark.ai_generated
def test_prepare_queue_limit_stops_after_n_assets(queue_directory: pathlib.Path) -> None:
    """prepare_queue stops after preparing exactly limit assets when limit is set."""
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
        prepare_queue(queue_directory=queue_directory, limit=2)

    assert mock_prepare.call_count == 2


@pytest.mark.ai_generated
def test_prepare_queue_limit_samples_uniformly_over_dandisets(queue_directory: pathlib.Path) -> None:
    """prepare_queue interleaves qualifying assets so --limit is not biased by asset-rich Dandisets."""
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
        prepare_queue(queue_directory=queue_directory, limit=2)

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
def test_prepare_queue_uses_explicit_content_ids_when_provided(queue_directory: pathlib.Path) -> None:
    """prepare_queue uses provided content_ids directly and skips the network fetch."""
    explicit_ids = ["explicit-asset-001"]

    with (
        mock.patch("urllib.request.urlopen") as mock_urlopen,
        mock.patch("dandi_compute_code.queue._prepare_queue.prepare_aind_ephys_job") as mock_prepare,
    ):
        prepare_queue(queue_directory=queue_directory, content_ids=explicit_ids)

    mock_urlopen.assert_not_called()
    assert mock_prepare.call_count == 1
    assert mock_prepare.call_args.kwargs["content_id"] == "explicit-asset-001"
