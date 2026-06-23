import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID
from dandi_compute_code.queue import QueueState

# write_archive_state derives archive_state.jsonl from DANDI assets.jsonld metadata,
# fetched over the network. That loader is the one external boundary mocked here.


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """A queue directory containing a minimal valid queue_config.json."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps({"pipelines": {"test": {"version_priority": ["v1.0"], "params_priority": ["default"]}}})
    )
    return queue_dir


def _read_jsonl(file_path: pathlib.Path) -> list[dict]:
    return [json.loads(line) for line in file_path.read_text().splitlines() if line.strip()]


@pytest.mark.ai_generated
def test_write_archive_state_writes_adjacent_file_from_archive_dandiset(tmp_path: pathlib.Path) -> None:
    """write_archive_state writes archive_state.jsonl from the archive Dandiset metadata."""
    queue_dir = _make_queue_dir(tmp_path)
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"
    attempt_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-default_config-def5678_attempt-1/code/submit.sh"
    )
    load_metadata = mock.Mock(
        return_value=AssetsJsonldMetadata(
            content_id_to_asset={},
            path_to_asset_metadata={
                attempt_path: AssetMetadata(
                    path=attempt_path,
                    date_modified="2024-01-01T00:00:00+00:00",
                    content_size=1,
                    content_id="attempt-id",
                )
            },
        )
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            load_metadata,
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_state._load_upstream_assets_jsonld_metadata",
            return_value=AssetsJsonldMetadata(
                content_id_to_asset={},
                path_to_asset_metadata={
                    source_path: AssetMetadata(
                        path=source_path,
                        date_modified="2024-01-01T00:00:00+00:00",
                        content_size=1234,
                        content_id="source-id",
                    )
                },
            ),
        ),
    ):
        QueueState.write_archive_state(queue_directory=queue_dir)

    # The archive metadata is read from the failed runs archive Dandiset, not the job capsules one.
    load_metadata.assert_called_once_with(dandiset_id=_FAILED_RUNS_ARCHIVE_DANDISET_ID)

    archive_state_file = queue_dir / "archive_state.jsonl"
    assert archive_state_file.exists()
    # Lives adjacent to (and does not clobber) the main queue state file.
    assert not (queue_dir / "state.jsonl").exists()

    archive_entries = _read_jsonl(archive_state_file)
    assert len(archive_entries) == 1
    assert archive_entries[0]["dandi_path"] == source_path
    assert archive_entries[0]["content_id"] == "source-id"
    assert archive_entries[0]["has_code"] is True


@pytest.mark.ai_generated
def test_write_archive_state_writes_empty_file_when_no_attempts(tmp_path: pathlib.Path) -> None:
    """write_archive_state writes an empty archive_state.jsonl when no attempts are present."""
    queue_dir = _make_queue_dir(tmp_path)
    with mock.patch(
        "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
        return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
    ):
        QueueState.write_archive_state(queue_directory=queue_dir)

    archive_state_file = queue_dir / "archive_state.jsonl"
    assert archive_state_file.exists()
    assert archive_state_file.read_text() == ""
