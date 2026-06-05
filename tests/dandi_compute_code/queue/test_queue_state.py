import json
import pathlib
from unittest import mock

import pytest
import yaml

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import JobEntry, JobInfo, QueueState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entry_dict(
    *,
    dandiset_id: str = "000001",
    dandi_path: str = "sub-mouse01/sub-mouse01_ecephys.nwb",
    pipeline: str = "test",
    version: str = "v1.0",
    params: str = "default",
    config: str = "abc123",
    attempt: int = 1,
    codebase: str = "v0.3.0",
    content_id: str | None = "content-id-abc",
    asset_size_bytes: int | None = 1000,
    has_code: bool = False,
    has_been_submitted: bool = False,
    has_output: bool = False,
    has_logs: bool = False,
    created_at: str | None = None,
    job_completion_time: str | None = None,
    output_paths: dict | None = None,
) -> dict:
    return {
        "dandiset_id": dandiset_id,
        "dandi_path": dandi_path,
        "pipeline": pipeline,
        "version": version,
        "params": params,
        "config": config,
        "attempt": attempt,
        "codebase": codebase,
        "content_id": content_id,
        "asset_size_bytes": asset_size_bytes,
        "has_code": has_code,
        "has_been_submitted": has_been_submitted,
        "has_output": has_output,
        "has_logs": has_logs,
        "created_at": created_at,
        "job_completion_time": job_completion_time,
        "output_paths": output_paths if output_paths is not None else {},
    }


# ---------------------------------------------------------------------------
# JobEntry.from_dict
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_job_entry_from_dict_populates_all_fields() -> None:
    data = _make_entry_dict(
        dandiset_id="000001",
        dandi_path="sub-mouse01/sub-mouse01_ecephys.nwb",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
        codebase="v0.3.0",
        content_id="content-id-abc",
        asset_size_bytes=1000,
        has_code=True,
        has_been_submitted=True,
        has_output=False,
        has_logs=False,
        created_at="2024-01-01T00:00:00+00:00",
        job_completion_time=None,
        output_paths={},
    )
    entry = JobEntry.from_dict(data)
    assert entry.job == JobInfo(
        dandiset_id="000001",
        dandi_path="sub-mouse01/sub-mouse01_ecephys.nwb",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
        codebase="v0.3.0",
    )
    assert entry.content_id == "content-id-abc"
    assert entry.asset_size_bytes == 1000
    assert entry.has_code is True
    assert entry.has_been_submitted is True
    assert entry.has_output is False
    assert entry.has_logs is False
    assert entry.created_at == "2024-01-01T00:00:00+00:00"
    assert entry.job_completion_time is None
    assert entry.output_paths == {}


@pytest.mark.ai_generated
def test_job_entry_from_dict_defaults_optional_fields_to_falsy() -> None:
    data = _make_entry_dict()
    entry = JobEntry.from_dict(data)
    assert entry.has_code is False
    assert entry.has_been_submitted is False
    assert entry.has_output is False
    assert entry.has_logs is False
    assert entry.output_paths == {}


@pytest.mark.ai_generated
def test_job_entry_from_dict_handles_missing_codebase() -> None:
    data = _make_entry_dict()
    del data["codebase"]
    entry = JobEntry.from_dict(data)
    assert entry.job.codebase == ""


# ---------------------------------------------------------------------------
# JobEntry.to_dict round-trip
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_job_entry_to_dict_round_trips_all_fields() -> None:
    data = _make_entry_dict(
        has_code=True,
        has_been_submitted=True,
        has_output=True,
        has_logs=True,
        created_at="2024-06-01T00:00:00+00:00",
        job_completion_time="2024-06-01T01:00:00+00:00",
        output_paths={"derivatives/out.nwb": "blob-id-1"},
    )
    entry = JobEntry.from_dict(data)
    result = entry.to_dict()
    assert result == data


# ---------------------------------------------------------------------------
# JobEntry status properties
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "has_code, has_logs, has_output, expected_pending, expected_running, expected_successful, expected_failed",
    [
        (True, False, False, True, False, False, False),   # pending
        (True, True, False, False, True, False, True),     # running / failed
        (True, True, True, False, False, True, False),     # successful
        (False, False, False, False, False, False, False), # no state
        (True, False, True, False, False, True, False),    # has code and output but no logs
    ],
)
def test_job_entry_status_properties(
    has_code: bool,
    has_logs: bool,
    has_output: bool,
    expected_pending: bool,
    expected_running: bool,
    expected_successful: bool,
    expected_failed: bool,
) -> None:
    data = _make_entry_dict(has_code=has_code, has_logs=has_logs, has_output=has_output)
    entry = JobEntry.from_dict(data)
    assert entry.is_pending == expected_pending
    assert entry.is_running == expected_running
    assert entry.is_successful == expected_successful
    assert entry.is_failed == expected_failed


# ---------------------------------------------------------------------------
# QueueState.from_jsonl
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_queue_state_from_jsonl_loads_all_entries(tmp_path: pathlib.Path) -> None:
    entries = [_make_entry_dict(dandiset_id=f"00000{i}") for i in range(1, 4)]
    state_file = tmp_path / "state.jsonl"
    state_file.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

    queue_state = QueueState.from_jsonl(state_file)

    assert len(queue_state) == 3
    assert queue_state.entries[0].job.dandiset_id == "000001"
    assert queue_state.entries[2].job.dandiset_id == "000003"


@pytest.mark.ai_generated
def test_queue_state_from_jsonl_handles_empty_file(tmp_path: pathlib.Path) -> None:
    state_file = tmp_path / "state.jsonl"
    state_file.write_text("")

    queue_state = QueueState.from_jsonl(state_file)

    assert len(queue_state) == 0


@pytest.mark.ai_generated
def test_queue_state_from_jsonl_raises_when_file_missing(tmp_path: pathlib.Path) -> None:
    missing = tmp_path / "state.jsonl"

    with pytest.raises(FileNotFoundError):
        QueueState.from_jsonl(missing)


# ---------------------------------------------------------------------------
# QueueState.to_file round-trip
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_queue_state_to_file_writes_newline_delimited_json(tmp_path: pathlib.Path) -> None:
    entry_dicts = [_make_entry_dict(dandiset_id=f"00000{i}") for i in range(1, 3)]
    state = QueueState(entries=[JobEntry.from_dict(d) for d in entry_dicts])
    out_file = tmp_path / "state.jsonl"

    state.to_file(out_file)

    lines = [line for line in out_file.read_text().splitlines() if line.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0])["dandiset_id"] == "000001"
    assert json.loads(lines[1])["dandiset_id"] == "000002"


@pytest.mark.ai_generated
def test_queue_state_to_file_and_from_jsonl_round_trip(tmp_path: pathlib.Path) -> None:
    entry_dicts = [
        _make_entry_dict(dandiset_id="000001", has_code=True),
        _make_entry_dict(dandiset_id="000002", has_code=True, has_logs=True),
        _make_entry_dict(dandiset_id="000003", has_output=True),
    ]
    original = QueueState(entries=[JobEntry.from_dict(d) for d in entry_dicts])
    state_file = tmp_path / "state.jsonl"

    original.to_file(state_file)
    reloaded = QueueState.from_jsonl(state_file)

    assert len(reloaded) == 3
    assert [e.to_dict() for e in reloaded] == [e.to_dict() for e in original]


# ---------------------------------------------------------------------------
# QueueState filter properties
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_queue_state_filter_properties_return_correct_entries() -> None:
    entry_dicts = [
        _make_entry_dict(dandiset_id="pending", has_code=True, has_logs=False, has_output=False),
        _make_entry_dict(dandiset_id="running", has_code=True, has_logs=True, has_output=False),
        _make_entry_dict(dandiset_id="success", has_code=True, has_logs=True, has_output=True),
        _make_entry_dict(dandiset_id="nothing", has_code=False, has_logs=False, has_output=False),
    ]
    state = QueueState(entries=[JobEntry.from_dict(d) for d in entry_dicts])

    assert len(state.pending) == 1
    assert state.pending[0].job.dandiset_id == "pending"

    assert len(state.running) == 1
    assert state.running[0].job.dandiset_id == "running"

    assert len(state.successful) == 1
    assert state.successful[0].job.dandiset_id == "success"

    assert len(state.failed) == 1
    assert state.failed[0].job.dandiset_id == "running"


# ---------------------------------------------------------------------------
# QueueState.__iter__ and __len__
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_queue_state_iter_yields_all_entries() -> None:
    entry_dicts = [_make_entry_dict(dandiset_id=f"00000{i}") for i in range(1, 4)]
    state = QueueState(entries=[JobEntry.from_dict(d) for d in entry_dicts])

    collected = list(state)

    assert len(collected) == 3
    assert all(isinstance(e, JobEntry) for e in collected)


@pytest.mark.ai_generated
def test_queue_state_len_returns_entry_count() -> None:
    entry_dicts = [_make_entry_dict(dandiset_id=f"00000{i}") for i in range(1, 6)]
    state = QueueState(entries=[JobEntry.from_dict(d) for d in entry_dicts])
    assert len(state) == 5


# ---------------------------------------------------------------------------
# QueueState.from_assets_yaml
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_upstream_metadata_empty() -> mock.MagicMock:
    """Patch upstream metadata lookup to return empty results (no network calls)."""
    return mock.patch(
        "dandi_compute_code.queue._queue_state._UpstreamMetadataCache.get",
        return_value=AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata={}),
    )


@pytest.mark.ai_generated
def test_queue_state_from_assets_yaml_uses_network_when_no_file_path(
    _mock_upstream_metadata_empty: mock.MagicMock,
) -> None:
    attempt_asset_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-default_config-abc123_attempt-1/code/submit.sh"
    )
    source_path = "sub-mouse01/sub-mouse01_ecephys.nwb"

    network_metadata = AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            attempt_asset_path: AssetMetadata(
                path=attempt_asset_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=1,
                content_id="attempt-content-id",
            ),
            source_path: AssetMetadata(
                path=source_path,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=500,
                content_id="source-content-id",
            ),
        },
    )
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=network_metadata,
        ),
        _mock_upstream_metadata_empty,
    ):
        queue_state = QueueState.from_assets_yaml()

    assert len(queue_state) == 1
    assert queue_state.entries[0].job.dandiset_id == "001697"
    assert queue_state.entries[0].has_code is True


@pytest.mark.ai_generated
def test_queue_state_from_assets_yaml_loads_from_local_file(
    tmp_path: pathlib.Path,
    _mock_upstream_metadata_empty: mock.MagicMock,
) -> None:
    attempt_asset_path = (
        "derivatives/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/"
        "version-v1.0_codebase-v0.3.0_params-default_config-abc123_attempt-1/code/submit.sh"
    )
    assets = [
        {
            "path": attempt_asset_path,
            "contentSize": 1,
            "dateModified": "2024-01-01T00:00:00+00:00",
            "contentUrl": ["https://dandiarchive.s3.amazonaws.com/blobs/attempt-id-abc"],
        }
    ]
    assets_file = tmp_path / "assets.yaml"
    assets_file.write_text(yaml.dump(assets))

    with _mock_upstream_metadata_empty:
        queue_state = QueueState.from_assets_yaml(assets_file)

    assert len(queue_state) == 1
    assert queue_state.entries[0].job.dandiset_id == "001697"
    assert queue_state.entries[0].has_code is True


@pytest.mark.ai_generated
def test_queue_state_from_assets_yaml_raises_for_non_list_yaml(tmp_path: pathlib.Path) -> None:
    assets_file = tmp_path / "assets.yaml"
    assets_file.write_text(yaml.dump({"not": "a list"}))

    with pytest.raises(ValueError, match="Expected a YAML list"):
        QueueState.from_assets_yaml(assets_file)


# ---------------------------------------------------------------------------
# Public export check
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_job_entry_and_queue_state_are_publicly_exported() -> None:
    from dandi_compute_code import queue

    assert queue.JobEntry is JobEntry
    assert queue.QueueState is QueueState
