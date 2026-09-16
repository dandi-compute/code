"""
Tests for the ``job-{YYMMDD}+{hash}`` job capsule layout.

A job capsule directory name carries only the job ID, so the pipeline version, codebase
version, parameters and config are read back from the provenance block written into the
capsule's ``dataset_description.json``.
"""

import pathlib
from unittest import mock

import pytest

import dandi_compute_code.queue._queue_utils
from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.queue import JobEntry, QueueState

_JOB_ID = "job-240101+a1b2c3"
_CAPSULE_PATH = f"derivatives/dandisets-001/dandiset-001697/sub-mouse01/sub-mouse01_ecephys/pipeline-test/{_JOB_ID}"
_SOURCE_PATH = "sub-mouse01/sub-mouse01_ecephys.nwb"

_PROVENANCE = {
    "job_id": _JOB_ID,
    "pipeline": "test",
    "version": "v1.1.0",
    "codebase": "v0.4.0",
    "params": "abc1234",
    "config": "def5678",
}


def _capsule_metadata() -> AssetsJsonldMetadata:
    paths = [f"{_CAPSULE_PATH}/code/submit.sh", f"{_CAPSULE_PATH}/dataset_description.json"]
    path_to_asset_metadata = {
        path: AssetMetadata(
            path=path,
            date_modified="2024-01-01T00:00:00+00:00",
            content_size=1,
            content_id=f"content-{index}",
        )
        for index, path in enumerate(paths)
    }
    content_id_to_asset = {
        f"content-{index}": {"path": path, "contentUrl": [f"https://example.org/blobs/content-{index}"]}
        for index, path in enumerate(paths)
    }
    return AssetsJsonldMetadata(
        content_id_to_asset=content_id_to_asset,
        path_to_asset_metadata=path_to_asset_metadata,
    )


def _source_metadata() -> AssetsJsonldMetadata:
    return AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            _SOURCE_PATH: AssetMetadata(
                path=_SOURCE_PATH,
                date_modified="2024-01-01T00:00:00+00:00",
                content_size=512,
                content_id="source-asset",
            )
        },
    )


def _build_state(*, dataset_description: dict) -> QueueState:
    with (
        mock.patch(
            "dandi_compute_code.queue._queue_state.load_assets_jsonld_metadata",
            return_value=_capsule_metadata(),
        ),
        mock.patch(
            "dandi_compute_code.queue._queue_utils._load_upstream_assets_jsonld_metadata",
            return_value=_source_metadata(),
        ),
        mock.patch.object(
            dandi_compute_code.queue._queue_utils,
            "_read_asset_json",
            return_value=dataset_description,
        ),
    ):
        state = QueueState.from_dandi()
    return state


@pytest.mark.ai_generated
def test_from_dandi_reads_identity_from_capsule_provenance() -> None:
    """A job-ID capsule takes its version, codebase, params and config from its provenance."""
    state = _build_state(dataset_description={"Name": "example", "DandiCompute": _PROVENANCE})

    assert len(state) == 1
    entry = state.entries[0]
    assert entry.job.job_id == _JOB_ID
    assert entry.job.dandiset_id == "001697"
    assert entry.job.dandi_path == _SOURCE_PATH
    assert entry.job.pipeline == "test"
    assert entry.job.version == "v1.1.0"
    assert entry.job.codebase == "v0.4.0"
    assert entry.job.params == "abc1234"
    assert entry.job.config == "def5678"
    assert entry.content_id == "source-asset"
    assert entry.asset_size_bytes == 512
    assert entry.is_pending is True


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "dataset_description",
    [
        pytest.param({}, id="no_dataset_description"),
        pytest.param({"Name": "example"}, id="no_provenance_block"),
        pytest.param({"DandiCompute": "not-a-mapping"}, id="malformed_provenance_block"),
    ],
)
def test_from_dandi_keeps_capsule_without_provenance(dataset_description: dict) -> None:
    """A job-ID capsule with unreadable provenance is still reported, with blank identity fields."""
    state = _build_state(dataset_description=dataset_description)

    assert len(state) == 1
    entry = state.entries[0]
    assert entry.job.job_id == _JOB_ID
    assert entry.job.version == ""
    assert entry.job.codebase == ""
    assert entry.job.params == ""
    assert entry.job.config == ""


@pytest.mark.ai_generated
def test_capsule_dir_candidates_prefers_the_job_id_directory(tmp_path: pathlib.Path) -> None:
    """The job-ID directory comes first, with the two legacy layouts kept as fallbacks."""
    entry = JobEntry.from_dict(
        {
            "job_id": _JOB_ID,
            "dandiset_id": "001697",
            "dandi_path": _SOURCE_PATH,
            "pipeline": "test",
            "version": "v1.1.0",
            "params": "abc1234",
            "config": "def5678",
            "codebase": "v0.4.0",
            "content_id": None,
            "asset_size_bytes": None,
        }
    )

    candidates = entry.capsule_dir_candidates(tmp_path)

    pipeline_dir = (
        tmp_path
        / "derivatives"
        / "dandisets-001"
        / "dandiset-001697"
        / "sub-mouse01"
        / "sub-mouse01_ecephys"
        / "pipeline-test"
    )
    assert candidates == (
        pipeline_dir / _JOB_ID,
        pipeline_dir / "version-v1.1.0_codebase-v0.4.0_params-abc1234_config-def5678",
        pipeline_dir / "version-v1.1.0" / "params-abc1234_config-def5678",
    )


@pytest.mark.ai_generated
def test_capsule_dir_candidates_omits_job_id_when_absent(tmp_path: pathlib.Path) -> None:
    """A legacy entry without a job ID offers only the two legacy layouts."""
    entry = JobEntry.from_dict(
        {
            "job_id": "",
            "dandiset_id": "001697",
            "dandi_path": _SOURCE_PATH,
            "pipeline": "test",
            "version": "v1.1.0",
            "params": "abc1234",
            "config": "def5678",
            "codebase": "v0.4.0",
            "content_id": None,
            "asset_size_bytes": None,
        }
    )

    candidates = entry.capsule_dir_candidates(tmp_path)

    assert len(candidates) == 2
    assert all("job-" not in candidate.name for candidate in candidates)
