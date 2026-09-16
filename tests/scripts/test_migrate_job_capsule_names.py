"""
Tests for the one-off legacy job capsule migration script.

Only the planning side is exercised. The parts that talk to the archive (``dandi download``,
``dandi upload``, ``dandi delete``) are deliberately left alone.
"""

import datetime
import importlib.util
import pathlib
import sys
from unittest import mock

import pytest

from dandi_compute_code.dandiset import AssetMetadata, AssetsJsonldMetadata
from dandi_compute_code.dandiset._job_id import _compute_job_hash

_SCRIPT_PATH = pathlib.Path(__file__).parent.parent.parent / "scripts" / "migrate_job_capsule_names.py"


def _load_script():
    """Import the migration script by path, since ``scripts/`` is not an installed package."""
    spec = importlib.util.spec_from_file_location("migrate_job_capsule_names", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_SUBMIT_DATE = "2025-06-07T08:00:00+00:00"
_SOURCE_PATH = "sub-01/sub-01_ecephys.nwb"
_AIND_PIPELINE_PATH = "derivatives/dandisets-000/dandiset-000409/sub-01/sub-01_ecephys/pipeline-aind+ephys"


def _metadata_for(capsule_names: list[str], *, pipeline_path: str) -> AssetsJsonldMetadata:
    path_to_asset_metadata = {}
    for capsule_name in capsule_names:
        for subpath in ("code/submit.sh", "dataset_description.json"):
            asset_path = f"{pipeline_path}/{capsule_name}/{subpath}"
            path_to_asset_metadata[asset_path] = AssetMetadata(
                path=asset_path,
                date_modified=_SUBMIT_DATE,
                content_size=1,
                content_id=f"capsule-{len(path_to_asset_metadata)}",
            )
    return AssetsJsonldMetadata(content_id_to_asset={}, path_to_asset_metadata=path_to_asset_metadata)


def _upstream_metadata() -> AssetsJsonldMetadata:
    return AssetsJsonldMetadata(
        content_id_to_asset={},
        path_to_asset_metadata={
            _SOURCE_PATH: AssetMetadata(
                path=_SOURCE_PATH,
                date_modified="2025-01-01T00:00:00+00:00",
                content_size=9,
                content_id="content-xyz",
            )
        },
    )


def _plan(capsule_names: list[str], *, pipeline_path: str = _AIND_PIPELINE_PATH) -> list[tuple[str, str, dict]]:
    script = _load_script()
    with (
        mock.patch.object(
            script,
            "load_assets_jsonld_metadata",
            return_value=_metadata_for(capsule_names, pipeline_path=pipeline_path),
        ),
        mock.patch.object(script, "_load_upstream_assets_jsonld_metadata", return_value=_upstream_metadata()),
    ):
        return script.plan_migration(dandiset_id="001697")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("capsule_name", "expected_version", "expected_codebase", "expected_config"),
    [
        pytest.param(
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678", "v1.1.0", "v0.3.0", "def5678", id="flat"
        ),
        pytest.param(
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678_attempt-2",
            "v1.1.0",
            "v0.3.0",
            "def5678",
            id="flat_attempt",
        ),
        pytest.param("version-v1.0.0/params-abc1234_config-def5678", "v1.0.0", "", "def5678", id="nested"),
        pytest.param(
            "version-v1.0.0/params-abc1234_config-def5678_attempt-3", "v1.0.0", "", "def5678", id="nested_attempt"
        ),
        pytest.param("version-v1.1.0_codebase-v0.3.0_params-abc1234", "v1.1.0", "v0.3.0", "", id="flat_without_config"),
    ],
)
def test_plan_migration_parses_every_legacy_layout(
    capsule_name: str,
    expected_version: str,
    expected_codebase: str,
    expected_config: str,
) -> None:
    """Each legacy capsule layout is parsed back into the identity its name spelled out."""
    plan = _plan([capsule_name])

    assert len(plan) == 1
    old_path, new_path, identity = plan[0]
    assert old_path == f"{_AIND_PIPELINE_PATH}/{capsule_name}"
    assert new_path.startswith(f"{_AIND_PIPELINE_PATH}/job-")
    assert identity["version"] == expected_version
    assert identity["codebase"] == expected_codebase
    assert identity["config"] == expected_config
    assert identity["params"] == "abc1234"
    assert identity["dandi_path"] == _SOURCE_PATH
    assert identity["content_id"] == "content-xyz"


@pytest.mark.ai_generated
def test_plan_migration_dates_the_job_id_from_the_submission_script() -> None:
    """A migrated capsule keeps the date it was originally prepared."""
    plan = _plan(["version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678"])

    submit_date = datetime.datetime.fromisoformat(_SUBMIT_DATE).date()
    assert plan[0][2]["job_id"].startswith(f"job-{submit_date:%y%m%d}+")


@pytest.mark.ai_generated
def test_plan_migration_hash_matches_what_preparation_would_compute() -> None:
    """A migrated job keeps the hash preparation gives it, so it is never formed a second time."""
    plan = _plan(["version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678"])

    expected_hash = _compute_job_hash(
        dandiset_id="000409",
        dandi_path=_SOURCE_PATH,
        pipeline="aind+ephys",
        version="v1.1.0",
        params="abc1234",
        config="def5678",
        content_id="content-xyz",
    )
    assert plan[0][2]["job_id"].split("+")[-1] == expected_hash


@pytest.mark.ai_generated
def test_plan_migration_ignores_already_migrated_capsules() -> None:
    """Capsules that already carry a job ID are left out of the plan."""
    plan = _plan(["job-250607+abc123", "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678"])

    assert len(plan) == 1
    assert plan[0][0].endswith("version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678")


@pytest.mark.ai_generated
def test_plan_migration_gives_distinct_jobs_distinct_ids() -> None:
    """Capsules that differ in any identifying field do not collide on one job ID."""
    plan = _plan(
        [
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678",
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-aaa1111",
            "version-v1.0.0/params-abc1234_config-def5678",
        ]
    )

    assert len(plan) == 3
    assert len({new_path for _, new_path, _ in plan}) == 3


@pytest.mark.ai_generated
def test_plan_migration_skips_capsules_that_would_collide() -> None:
    """
    Two capsules differing only in codebase version are the same logical job, so they map to
    one job ID. Migrating both would merge them, so neither is planned.
    """
    plan = _plan(
        [
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678",
            "version-v1.1.0_codebase-v0.4.0_params-abc1234_config-def5678",
        ]
    )

    assert plan == []


@pytest.mark.ai_generated
def test_plan_migration_keeps_non_colliding_capsules_alongside_a_collision() -> None:
    """A collision removes only the capsules involved in it."""
    plan = _plan(
        [
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-def5678",
            "version-v1.1.0_codebase-v0.4.0_params-abc1234_config-def5678",
            "version-v1.1.0_codebase-v0.3.0_params-abc1234_config-aaa1111",
        ]
    )

    assert len(plan) == 1
    assert plan[0][0].endswith("config-aaa1111")
