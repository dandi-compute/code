import datetime
import re

import pytest

from dandi_compute_code.lfp_pipeline import (
    build_lfp_job_id,
    build_lfp_pipeline_path,
    find_existing_lfp_capsule_path,
)


@pytest.mark.ai_generated
def test_build_pipeline_path_layout() -> None:
    pipeline_path = build_lfp_pipeline_path(dandiset_id="000409", output_dandi_path="sub-01/sub-01_ecephys")

    assert pipeline_path == "derivatives/dandiset-000409/sub-01/sub-01_ecephys/pipeline-lfp"


@pytest.mark.ai_generated
def test_build_job_id_is_a_dated_hash() -> None:
    job_id = build_lfp_job_id(
        dandiset_id="000409",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id="2f6768c",
        content_id="content-aaa",
    )

    today = datetime.datetime.now(tz=datetime.timezone.utc).date()
    assert re.fullmatch(rf"job-{today:%y%m%d}\+[0-9a-f]{{6}}", job_id) is not None


@pytest.mark.ai_generated
def test_build_job_id_carries_no_legacy_entities() -> None:
    job_id = build_lfp_job_id(
        dandiset_id="000409",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id="2f6768c",
        content_id="content-aaa",
    )

    assert "_attempt-" not in job_id
    assert "_config-" not in job_id
    assert "version-" not in job_id
    assert "_params-" not in job_id


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("params_id", "content_id"),
    [
        pytest.param("0000000", "content-aaa", id="different_params"),
        pytest.param("2f6768c", "content-bbb", id="different_asset"),
    ],
)
def test_build_job_id_differs_per_identifying_field(params_id: str, content_id: str) -> None:
    baseline_job_id = build_lfp_job_id(
        dandiset_id="000409",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id="2f6768c",
        content_id="content-aaa",
    )
    job_id = build_lfp_job_id(
        dandiset_id="000409",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id=params_id,
        content_id=content_id,
    )

    assert job_id != baseline_job_id


@pytest.mark.ai_generated
def test_find_existing_capsule_matches_an_earlier_date() -> None:
    """A capsule prepared on an earlier date is recognised by its job hash."""
    pipeline_path = build_lfp_pipeline_path(dandiset_id="000409", output_dandi_path="sub-01/sub-01_ecephys")
    job_id = build_lfp_job_id(
        dandiset_id="000409",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id="2f6768c",
        content_id="content-aaa",
    )
    job_hash = job_id.split("+")[-1]
    existing_capsule_name = f"job-200101+{job_hash}"

    found = find_existing_lfp_capsule_path(
        asset_paths=[f"{pipeline_path}/{existing_capsule_name}/code/submit.sh"],
        pipeline_path=pipeline_path,
        job_id=job_id,
    )

    assert found == f"{pipeline_path}/{existing_capsule_name}"


@pytest.mark.ai_generated
def test_find_existing_capsule_ignores_another_job() -> None:
    """A capsule for a different job is not mistaken for this one."""
    pipeline_path = build_lfp_pipeline_path(dandiset_id="000409", output_dandi_path="sub-01/sub-01_ecephys")
    job_id = build_lfp_job_id(
        dandiset_id="000409",
        dandi_path="sub-01/sub-01_ecephys.nwb",
        bidsy_version="v0.4.0",
        params_id="2f6768c",
        content_id="content-aaa",
    )

    found = find_existing_lfp_capsule_path(
        asset_paths=[f"{pipeline_path}/job-200101+ffffff/code/submit.sh"],
        pipeline_path=pipeline_path,
        job_id=job_id,
    )

    assert found is None
