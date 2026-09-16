import pytest

from dandi_compute_code.lfp_pipeline import build_lfp_output_path_base


@pytest.mark.ai_generated
def test_build_output_path_base_layout() -> None:
    output_path_base = build_lfp_output_path_base(
        dandiset_id="000409",
        output_dandi_path="sub-01/sub-01_ecephys",
        bidsy_version="v0.4.0",
        codebase_version="0.4.0",
        params_id="2f6768c",
    )

    assert output_path_base == (
        "derivatives/dandiset-000409/sub-01/sub-01_ecephys/"
        "pipeline-lfp/version-v0.4.0_codebase-v0.4.0_params-2f6768c"
    )


@pytest.mark.ai_generated
def test_build_output_path_base_has_no_attempt_or_config() -> None:
    output_path_base = build_lfp_output_path_base(
        dandiset_id="000409",
        output_dandi_path="sub-01/sub-01_ecephys",
        bidsy_version="v0.4.0",
        codebase_version="0.4.0",
        params_id="2f6768c",
    )

    assert "_attempt-" not in output_path_base
    assert "_config-" not in output_path_base
