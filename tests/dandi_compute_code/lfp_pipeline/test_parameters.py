import pydantic
import pytest

from dandi_compute_code.lfp_pipeline import LFPParameters


@pytest.mark.ai_generated
def test_default_parameters() -> None:
    parameters = LFPParameters()

    assert parameters.filter_family == "butter"
    assert parameters.filter_band == (1.0, 400.0)
    assert parameters.filter_order == 4
    assert parameters.filter_direction == "zero-phase"
    assert parameters.reference_scheme == "CMR"
    assert parameters.resample_target_fs == 1250
    assert parameters.spatial_factor == 1
    assert parameters.bad_channel_method == "coherence+psd"


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_family", ["butter", "bessel", "fir"])
def test_valid_filter_family(filter_family: str) -> None:
    parameters = LFPParameters(filter_family=filter_family)

    assert parameters.filter_family == filter_family


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_band", [(1.0, 400.0), (0.5, 500.0), (0.1, 300.0), (0.5, 250.0)])
def test_valid_filter_band(filter_band: tuple[float, float]) -> None:
    parameters = LFPParameters(filter_band=filter_band)

    assert parameters.filter_band == filter_band


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_order", [2, 4, 8])
def test_valid_filter_order(filter_order: int) -> None:
    parameters = LFPParameters(filter_order=filter_order)

    assert parameters.filter_order == filter_order


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_direction", ["causal", "zero-phase"])
def test_valid_filter_direction(filter_direction: str) -> None:
    parameters = LFPParameters(filter_direction=filter_direction)

    assert parameters.filter_direction == filter_direction


@pytest.mark.ai_generated
@pytest.mark.parametrize("reference_scheme", ["none", "CMR", "per-shank median"])
def test_valid_reference_scheme(reference_scheme: str) -> None:
    parameters = LFPParameters(reference_scheme=reference_scheme)

    assert parameters.reference_scheme == reference_scheme


@pytest.mark.ai_generated
@pytest.mark.parametrize("resample_target_fs", [1000, 1250, 2500])
def test_valid_resample_target_fs(resample_target_fs: int) -> None:
    parameters = LFPParameters(resample_target_fs=resample_target_fs)

    assert parameters.resample_target_fs == resample_target_fs


@pytest.mark.ai_generated
@pytest.mark.parametrize("spatial_factor", [1, 4])
def test_valid_spatial_factor(spatial_factor: int) -> None:
    parameters = LFPParameters(spatial_factor=spatial_factor)

    assert parameters.spatial_factor == spatial_factor


@pytest.mark.ai_generated
@pytest.mark.parametrize("bad_channel_method", ["none", "coherence+psd", "neighborhood_r2"])
def test_valid_bad_channel_method(bad_channel_method: str) -> None:
    parameters = LFPParameters(bad_channel_method=bad_channel_method)

    assert parameters.bad_channel_method == bad_channel_method


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("filter_family", "chebyshev"),
        ("filter_band", (2.0, 400.0)),
        ("filter_order", 3),
        ("filter_direction", "backward"),
        ("reference_scheme", "average"),
        ("resample_target_fs", 30000),
        ("spatial_factor", 2),
        ("bad_channel_method", "std"),
    ],
)
def test_invalid_values_are_rejected(field: str, value: object) -> None:
    with pytest.raises(pydantic.ValidationError):
        LFPParameters(**{field: value})


@pytest.mark.ai_generated
def test_unknown_field_is_rejected() -> None:
    with pytest.raises(pydantic.ValidationError):
        LFPParameters(highpass_spatial_filter=True)


@pytest.mark.ai_generated
def test_parameters_are_frozen() -> None:
    parameters = LFPParameters()

    with pytest.raises(pydantic.ValidationError):
        parameters.filter_family = "bessel"
