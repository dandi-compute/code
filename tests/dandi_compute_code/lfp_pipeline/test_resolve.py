import pytest

from dandi_compute_code.lfp_pipeline import resolve_filter_kwargs, resolve_reference_spec

_VALID_PARAMETERS = {
    "filter_family": "butter",
    "filter_band": [1, 400],
    "filter_order": 4,
    "filter_direction": "zero-phase",
    "reference_scheme": "CMR",
    "resample_target_fs": 1250,
    "spatial_factor": 1,
    "bad_channel_method": "coherence+psd",
}


def _parameters(**overrides: object) -> dict:
    parameters = dict(_VALID_PARAMETERS)
    parameters.update(overrides)
    return parameters


@pytest.mark.ai_generated
def test_resolve_filter_kwargs_defaults() -> None:
    filter_kwargs = resolve_filter_kwargs(_parameters())

    assert filter_kwargs == {
        "freq_min": 1,
        "freq_max": 400,
        "filter_order": 4,
        "ftype": "butter",
        "direction": "forward-backward",
    }


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("filter_direction", "expected_direction"),
    [("causal", "forward"), ("zero-phase", "forward-backward")],
)
def test_resolve_filter_kwargs_direction(filter_direction: str, expected_direction: str) -> None:
    filter_kwargs = resolve_filter_kwargs(_parameters(filter_direction=filter_direction))

    assert filter_kwargs["direction"] == expected_direction


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_family", ["butter", "bessel", "fir"])
def test_resolve_filter_kwargs_family_is_forwarded_as_ftype(filter_family: str) -> None:
    filter_kwargs = resolve_filter_kwargs(_parameters(filter_family=filter_family))

    assert filter_kwargs["ftype"] == filter_family


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_band", [[1, 400], [0.5, 500], [0.1, 300], [0.5, 250]])
def test_resolve_filter_kwargs_band_edges(filter_band: list) -> None:
    filter_kwargs = resolve_filter_kwargs(_parameters(filter_band=filter_band))

    assert (filter_kwargs["freq_min"], filter_kwargs["freq_max"]) == tuple(filter_band)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("reference_scheme", "expected_spec"),
    [
        ("none", {"apply": False, "operator": None, "per_shank": False}),
        ("CMR", {"apply": True, "operator": "median", "per_shank": False}),
        ("per-shank median", {"apply": True, "operator": "median", "per_shank": True}),
    ],
)
def test_resolve_reference_spec(reference_scheme: str, expected_spec: dict) -> None:
    reference_spec = resolve_reference_spec(_parameters(reference_scheme=reference_scheme))

    assert reference_spec == expected_spec
