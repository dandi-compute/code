import pytest

from dandi_compute_code.lfp_pipeline import LFPParameters, resolve_filter_kwargs, resolve_reference_spec


@pytest.mark.ai_generated
def test_resolve_filter_kwargs_defaults() -> None:
    filter_kwargs = resolve_filter_kwargs(LFPParameters())

    assert filter_kwargs == {
        "freq_min": 1.0,
        "freq_max": 400.0,
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
    filter_kwargs = resolve_filter_kwargs(LFPParameters(filter_direction=filter_direction))

    assert filter_kwargs["direction"] == expected_direction


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_family", ["butter", "bessel", "fir"])
def test_resolve_filter_kwargs_family_is_forwarded_as_ftype(filter_family: str) -> None:
    filter_kwargs = resolve_filter_kwargs(LFPParameters(filter_family=filter_family))

    assert filter_kwargs["ftype"] == filter_family


@pytest.mark.ai_generated
@pytest.mark.parametrize("filter_band", [(1.0, 400.0), (0.5, 500.0), (0.1, 300.0), (0.5, 250.0)])
def test_resolve_filter_kwargs_band_edges(filter_band: tuple[float, float]) -> None:
    filter_kwargs = resolve_filter_kwargs(LFPParameters(filter_band=filter_band))

    assert (filter_kwargs["freq_min"], filter_kwargs["freq_max"]) == filter_band


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
    reference_spec = resolve_reference_spec(LFPParameters(reference_scheme=reference_scheme))

    assert reference_spec == expected_spec
