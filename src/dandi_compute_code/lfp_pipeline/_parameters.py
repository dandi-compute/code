import typing

import pydantic

from ._globals import _ALLOWED_FILTER_BANDS


class LFPParameters(pydantic.BaseModel):
    """
    The minimal set of parameters exposed by the LFP extraction pipeline.

    Every field is constrained to a small, explicit set of choices. The values
    map onto SpikeInterface preprocessing calls through
    :func:`.resolve_filter_kwargs`, :func:`.resolve_reference_spec`, and the
    ``extract_lfp`` step ordering.

    Parameters
    ----------
    filter_family : {"butter", "bessel", "fir"}
        The filter design family forwarded to SpikeInterface as the ``ftype``.
    filter_band : tuple of (float, float)
        The ``(freq_min, freq_max)`` bandpass edges in Hz. Must be one of the
        registered bands.
    filter_order : {2, 4, 8}
        The filter order forwarded to SpikeInterface as ``filter_order``.
    filter_direction : {"causal", "zero-phase"}
        Whether the filter is applied in a single forward (causal) pass or a
        forward-backward (zero-phase) pass.
    reference_scheme : {"none", "CMR", "per-shank median"}
        The re-referencing scheme. "CMR" applies a common median reference
        across all channels. "per-shank median" applies a median reference
        within each shank group.
    resample_target_fs : {1000, 1250, 2500}
        The target sampling frequency in Hz after resampling.
    spatial_factor : {1, 4}
        The spatial decimation factor. A factor of 1 keeps every channel while
        a factor of 4 keeps every fourth channel ordered by depth.
    bad_channel_method : {"none", "coherence+psd", "neighborhood_r2"}
        The SpikeInterface bad channel detection method. Detected channels are
        removed before filtering. "none" skips detection entirely.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")

    filter_family: typing.Literal["butter", "bessel", "fir"] = "butter"
    filter_band: tuple[float, float] = (1.0, 400.0)
    filter_order: typing.Literal[2, 4, 8] = 4
    filter_direction: typing.Literal["causal", "zero-phase"] = "zero-phase"
    reference_scheme: typing.Literal["none", "CMR", "per-shank median"] = "CMR"
    resample_target_fs: typing.Literal[1000, 1250, 2500] = 1250
    spatial_factor: typing.Literal[1, 4] = 1
    bad_channel_method: typing.Literal["none", "coherence+psd", "neighborhood_r2"] = "coherence+psd"

    @pydantic.field_validator("filter_band", mode="after")
    @classmethod
    def _restrict_filter_band(cls, value: tuple[float, float]) -> tuple[float, float]:
        if value not in _ALLOWED_FILTER_BANDS:
            allowed = ", ".join(str(band) for band in _ALLOWED_FILTER_BANDS)
            message = f"filter_band {value} is not one of the registered bands. Allowed bands are: {allowed}."
            raise ValueError(message)
        return value
