"""Allowed values for the minimal set of LFP extraction parameters.

Each constant is the exhaustive set of choices exposed for one parameter. They
are kept here so that the parameter model, the SpikeInterface resolver, and the
tests all agree on a single source of truth.
"""

_ALLOWED_FILTER_FAMILIES = ("butter", "bessel", "fir")
_ALLOWED_FILTER_BANDS = ((1.0, 400.0), (0.5, 500.0), (0.1, 300.0), (0.5, 250.0))
_ALLOWED_FILTER_ORDERS = (2, 4, 8)
_ALLOWED_FILTER_DIRECTIONS = ("causal", "zero-phase")
_ALLOWED_REFERENCE_SCHEMES = ("none", "CMR", "per-shank median")
_ALLOWED_RESAMPLE_TARGET_FS = (1000, 1250, 2500)
_ALLOWED_SPATIAL_FACTORS = (1, 4)
_ALLOWED_BAD_CHANNEL_METHODS = ("none", "coherence+psd", "neighborhood_r2")
