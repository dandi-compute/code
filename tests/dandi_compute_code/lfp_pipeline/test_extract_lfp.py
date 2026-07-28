import types

import numpy
import pytest

from dandi_compute_code.lfp_pipeline import LFPParameters, extract_lfp


class _FakeRecording:
    def __init__(self, *, channel_ids, groups, locations, events):
        self.channel_ids = numpy.asarray(channel_ids)
        self._groups = numpy.asarray(groups)
        self._locations = numpy.asarray(locations)
        self._events = events

    def _subset(self, keep_mask):
        return _FakeRecording(
            channel_ids=self.channel_ids[keep_mask],
            groups=self._groups[keep_mask],
            locations=self._locations[keep_mask],
            events=self._events,
        )

    def remove_channels(self, bad_channel_ids):
        self._events.append(("remove_channels", list(bad_channel_ids)))
        keep_mask = numpy.array([channel_id not in set(bad_channel_ids) for channel_id in self.channel_ids])
        return self._subset(keep_mask)

    def channel_slice(self, *, channel_ids):
        self._events.append(("channel_slice", list(channel_ids)))
        keep_mask = numpy.array([channel_id in set(channel_ids) for channel_id in self.channel_ids])
        return self._subset(keep_mask)

    def get_channel_groups(self):
        return self._groups

    def get_channel_locations(self):
        return self._locations


def _install_fake_spikeinterface(monkeypatch, *, events, bad_channel_ids):
    full_module = types.ModuleType("spikeinterface.full")

    def detect_bad_channels(recording, *, method):
        events.append(("detect_bad_channels", method))
        return list(bad_channel_ids), None

    def bandpass_filter(recording, **kwargs):
        events.append(("bandpass_filter", kwargs))
        return recording

    def common_reference(recording, *, reference, operator, groups):
        events.append(("common_reference", {"reference": reference, "operator": operator, "groups": groups}))
        return recording

    def resample(recording, *, resample_rate):
        events.append(("resample", resample_rate))
        return recording

    full_module.detect_bad_channels = detect_bad_channels
    full_module.bandpass_filter = bandpass_filter
    full_module.common_reference = common_reference
    full_module.resample = resample

    spikeinterface_module = types.ModuleType("spikeinterface")
    spikeinterface_module.full = full_module
    monkeypatch.setitem(__import__("sys").modules, "spikeinterface", spikeinterface_module)
    monkeypatch.setitem(__import__("sys").modules, "spikeinterface.full", full_module)


def _make_recording(events):
    return _FakeRecording(
        channel_ids=[0, 1, 2, 3, 4, 5, 6, 7],
        groups=[0, 0, 0, 0, 1, 1, 1, 1],
        locations=[[0.0, float(index)] for index in range(4)] + [[250.0, float(index)] for index in range(4, 8)],
        events=events,
    )


@pytest.mark.ai_generated
def test_extract_lfp_default_step_order(monkeypatch) -> None:
    events: list = []
    _install_fake_spikeinterface(monkeypatch, events=events, bad_channel_ids=[0])
    recording = _make_recording(events)

    extract_lfp(recording=recording, parameters=LFPParameters())

    step_names = [event[0] for event in events]
    assert step_names == ["detect_bad_channels", "remove_channels", "bandpass_filter", "common_reference", "resample"]


@pytest.mark.ai_generated
def test_extract_lfp_forwards_filter_and_reference_and_resample(monkeypatch) -> None:
    events: list = []
    _install_fake_spikeinterface(monkeypatch, events=events, bad_channel_ids=[])
    recording = _make_recording(events)

    extract_lfp(
        recording=recording,
        parameters=LFPParameters(
            filter_family="bessel",
            filter_band=(0.5, 500.0),
            filter_order=8,
            filter_direction="causal",
            reference_scheme="CMR",
            resample_target_fs=1000,
        ),
    )

    events_by_name = {event[0]: event[1] for event in events}
    assert events_by_name["bandpass_filter"] == {
        "freq_min": 0.5,
        "freq_max": 500.0,
        "filter_order": 8,
        "ftype": "bessel",
        "direction": "forward",
    }
    assert events_by_name["common_reference"] == {"reference": "global", "operator": "median", "groups": None}
    assert events_by_name["resample"] == 1000


@pytest.mark.ai_generated
def test_extract_lfp_per_shank_reference_uses_shank_groups(monkeypatch) -> None:
    events: list = []
    _install_fake_spikeinterface(monkeypatch, events=events, bad_channel_ids=[])
    recording = _make_recording(events)

    extract_lfp(recording=recording, parameters=LFPParameters(reference_scheme="per-shank median"))

    events_by_name = {event[0]: event[1] for event in events}
    assert events_by_name["common_reference"]["groups"] == [[0, 1, 2, 3], [4, 5, 6, 7]]


@pytest.mark.ai_generated
def test_extract_lfp_spatial_decimation_selects_every_fourth_channel(monkeypatch) -> None:
    events: list = []
    _install_fake_spikeinterface(monkeypatch, events=events, bad_channel_ids=[])
    recording = _make_recording(events)

    extract_lfp(recording=recording, parameters=LFPParameters(spatial_factor=4))

    events_by_name = {event[0]: event[1] for event in events}
    assert "channel_slice" in events_by_name
    assert events_by_name["channel_slice"] == [0, 4]


@pytest.mark.ai_generated
def test_extract_lfp_skips_optional_steps_when_disabled(monkeypatch) -> None:
    events: list = []
    _install_fake_spikeinterface(monkeypatch, events=events, bad_channel_ids=[])
    recording = _make_recording(events)

    extract_lfp(
        recording=recording,
        parameters=LFPParameters(bad_channel_method="none", reference_scheme="none", spatial_factor=1),
    )

    step_names = [event[0] for event in events]
    assert step_names == ["bandpass_filter", "resample"]
