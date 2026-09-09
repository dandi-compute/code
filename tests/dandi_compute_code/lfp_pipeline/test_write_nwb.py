import neuroconv.tools.spikeinterface
import numpy
import pynwb
import pytest
import spikeinterface.full

from dandi_compute_code.lfp_pipeline import add_lfp_to_nwbfile, run_lfp_pipeline

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


class _FakeRecording:
    def __init__(self, events):
        self.channel_ids = numpy.asarray([0, 1, 2, 3])
        self._events = events

    def remove_channels(self, bad_channel_ids):
        self._events.append(("remove_channels", list(bad_channel_ids)))
        return self


def _patch_spikeinterface_passthrough(monkeypatch):
    monkeypatch.setattr(spikeinterface.full, "detect_bad_channels", lambda recording, *, method: ([], None))
    monkeypatch.setattr(spikeinterface.full, "bandpass_filter", lambda recording, **kwargs: recording)
    monkeypatch.setattr(
        spikeinterface.full, "common_reference", lambda recording, *, reference, operator, groups: recording
    )
    monkeypatch.setattr(spikeinterface.full, "resample", lambda recording, *, resample_rate: recording)


def _patch_add_recording(monkeypatch, calls):
    def add_recording_to_nwbfile(*, recording, nwbfile, parent_container):
        calls.append({"recording": recording, "nwbfile": nwbfile, "parent_container": parent_container})

    monkeypatch.setattr(neuroconv.tools.spikeinterface, "add_recording_to_nwbfile", add_recording_to_nwbfile)


class _FakeIO:
    def __init__(self, *, path, mode):
        self.path = path
        self.mode = mode

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        return False

    def write(self, nwbfile):
        _FakeIO.written.append({"path": self.path, "mode": self.mode, "nwbfile": nwbfile})


@pytest.mark.ai_generated
def test_add_lfp_to_nwbfile_uses_processing_lfp_container(monkeypatch) -> None:
    calls: list = []
    _patch_add_recording(monkeypatch, calls)
    recording = object()
    nwbfile = object()

    returned = add_lfp_to_nwbfile(recording=recording, nwbfile=nwbfile)

    assert returned is nwbfile
    assert len(calls) == 1
    assert calls[0]["recording"] is recording
    assert calls[0]["nwbfile"] is nwbfile
    assert calls[0]["parent_container"] == "processing/LFP"


@pytest.mark.ai_generated
def test_run_lfp_pipeline_adds_extracted_lfp_without_writing(monkeypatch) -> None:
    _patch_spikeinterface_passthrough(monkeypatch)
    calls: list = []
    _patch_add_recording(monkeypatch, calls)
    events: list = []
    recording = _FakeRecording(events)
    nwbfile = object()

    returned = run_lfp_pipeline(recording=recording, nwbfile=nwbfile, parameters=dict(_VALID_PARAMETERS))

    assert returned is nwbfile
    assert len(calls) == 1
    assert calls[0]["nwbfile"] is nwbfile
    assert calls[0]["parent_container"] == "processing/LFP"


@pytest.mark.ai_generated
def test_run_lfp_pipeline_writes_nwbfile_when_path_given(monkeypatch, tmp_path) -> None:
    _patch_spikeinterface_passthrough(monkeypatch)
    _patch_add_recording(monkeypatch, [])
    _FakeIO.written = []
    monkeypatch.setattr(pynwb, "NWBHDF5IO", _FakeIO)
    events: list = []
    recording = _FakeRecording(events)
    nwbfile = object()
    nwbfile_path = tmp_path / "lfp.nwb"

    run_lfp_pipeline(
        recording=recording, nwbfile=nwbfile, parameters=dict(_VALID_PARAMETERS), nwbfile_path=nwbfile_path
    )

    assert len(_FakeIO.written) == 1
    assert _FakeIO.written[0]["nwbfile"] is nwbfile
    assert _FakeIO.written[0]["mode"] == "w"
    assert _FakeIO.written[0]["path"] == str(nwbfile_path)
