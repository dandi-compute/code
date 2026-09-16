import argparse

import pynwb
import spikeinterface.extractors

from ._load_parameters import load_lfp_parameters
from ._write_nwb import run_lfp_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dandi_compute_code.lfp_pipeline",
        description="Run the LFP pipeline on a local NWB file and write an NWB file with the LFP under processing.",
    )
    parser.add_argument("--input", required=True, help="Path to the input NWB file (a valid NWB file, no .nwb suffix).")
    parser.add_argument("--output", required=True, help="Path to write the resulting NWB file.")
    parser.add_argument("--params", default="default", help="Registered LFP parameters key. Defaults to 'default'.")
    return parser


def main() -> None:
    arguments = _build_parser().parse_args()

    parameters = load_lfp_parameters(arguments.params)
    recording = spikeinterface.extractors.NwbRecordingExtractor(file_path=arguments.input)
    with pynwb.NWBHDF5IO(path=arguments.input, mode="r") as io:
        source_nwbfile = io.read()
        session_description = source_nwbfile.session_description or "LFP derivative"
        identifier = source_nwbfile.identifier
        session_start_time = source_nwbfile.session_start_time

    nwbfile = pynwb.NWBFile(
        session_description=session_description,
        identifier=identifier,
        session_start_time=session_start_time,
    )
    run_lfp_pipeline(
        recording=recording,
        nwbfile=nwbfile,
        parameters=parameters,
        nwbfile_path=arguments.output,
    )


if __name__ == "__main__":
    main()
