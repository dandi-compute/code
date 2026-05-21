"""
Unit tests for prepare_aind_ephys_job in the AIND ephys pipeline module.

Tests focus on BIDS entity parsing from Dandiset path components, which is the
logic that resolves the ``sub-`` label used in the output directory hierarchy.
"""

import gzip
import json
import os
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.aind_ephys_pipeline._prepare_job import prepare_aind_ephys_job

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_COMMIT_HASH = "a" * 40


def _make_urlopen_mock(mapping: dict) -> mock.MagicMock:
    """Build an ``urllib.request.urlopen`` mock that returns a gzip-compressed JSON mapping."""
    payload = gzip.compress(json.dumps(mapping).encode())
    response = mock.MagicMock()
    response.read.return_value = payload
    response.__enter__ = lambda s: s
    response.__exit__ = mock.MagicMock(return_value=False)
    return mock.MagicMock(return_value=response)


def _git_check_output(cmd, *, cwd=None, text=False, **kwargs):
    """Return plausible fake git output based on the subcommand."""
    if "describe" in cmd:
        return "v1.0.0-0-gaaaaaaa\n"
    return _FAKE_COMMIT_HASH + "\n"


@pytest.fixture()
def fake_pipeline_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """Create a minimal fake pipeline directory structure."""
    pipeline_dir = tmp_path / "pipeline_repo"
    pipeline_dir.mkdir()
    main_nf = pipeline_dir / "pipeline" / "main_multi_backend.nf"
    main_nf.parent.mkdir(parents=True)
    main_nf.write_text("// fake nextflow pipeline")
    (pipeline_dir / "pipeline" / "capsule_versions.env").write_text("CAPSULE_VERSION=1\n")
    return pipeline_dir


# ---------------------------------------------------------------------------
# Tests for BIDS entity parsing via directory components
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dandiset_path", "expected_sub"),
    [
        # Standard BIDS filename with sub- in stem
        ("sub-mouse01_ses-01_ecephys.nwb", "mouse01"),
        # AIND-style: sub- only in the first directory component
        (
            "sub-703986_2024-09-13_11-19-19"
            "/ecephys_703986_2024-09-13_11-19-19"
            "/ecephys_703986_2024-09-13_11-19-19.nwb",
            "703986",
        ),
        # sub- in directory alongside an explicit ses- directory
        ("sub-mouse01/ses-20240101/sub-mouse01_ses-20240101_ecephys.nwb", "mouse01"),
    ],
)
def test_prepare_aind_ephys_job_extracts_sub_entity_from_path(
    dandiset_path: str,
    expected_sub: str,
    tmp_path: pathlib.Path,
    fake_pipeline_dir: pathlib.Path,
) -> None:
    """prepare_aind_ephys_job resolves the 'sub' entity from directory parts when absent in the filename."""
    content_id = "04000000-0000-0000-0000-000000000000"
    mapping = {content_id: {"000001": dandiset_path}}

    temp_dir = tmp_path / "tmpdir"
    temp_dir.mkdir()

    mock_dandiset = mock.MagicMock()
    mock_dandiset.get_assets_with_path_prefix.return_value = iter([])

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        mock.patch("subprocess.check_output", side_effect=_git_check_output),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.dandiapi.DandiAPIClient") as mock_client,
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.download.download"),
        mock.patch("dandi_compute_code.aind_ephys_pipeline._prepare_job.dandi.upload.upload"),
        mock.patch("tempfile.mkdtemp", return_value=str(temp_dir)),
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "fake-key"}),
    ):
        mock_client.return_value.get_dandiset.return_value = mock_dandiset

        script_path = prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=content_id,
            config_key="default",
            parameters_key="default",
            pipeline_directory=fake_pipeline_dir,
        )

    assert f"sub-{expected_sub}" in str(script_path)


@pytest.mark.ai_generated
def test_prepare_aind_ephys_job_raises_on_missing_sub_entity(tmp_path: pathlib.Path) -> None:
    """prepare_aind_ephys_job raises a clear ValueError when no 'sub' entity can be extracted from the path."""
    content_id = "05000000-0000-0000-0000-000000000000"
    no_sub_path = "ecephys_703986_2024-09-13_11-19-19/ecephys_703986_2024-09-13.nwb"
    mapping = {content_id: {"000001": no_sub_path}}

    with (
        mock.patch("urllib.request.urlopen", _make_urlopen_mock(mapping)),
        pytest.raises(ValueError, match="Could not extract 'sub' BIDS entity"),
    ):
        prepare_aind_ephys_job(
            pipeline_version="v1.1.0",
            content_id=content_id,
            config_key="default",
            parameters_key="default",
            pipeline_directory=tmp_path,
        )
