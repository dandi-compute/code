"""
Unit tests for the queue processing module (dandi_compute_code.queue).

Each test is flagged with the `ai_generated` pytest marker and exercises the
core helper functions using an in-memory, temporary queue directory rather
than touching the network or a SLURM cluster.
"""

import collections
import gzip
import json
import pathlib
from unittest import mock

import pytest

from dandi_compute_code.queue._process_queue import (
    _compute_default_config_id,
    _compute_params_id,
    _count_dandiset_failures,
    _determine_running,
    _fetch_counts,
    _fill_waiting,
    _submit_next,
    process_queue,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#: Consolidated queue config used by all tests that need a queue directory.
#: Schema: top-level "pipelines" key; pipeline names are plain (no prefix);
#: version_priority / params_priority are flat lists of plain names;
#: max_attempts_per_asset and asset_overrides live directly on the pipeline object.
_EXAMPLE_QUEUE_CONFIG = {
    "pipelines": {
        "test": {
            "version_priority": ["v1.0"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 2,
            "asset_overrides": {"asset-aaa": 1},
        }
    }
}


def _make_queue_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Build a minimal but realistic queue directory under *tmp_path*.

    Structure
    ---------
    queue/
        waiting.jsonl
        submitted.jsonl
        queue_config.json   (single consolidated config for all pipelines/versions/params)
    """
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    (queue_dir / "waiting.jsonl").write_text("")
    (queue_dir / "submitted.jsonl").write_text("")
    (queue_dir / "queue_config.json").write_text(json.dumps(_EXAMPLE_QUEUE_CONFIG))

    return queue_dir


def _write_jsonl(file_path: pathlib.Path, entries: list[dict]) -> None:
    file_path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")


# ---------------------------------------------------------------------------
# Tests for _fetch_counts
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_fetch_counts_empty_file(tmp_path: pathlib.Path) -> None:
    """_fetch_counts returns an empty Counter when the file is empty."""
    jsonl_file = tmp_path / "submitted.jsonl"
    jsonl_file.write_text("")

    result = _fetch_counts(
        file_path=jsonl_file,
        pipeline="test",
        version="v1.0",
        params="default",
    )

    assert result == collections.Counter()


@pytest.mark.ai_generated
def test_fetch_counts_matches_pipeline(tmp_path: pathlib.Path) -> None:
    """_fetch_counts only counts entries that match pipeline/version/params."""
    jsonl_file = tmp_path / "submitted.jsonl"
    _write_jsonl(
        jsonl_file,
        [
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"},
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"},
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"},
            # Different pipeline – should NOT be counted
            {"pipeline": "other", "version": "v1.0", "params": "default", "content_id": "asset-aaa"},
            # Different params – should NOT be counted
            {"pipeline": "test", "version": "v1.0", "params": "other", "content_id": "asset-bbb"},
        ],
    )

    result = _fetch_counts(
        file_path=jsonl_file,
        pipeline="test",
        version="v1.0",
        params="default",
    )

    assert result["asset-aaa"] == 2
    assert result["asset-bbb"] == 1
    assert result["asset-ccc"] == 0


@pytest.mark.ai_generated
def test_fetch_counts_ignores_blank_lines(tmp_path: pathlib.Path) -> None:
    """_fetch_counts gracefully skips blank lines in the JSONL file."""
    jsonl_file = tmp_path / "submitted.jsonl"
    jsonl_file.write_text(
        "\n"
        + json.dumps({"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-ccc"})
        + "\n\n"
    )

    result = _fetch_counts(
        file_path=jsonl_file,
        pipeline="test",
        version="v1.0",
        params="default",
    )

    assert result["asset-ccc"] == 1


# ---------------------------------------------------------------------------
# Tests for _fill_waiting
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_fill_waiting_adds_entries(tmp_path: pathlib.Path) -> None:
    """_fill_waiting appends qualifying content IDs to waiting.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)

    qualifying_ids = ["asset-bbb", "asset-ccc"]
    with mock.patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = mock.MagicMock()
        mock_response.read.return_value = gzip.compress(json.dumps(qualifying_ids).encode())
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        _fill_waiting(
            cwd=queue_dir,
            pipeline="test",
            version="v1.0",
            params="default",
        )

    lines = [line for line in (queue_dir / "waiting.jsonl").read_text().splitlines() if line.strip()]
    assert len(lines) == 2
    written_ids = {json.loads(line)["content_id"] for line in lines}
    assert written_ids == {"asset-bbb", "asset-ccc"}


@pytest.mark.ai_generated
def test_fill_waiting_skips_when_already_populated(tmp_path: pathlib.Path) -> None:
    """_fill_waiting does not add entries when the waiting queue already has entries for this combination."""
    queue_dir = _make_queue_dir(tmp_path)

    existing_entry = {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"}
    _write_jsonl(queue_dir / "waiting.jsonl", [existing_entry])

    with mock.patch("urllib.request.urlopen") as mock_urlopen:
        _fill_waiting(
            cwd=queue_dir,
            pipeline="test",
            version="v1.0",
            params="default",
        )
        mock_urlopen.assert_not_called()

    lines = [line for line in (queue_dir / "waiting.jsonl").read_text().splitlines() if line.strip()]
    assert len(lines) == 1


@pytest.mark.ai_generated
def test_fill_waiting_respects_max_attempts(tmp_path: pathlib.Path) -> None:
    """_fill_waiting excludes content IDs that have already reached max_attempts_per_asset."""
    queue_dir = _make_queue_dir(tmp_path)

    # asset-aaa has asset_overrides max of 1; mark it as already submitted once
    _write_jsonl(
        queue_dir / "submitted.jsonl",
        [{"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"}],
    )

    qualifying_ids = ["asset-aaa", "asset-bbb"]
    with mock.patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = mock.MagicMock()
        mock_response.read.return_value = gzip.compress(json.dumps(qualifying_ids).encode())
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        _fill_waiting(
            cwd=queue_dir,
            pipeline="test",
            version="v1.0",
            params="default",
        )

    lines = [line for line in (queue_dir / "waiting.jsonl").read_text().splitlines() if line.strip()]
    written_ids = {json.loads(line)["content_id"] for line in lines}
    # asset-aaa has hit its override limit of 1 and must not appear
    assert "asset-aaa" not in written_ids
    assert "asset-bbb" in written_ids


# ---------------------------------------------------------------------------
# Tests for _determine_running
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_determine_running_true_when_aind_job_present() -> None:
    """_determine_running returns True when an AIND job appears in squeue output."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(stdout="JOBNAME\nAIND_ephys_job\nother_job\n", stderr="")
        assert _determine_running() is True


@pytest.mark.ai_generated
def test_determine_running_false_when_no_aind_jobs() -> None:
    """_determine_running returns False when no AIND jobs are in squeue output."""
    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(stdout="JOBNAME\nsome_other_job\n", stderr="")
        assert _determine_running() is False


# ---------------------------------------------------------------------------
# Tests for _submit_next
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_empty(tmp_path: pathlib.Path) -> None:
    """_submit_next returns False and leaves files intact when waiting.jsonl is empty."""
    queue_dir = _make_queue_dir(tmp_path)

    result = _submit_next(cwd=queue_dir)

    assert result is False


@pytest.mark.ai_generated
def test_submit_next_pops_entry_and_records_submission(tmp_path: pathlib.Path) -> None:
    """_submit_next removes the popped entry from waiting and appends it to submitted."""
    queue_dir = _make_queue_dir(tmp_path)

    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"},
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-ccc"},
        ],
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir)

    assert result is True

    # First entry should have been consumed
    remaining = [json.loads(line) for line in (queue_dir / "waiting.jsonl").read_text().splitlines() if line.strip()]
    assert len(remaining) == 1
    assert remaining[0]["content_id"] == "asset-ccc"

    # First entry should have been written to submitted.jsonl
    submitted = [json.loads(line) for line in (queue_dir / "submitted.jsonl").read_text().splitlines() if line.strip()]
    assert len(submitted) == 1
    assert submitted[0]["content_id"] == "asset-bbb"


@pytest.mark.ai_generated
def test_submit_next_skips_exhausted_entries(tmp_path: pathlib.Path) -> None:
    """_submit_next skips entries that have already reached their max attempt count."""
    queue_dir = _make_queue_dir(tmp_path)

    # asset-aaa has override limit of 1; pre-populate submitted with one entry for it
    _write_jsonl(
        queue_dir / "submitted.jsonl",
        [{"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"}],
    )
    # Put asset-aaa first in the waiting queue; asset-bbb is the valid next entry
    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-aaa"},
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"},
        ],
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir)

    assert result is True

    submitted = [json.loads(line) for line in (queue_dir / "submitted.jsonl").read_text().splitlines() if line.strip()]
    submitted_ids = [e["content_id"] for e in submitted]
    assert "asset-bbb" in submitted_ids
    # asset-aaa must not appear as a new submission
    assert submitted_ids.count("asset-aaa") == 1  # only the original pre-populated one


# ---------------------------------------------------------------------------
# Tests for process_queue (top-level orchestration)
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_process_queue_raises_for_wrong_dir_name(tmp_path: pathlib.Path) -> None:
    """process_queue raises ValueError when the directory is not named 'queue'."""
    wrong_dir = tmp_path / "not_queue"
    wrong_dir.mkdir()

    with pytest.raises(ValueError, match="must be 'queue'"):
        process_queue(cwd=wrong_dir)


@pytest.mark.ai_generated
def test_process_queue_creates_missing_jsonl_files(tmp_path: pathlib.Path) -> None:
    """process_queue creates waiting.jsonl and submitted.jsonl if they don't exist."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))

    with (
        mock.patch("dandi_compute_code.queue._process_queue._fill_waiting"),
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=True),
    ):
        process_queue(cwd=queue_dir)

    assert (queue_dir / "waiting.jsonl").exists()
    assert (queue_dir / "submitted.jsonl").exists()


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue calls _submit_next when no AIND jobs are running."""
    queue_dir = _make_queue_dir(tmp_path)

    qualifying_ids = ["asset-bbb"]
    mock_response = mock.MagicMock()
    mock_response.read.return_value = gzip.compress(json.dumps(qualifying_ids).encode())
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = mock.MagicMock(return_value=False)

    with (
        mock.patch("urllib.request.urlopen", return_value=mock_response),
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=False),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir)

    mock_submit.assert_called_once_with(cwd=queue_dir, dandiset_dir=None)


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue does NOT call _submit_next when AIND jobs are currently running."""
    queue_dir = _make_queue_dir(tmp_path)

    qualifying_ids = ["asset-bbb"]
    mock_response = mock.MagicMock()
    mock_response.read.return_value = gzip.compress(json.dumps(qualifying_ids).encode())
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = mock.MagicMock(return_value=False)

    with (
        mock.patch("urllib.request.urlopen", return_value=mock_response),
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=True),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir)

    mock_submit.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for _count_dandiset_failures
# ---------------------------------------------------------------------------


def _make_attempt_dir(
    dandiset_dir: pathlib.Path,
    *,
    version: str,
    params_config_prefix: str,
    attempt_number: int,
    has_code: bool = True,
    has_output: bool = False,
) -> pathlib.Path:
    """Build a mock attempt directory under *dandiset_dir*."""
    attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-000123"
        / "sub-001"
        / "pipeline-aind+ephys"
        / f"version-{version}"
        / f"{params_config_prefix}_attempt-{attempt_number}"
    )
    attempt_dir.mkdir(parents=True, exist_ok=True)
    if has_code:
        (attempt_dir / "code").mkdir()
    if has_output:
        (attempt_dir / "output").mkdir()
    return attempt_dir


@pytest.mark.ai_generated
def test_count_dandiset_failures_empty_dir(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures returns an empty Counter for an empty directory."""
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    result = _count_dandiset_failures(dandiset_dir)

    assert result == collections.Counter()


@pytest.mark.ai_generated
def test_count_dandiset_failures_detects_failure(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures counts a directory with code/ but no output/ as a failure."""
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    _make_attempt_dir(dandiset_dir, version="v1.0", params_config_prefix="params-abc_config-def", attempt_number=1)

    result = _count_dandiset_failures(dandiset_dir)

    assert result[("version-v1.0", "params-abc_config-def")] == 1


@pytest.mark.ai_generated
def test_count_dandiset_failures_ignores_successes(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures does NOT count attempt directories that have an output/ directory."""
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    # One success (has output/)
    _make_attempt_dir(
        dandiset_dir,
        version="v1.0",
        params_config_prefix="params-abc_config-def",
        attempt_number=1,
        has_output=True,
    )

    result = _count_dandiset_failures(dandiset_dir)

    assert result == collections.Counter()


@pytest.mark.ai_generated
def test_count_dandiset_failures_ignores_missing_code(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures does NOT count attempt directories that lack a code/ directory."""
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    _make_attempt_dir(
        dandiset_dir,
        version="v1.0",
        params_config_prefix="params-abc_config-def",
        attempt_number=1,
        has_code=False,
    )

    result = _count_dandiset_failures(dandiset_dir)

    assert result == collections.Counter()


@pytest.mark.ai_generated
def test_count_dandiset_failures_groups_by_version_and_params_config(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures groups failures by (version_dir, params_config_prefix)."""
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    # Two failures under (v1.0, params-abc_config-def)
    _make_attempt_dir(dandiset_dir, version="v1.0", params_config_prefix="params-abc_config-def", attempt_number=1)
    _make_attempt_dir(dandiset_dir, version="v1.0", params_config_prefix="params-abc_config-def", attempt_number=2)
    # One failure under a different params_config combination
    _make_attempt_dir(dandiset_dir, version="v1.0", params_config_prefix="params-xyz_config-uvw", attempt_number=1)
    # One failure under a different version
    _make_attempt_dir(dandiset_dir, version="v2.0", params_config_prefix="params-abc_config-def", attempt_number=1)
    # One success (has output/) – must not be counted
    _make_attempt_dir(
        dandiset_dir,
        version="v1.0",
        params_config_prefix="params-abc_config-def",
        attempt_number=3,
        has_output=True,
    )

    result = _count_dandiset_failures(dandiset_dir)

    assert result[("version-v1.0", "params-abc_config-def")] == 2
    assert result[("version-v1.0", "params-xyz_config-uvw")] == 1
    assert result[("version-v2.0", "params-abc_config-def")] == 1


# ---------------------------------------------------------------------------
# Tests for _submit_next with dandiset_dir / max_fail_per_dandiset
# ---------------------------------------------------------------------------

#: Queue config that includes max_fail_per_dandiset.
_QUEUE_CONFIG_WITH_MAX_FAIL = {
    "pipelines": {
        "test": {
            "version_priority": ["v1.0"],
            "params_priority": ["default"],
            "max_attempts_per_asset": 5,
            "max_fail_per_dandiset": 2,
        }
    }
}


def _make_queue_dir_with_max_fail(tmp_path: pathlib.Path) -> pathlib.Path:
    """Build a queue directory whose config includes max_fail_per_dandiset."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "waiting.jsonl").write_text("")
    (queue_dir / "submitted.jsonl").write_text("")
    (queue_dir / "queue_config.json").write_text(json.dumps(_QUEUE_CONFIG_WITH_MAX_FAIL))
    return queue_dir


@pytest.mark.ai_generated
def test_submit_next_skips_when_max_fail_exceeded(tmp_path: pathlib.Path) -> None:
    """_submit_next skips an entry when failure count in dandiset_dir >= max_fail_per_dandiset."""
    queue_dir = _make_queue_dir_with_max_fail(tmp_path)

    params_id = _compute_params_id("default")
    config_id = _compute_default_config_id()
    assert params_id is not None

    version = "v1.0"
    params_config_prefix = f"params-{params_id}_config-{config_id}"

    # Pre-populate waiting queue with one entry
    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [{"pipeline": "test", "version": version, "params": "default", "content_id": "asset-bbb"}],
    )

    # Create dandiset dir with failures equal to max_fail_per_dandiset (= 2)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()
    _make_attempt_dir(
        dandiset_dir, version=version, params_config_prefix=params_config_prefix, attempt_number=1
    )
    _make_attempt_dir(
        dandiset_dir, version=version, params_config_prefix=params_config_prefix, attempt_number=2
    )

    result = _submit_next(cwd=queue_dir, dandiset_dir=dandiset_dir)

    # No job should have been submitted
    assert result is False
    # The waiting file should be empty (entry was consumed as invalid)
    remaining = [
        json.loads(line)
        for line in (queue_dir / "waiting.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert len(remaining) == 0
    # submitted.jsonl must not have gained any new entries
    submitted = [
        json.loads(line)
        for line in (queue_dir / "submitted.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert len(submitted) == 0


@pytest.mark.ai_generated
def test_submit_next_allows_when_below_max_fail(tmp_path: pathlib.Path) -> None:
    """_submit_next does NOT skip an entry when failure count is below max_fail_per_dandiset."""
    queue_dir = _make_queue_dir_with_max_fail(tmp_path)

    params_id = _compute_params_id("default")
    config_id = _compute_default_config_id()
    assert params_id is not None

    version = "v1.0"
    params_config_prefix = f"params-{params_id}_config-{config_id}"

    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [{"pipeline": "test", "version": version, "params": "default", "content_id": "asset-bbb"}],
    )

    # Create dandiset dir with only one failure (below max of 2)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()
    _make_attempt_dir(
        dandiset_dir, version=version, params_config_prefix=params_config_prefix, attempt_number=1
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_dir=dandiset_dir)

    assert result is True
    submitted = [
        json.loads(line)
        for line in (queue_dir / "submitted.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert len(submitted) == 1
    assert submitted[0]["content_id"] == "asset-bbb"


@pytest.mark.ai_generated
def test_submit_next_without_dandiset_dir_ignores_failures(tmp_path: pathlib.Path) -> None:
    """_submit_next does not check failures when dandiset_dir is not provided."""
    queue_dir = _make_queue_dir_with_max_fail(tmp_path)

    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [{"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"}],
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_dir=None)

    assert result is True


@pytest.mark.ai_generated
def test_process_queue_passes_dandiset_dir_to_submit_next(tmp_path: pathlib.Path) -> None:
    """process_queue forwards dandiset_dir to _submit_next."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    qualifying_ids = ["asset-bbb"]
    mock_response = mock.MagicMock()
    mock_response.read.return_value = gzip.compress(json.dumps(qualifying_ids).encode())
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = mock.MagicMock(return_value=False)

    with (
        mock.patch("urllib.request.urlopen", return_value=mock_response),
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=False),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir, dandiset_dir=dandiset_dir)

    mock_submit.assert_called_once_with(cwd=queue_dir, dandiset_dir=dandiset_dir)
