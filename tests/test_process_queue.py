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
            "max_fail_per_dandiset": 2,
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


def _mock_urlopen_response(payload: object) -> mock.MagicMock:
    mock_response = mock.MagicMock()
    mock_response.read.return_value = gzip.compress(json.dumps(payload).encode())
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = False
    return mock_response


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
    mock_dandiset_map = {"asset-bbb": {"311000": "pathin/dandiset"}, "asset-ccc": {"311001": "pathin/another_dandiset"}}

    qualifying_ids = ["asset-bbb", "asset-ccc"]
    with mock.patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = [
            _mock_urlopen_response(qualifying_ids),
            _mock_urlopen_response(mock_dandiset_map),
        ]

        _fill_waiting(
            cwd=queue_dir,
            pipeline="test",
            version="v1.0",
            params="default",
            dandiset_directory=tmp_path,
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
            dandiset_directory=tmp_path,
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
    mock_dandiset_map = {"asset-bbb": {"311000": "pathin/dandiset"}}
    with mock.patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = [
            _mock_urlopen_response(qualifying_ids),
            _mock_urlopen_response(mock_dandiset_map),
        ]

        _fill_waiting(
            cwd=queue_dir,
            pipeline="test",
            version="v1.0",
            params="default",
            dandiset_directory=tmp_path,
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

    result = _submit_next(cwd=queue_dir, dandiset_directory=tmp_path)

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
        result = _submit_next(cwd=queue_dir, dandiset_directory=tmp_path)

    assert result is True

    # First entry should have been consumed
    remaining = [json.loads(line) for line in (queue_dir / "waiting.jsonl").read_text().splitlines() if line.strip()]
    assert len(remaining) == 1
    assert remaining[0]["content_id"] == "asset-ccc"

    # First entry should have been written to submitted.jsonl
    submitted = [json.loads(line) for line in (queue_dir / "submitted.jsonl").read_text().splitlines() if line.strip()]
    assert len(submitted) == 1
    assert submitted[0]["content_id"] == "asset-bbb"

    submitted_command = mock_run.call_args.args[0]
    assert submitted_command[submitted_command.index("--version") + 1] == "v1.0"


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
        result = _submit_next(cwd=queue_dir, dandiset_directory=tmp_path)

    assert result is True

    submitted = [json.loads(line) for line in (queue_dir / "submitted.jsonl").read_text().splitlines() if line.strip()]
    submitted_ids = [e["content_id"] for e in submitted]
    assert "asset-bbb" in submitted_ids
    # asset-aaa must not appear as a new submission
    assert submitted_ids.count("asset-aaa") == 1  # only the original pre-populated one


@pytest.mark.ai_generated
def test_submit_next_strips_commit_suffix_from_version(tmp_path: pathlib.Path) -> None:
    """_submit_next removes only a trailing commit hash suffix from the queued version."""
    queue_dir = _make_queue_dir(tmp_path)
    queue_config = json.loads((queue_dir / "queue_config.json").read_text())
    queue_config["pipelines"]["test"]["version_priority"] = ["v1.1.0+abcdef0"]
    (queue_dir / "queue_config.json").write_text(json.dumps(queue_config))

    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [{"pipeline": "test", "version": "v1.1.0+abcdef0", "params": "default", "content_id": "asset-bbb"}],
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_directory=tmp_path)

    assert result is True
    submitted_command = mock_run.call_args.args[0]
    assert submitted_command[submitted_command.index("--version") + 1] == "v1.1.0"


# ---------------------------------------------------------------------------
# Tests for process_queue (top-level orchestration)
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_process_queue_raises_for_wrong_dir_name(tmp_path: pathlib.Path) -> None:
    """process_queue raises ValueError when the directory is not named 'queue'."""
    wrong_dir = tmp_path / "not_queue"
    wrong_dir.mkdir()

    with pytest.raises(ValueError, match="must be 'queue'"):
        process_queue(cwd=wrong_dir, dandiset_directory=tmp_path)


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
        process_queue(cwd=queue_dir, dandiset_directory=tmp_path)

    assert (queue_dir / "waiting.jsonl").exists()
    assert (queue_dir / "submitted.jsonl").exists()


@pytest.mark.ai_generated
def test_process_queue_submits_when_no_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue calls _submit_next when no AIND jobs are running."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    qualifying_ids = ["asset-bbb"]
    mock_dandiset_map = {"asset-bbb": {"311000": "pathin/dandiset"}}

    with (
        mock.patch(
            "urllib.request.urlopen",
            side_effect=[
                _mock_urlopen_response(qualifying_ids),
                _mock_urlopen_response(mock_dandiset_map),
            ],
        ),
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=False),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    mock_submit.assert_called_once_with(cwd=queue_dir, dandiset_directory=dandiset_dir)


@pytest.mark.ai_generated
def test_process_queue_does_not_submit_when_jobs_running(tmp_path: pathlib.Path) -> None:
    """process_queue does NOT call _submit_next when AIND jobs are currently running."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    qualifying_ids = ["asset-bbb"]
    mock_dandiset_map = {"asset-bbb": {"311000": "pathin/dandiset"}}

    with (
        mock.patch(
            "urllib.request.urlopen",
            side_effect=[
                _mock_urlopen_response(qualifying_ids),
                _mock_urlopen_response(mock_dandiset_map),
            ],
        ),
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=True),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    mock_submit.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for _count_dandiset_failures
# ---------------------------------------------------------------------------


def _make_attempt_dir(
    base: pathlib.Path,
    dandiset_id: str,
    version: str,
    params_id: str,
    config_id: str,
    attempt_number: int,
    *,
    with_code: bool = True,
    with_output: bool = False,
) -> pathlib.Path:
    """
    Create a mock attempt directory inside a fake 001697 clone rooted at *base*.

    A directory path

    ``derivatives/dandiset-{dandiset_id}/sub-test/pipeline-aind+ephys/``
    ``version-{version}/params-{params_id}_config-{config_id}_attempt-{attempt_number}/``

    is created.  *with_code* and *with_output* control whether the ``code/`` and ``output/`` subdirectories are created.
    """
    attempt_dir = (
        base
        / "derivatives"
        / f"dandiset-{dandiset_id}"
        / "sub-test"
        / "pipeline-aind+ephys"
        / f"version-{version}"
        / f"params-{params_id}_config-{config_id}_attempt-{attempt_number}"
    )
    attempt_dir.mkdir(parents=True)
    if with_code:
        (attempt_dir / "code").mkdir()
    if with_output:
        (attempt_dir / "output").mkdir()
    return attempt_dir


@pytest.mark.ai_generated
def test_count_dandiset_failures_returns_zero_when_no_derivatives_dir(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures returns 0 when the derivatives directory does not exist."""
    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 0


@pytest.mark.ai_generated
def test_count_dandiset_failures_counts_failed_attempts(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures counts directories with code/ but no output/ as failures."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_code=True, with_output=False)
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 2, with_code=True, with_output=False)
    # Successful run – must NOT be counted
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 3, with_code=True, with_output=True)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 2


@pytest.mark.ai_generated
def test_count_dandiset_failures_ignores_different_version(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures ignores attempt directories under a different version."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_code=True, with_output=False)
    # Different version – should NOT be counted
    _make_attempt_dir(tmp_path, "000001", "v2.0", "abc1234", "def5678", 1, with_code=True, with_output=False)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 1


@pytest.mark.ai_generated
def test_count_dandiset_failures_counts_all_params_config_combos(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures counts failures across all params/config combinations for the given version."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_code=True, with_output=False)
    # Different params_id – also counted (no filtering by params/config)
    _make_attempt_dir(tmp_path, "000001", "v1.0", "zzz9999", "def5678", 1, with_code=True, with_output=False)
    # Different config_id – also counted
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "yyy8888", 1, with_code=True, with_output=False)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 3


@pytest.mark.ai_generated
def test_count_dandiset_failures_counts_across_all_dandisets(tmp_path: pathlib.Path) -> None:
    """_count_dandiset_failures counts failures across all source dandisets for the given version."""
    _make_attempt_dir(tmp_path, "000001", "v1.0", "abc1234", "def5678", 1, with_code=True, with_output=False)
    # Different dandiset_id – also counted (no per-dandiset filtering)
    _make_attempt_dir(tmp_path, "000002", "v1.0", "abc1234", "def5678", 1, with_code=True, with_output=False)

    result = _count_dandiset_failures(
        dandiset_directory=tmp_path,
        version="v1.0",
    )
    assert result == 2


# ---------------------------------------------------------------------------
# Tests for _submit_next with max_fail_per_dandiset
# ---------------------------------------------------------------------------

#: params_id and config_id used when building fake attempt directories.
_FAKE_PARAMS_ID = "abc1234"
_FAKE_CONFIG_ID = "def5678"


@pytest.mark.ai_generated
def test_submit_next_skips_all_entries_when_total_failures_exceed_max(tmp_path: pathlib.Path) -> None:
    """_submit_next skips all entries when the total failure count reaches max_fail_per_dandiset."""
    queue_dir = _make_queue_dir(tmp_path)

    # Create a fake 001697 clone with 2 failures for dandiset 000001 (== max_fail_per_dandiset)
    dandiset_dir = tmp_path / "001697"
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 2)

    # Queue: both entries for v1.0 – total failures == max_fail_per_dandiset, so all are skipped
    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"},
            {"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-ccc"},
        ],
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    # All entries skipped → queue returns False (nothing submitted)
    assert result is False
    submitted = [json.loads(line) for line in (queue_dir / "submitted.jsonl").read_text().splitlines() if line.strip()]
    assert submitted == []


@pytest.mark.ai_generated
def test_submit_next_allows_entry_when_dandiset_failures_below_max(tmp_path: pathlib.Path) -> None:
    """_submit_next does NOT skip an entry when total failure count is below the limit."""
    queue_dir = _make_queue_dir(tmp_path)

    # Only 1 total failure (< max_fail_per_dandiset=2) → entry should be submitted
    dandiset_dir = tmp_path / "001697"
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1)

    _write_jsonl(
        queue_dir / "waiting.jsonl",
        [{"pipeline": "test", "version": "v1.0", "params": "default", "content_id": "asset-bbb"}],
    )

    with mock.patch("subprocess.run") as mock_run:
        mock_run.return_value = mock.MagicMock(returncode=0, stdout="", stderr="")
        result = _submit_next(cwd=queue_dir, dandiset_directory=dandiset_dir)

    assert result is True

    submitted = [json.loads(line) for line in (queue_dir / "submitted.jsonl").read_text().splitlines() if line.strip()]
    assert submitted[0]["content_id"] == "asset-bbb"


@pytest.mark.ai_generated
def test_process_queue_passes_dandiset_directory_to_submit_next(tmp_path: pathlib.Path) -> None:
    """process_queue forwards dandiset_directory to _submit_next."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"
    dandiset_dir.mkdir()

    qualifying_ids = ["asset-bbb"]
    mock_dandiset_map = {"asset-bbb": {"311000": "pathin/dandiset"}}

    with (
        mock.patch(
            "urllib.request.urlopen",
            side_effect=[
                _mock_urlopen_response(qualifying_ids),
                _mock_urlopen_response(mock_dandiset_map),
            ],
        ),
        mock.patch("dandi_compute_code.queue._process_queue._determine_running", return_value=False),
        mock.patch("dandi_compute_code.queue._process_queue._submit_next") as mock_submit,
    ):
        process_queue(cwd=queue_dir, dandiset_directory=dandiset_dir)

    mock_submit.assert_called_once_with(cwd=queue_dir, dandiset_directory=dandiset_dir)
