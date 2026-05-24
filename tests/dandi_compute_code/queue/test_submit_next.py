# ruff: noqa: F821
import importlib.util as _importlib_util
import pathlib as _pathlib

_spec = _importlib_util.spec_from_file_location(
    "_process_queue_test_cases",
    _pathlib.Path(__file__).with_name("_process_queue_test_cases.py"),
)
assert _spec is not None
assert _spec.loader is not None
_support = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_support)

globals().update(
    {
        name: value
        for name, value in vars(_support).items()
        if not name.startswith("__") and not name.startswith("test_")
    }
)


@pytest.mark.ai_generated
def test_submit_next_raises_when_state_file_is_absent(tmp_path: pathlib.Path) -> None:
    """_submit_next raises when state.jsonl is absent."""
    queue_dir = _make_queue_dir(tmp_path)

    with pytest.raises(FileNotFoundError, match="State file not found"):
        _submit_next(queue_directory=queue_dir, datalad_directory=tmp_path, dandiset_directory=tmp_path)


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_max_submissions_less_than_one(tmp_path: pathlib.Path) -> None:
    """_submit_next returns False for invalid max_submissions before reading state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)
    result = _submit_next(
        queue_directory=queue_dir,
        datalad_directory=tmp_path,
        dandiset_directory=tmp_path,
        max_submissions=0,
    )
    assert result is False


@pytest.mark.ai_generated
def test_submit_next_logs_and_returns_false_when_state_file_is_empty(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """_submit_next logs and returns False when state.jsonl is empty."""
    queue_dir = _make_queue_dir(tmp_path)
    (queue_dir / "state.jsonl").write_text("")

    with caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._submit_next"):
        result = _submit_next(queue_directory=queue_dir, datalad_directory=tmp_path, dandiset_directory=tmp_path)

    assert result is False
    assert any("No pending entries in" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_submit_next_returns_false_when_no_eligible_entries(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """_submit_next returns False when all ordered entries already have submitted markers."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    first_entry = _make_state_entry(dandiset_id="000001")
    second_entry = _make_state_entry(dandiset_id="000002", subject="mouse02")
    _write_jsonl(queue_dir / "state.jsonl", [first_entry, second_entry])
    first_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    second_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    (first_attempt_dir / "code" / "submitted").touch()
    (second_attempt_dir / "code" / "submitted").touch()

    with (
        caplog.at_level(logging.INFO, logger="dandi_compute_code.queue._submit_next"),
        mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit,
    ):
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is False
    mock_submit.assert_not_called()
    assert any("No eligible pending entries available for submission" in record.message for record in caplog.records)


@pytest.mark.ai_generated
def test_submit_next_submits_first_pending_entry_in_state_order(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first pending entry in state.jsonl order."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True
    mock_submit.assert_called_once()
    assert "submit.sh" in str(mock_submit.call_args.kwargs["script_file_path"])


@pytest.mark.ai_generated
def test_submit_next_raised_when_script_missing(tmp_path: pathlib.Path) -> None:
    """_submit_next raises FileNotFoundError when the submit.sh for the first entry does not exist."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(dandiset_id="000001", pipeline="test", version="v1.0", params="default")
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    # Deliberately do NOT create the attempt directory / submit.sh

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        with pytest.raises(FileNotFoundError, match="Submit script not found in either location:"):
            _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)


@pytest.mark.ai_generated
def test_submit_next_submits_script_from_dandiset_when_absent_in_datalad(tmp_path: pathlib.Path) -> None:
    """_submit_next falls back to dandiset submit.sh when datalad path is absent."""
    queue_dir = _make_queue_dir(tmp_path)
    datalad_dir = tmp_path / "datalad"
    datalad_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(dandiset_id="000001", pipeline="test", version="v1.0", params="default")
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    expected_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        result = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=datalad_dir,
            dandiset_directory=dandiset_dir,
        )

    assert result is True
    mock_submit.assert_called_once_with(script_file_path=expected_attempt_dir / "code" / "submit.sh")
    assert (expected_attempt_dir / "code" / "submitted").exists()


@pytest.mark.ai_generated
def test_submit_next_writes_marker_next_to_submitted_script_when_dandiset_attempt_is_missing(
    tmp_path: pathlib.Path,
) -> None:
    """_submit_next writes code/submitted beside the script used for submission."""
    queue_dir = _make_queue_dir(tmp_path)
    datalad_dir = tmp_path / "datalad"
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    entry = _make_state_entry(
        dandiset_id="001849",
        subject="test",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+f37df9f",
        params="4af6a25",
        config="0d4bf36_date-2026+05+24",
        attempt=2,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    datalad_attempt_dir = _make_attempt_dir_with_script(
        datalad_dir,
        dandiset_id="001849",
        subject="test",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+f37df9f",
        params="4af6a25",
        config="0d4bf36_date-2026+05+24",
        attempt=2,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=datalad_dir,
            dandiset_directory=dandiset_dir,
        )

    marker_in_datalad = (
        datalad_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+f37df9f_params-4af6a25_config-0d4bf36_date-2026+05+24_attempt-2"
        / "code"
        / "submitted"
    )
    marker_in_dandiset = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+f37df9f_params-4af6a25_config-0d4bf36_date-2026+05+24_attempt-2"
        / "code"
        / "submitted"
    )
    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=datalad_attempt_dir / "code" / "submit.sh")
    assert marker_in_datalad.exists()
    assert marker_in_dandiset.exists() is False


@pytest.mark.ai_generated
def test_submit_next_writes_submitted_marker_using_dandi_path_without_nwb_suffix(tmp_path: pathlib.Path) -> None:
    """_submit_next resolves marker and submit paths under dandi_path with the .nwb suffix removed."""
    queue_dir = _make_queue_dir(tmp_path)
    datalad_dir = tmp_path / "datalad"
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(
        dandiset_id="001849",
        dandi_path="sub-test/sourcedata/aind-sample.nwb",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+f37df9f",
        params="4af6a25",
        config="0d4bf36_date-2026+05+24",
        attempt=2,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    attempt_dir = (
        datalad_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "sourcedata"
        / "aind-sample"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+f37df9f_params-4af6a25_config-0d4bf36_date-2026+05+24_attempt-2"
    )
    (attempt_dir / "code").mkdir(parents=True)
    script_file_path = attempt_dir / "code" / "submit.sh"
    script_file_path.write_text("#!/bin/bash\necho hello\n")

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=datalad_dir,
            dandiset_directory=dandiset_dir,
        )

    marker_dir_in_dandiset = (
        dandiset_dir / "derivatives" / "dandiset-001849" / "sub-test" / "sourcedata" / "aind-sample"
    )
    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=script_file_path)
    assert (attempt_dir / "code" / "submitted").exists()
    assert marker_dir_in_dandiset.exists() is False


@pytest.mark.ai_generated
def test_submit_next_found_flat_attempt_directory_under_scanned_dandi_path(tmp_path: pathlib.Path) -> None:
    """_submit_next can submit from flat attempt layout even when dandi_path points to source data."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(
        dandiset_id="001849",
        dandi_path="sourcedata",
        pipeline="test",
        version="v1.1.1+b268fd2+a66c8df",
        params="4af6a25",
        config="0d4bf36_date-2026+05+21",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    actual_attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-test"
        / "pipeline-test"
        / "version-v1.1.1+b268fd2+a66c8df_params-4af6a25_config-0d4bf36_date-2026+05+21_attempt-1"
    )
    (actual_attempt_dir / "code").mkdir(parents=True)
    script_file_path = actual_attempt_dir / "code" / "submit.sh"
    script_file_path.write_text("#!/bin/bash\necho hello\n")

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True
    mock_submit.assert_called_once_with(script_file_path=script_file_path)


@pytest.mark.ai_generated
def test_submit_next_uses_session_in_path_when_present(tmp_path: pathlib.Path) -> None:
    """_submit_next constructs the correct path when the entry has a session field."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        session="ses01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        session="ses01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True
    assert mock_submit.called


@pytest.mark.ai_generated
def test_submit_next_leaves_state_jsonl_unchanged_after_submission(tmp_path: pathlib.Path) -> None:
    """_submit_next submits without mutating state.jsonl contents."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry1 = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    entry2 = _make_state_entry(
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry1, entry2])

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)

    remaining = _read_jsonl(queue_dir / "state.jsonl")
    assert remaining == [entry1, entry2]


@pytest.mark.ai_generated
def test_submit_next_creates_submitted_marker_file(tmp_path: pathlib.Path) -> None:
    """_submit_next creates a submitted marker next to code/submit.sh."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])
    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)

    assert (attempt_dir / "code" / "submitted").exists()


@pytest.mark.ai_generated
def test_submit_next_submits_top_two_eligible_entries(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first two eligible entries by default."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"

    first_entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    second_entry = _make_state_entry(
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    third_entry = _make_state_entry(
        dandiset_id="000003",
        subject="mouse03",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [first_entry, second_entry, third_entry])
    first_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    second_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    third_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000003",
        subject="mouse03",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        _submit_next(queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir)

    assert mock_submit.call_count == 2
    assert mock_submit.call_args_list == [
        mock.call(script_file_path=first_attempt_dir / "code" / "submit.sh"),
        mock.call(script_file_path=second_attempt_dir / "code" / "submit.sh"),
    ]
    assert (first_attempt_dir / "code" / "submitted").exists()
    assert (second_attempt_dir / "code" / "submitted").exists()
    assert not (third_attempt_dir / "code" / "submitted").exists()


@pytest.mark.ai_generated
def test_submit_next_deduplicates_same_attempt_directory(tmp_path: pathlib.Path) -> None:
    """_submit_next submits a given attempt directory only once even if state has duplicate entries."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    entry = _make_state_entry(dandiset_id="000001")
    _write_jsonl(queue_dir / "state.jsonl", [entry, dict(entry)])
    attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
        )

    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=attempt_dir / "code" / "submit.sh")
    assert (attempt_dir / "code" / "submitted").exists()


@pytest.mark.ai_generated
def test_submit_next_submits_next_entry_when_first_has_submitted_marker(tmp_path: pathlib.Path) -> None:
    """_submit_next skips entries with code/submitted markers and submits the next one."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    first_entry = _make_state_entry(dandiset_id="000001")
    second_entry = _make_state_entry(dandiset_id="000002", subject="mouse02")
    _write_jsonl(queue_dir / "state.jsonl", [first_entry, second_entry])

    first_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    (first_attempt_dir / "code" / "submitted").touch()
    second_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
        )

    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=second_attempt_dir / "code" / "submit.sh")
    remaining = _read_jsonl(queue_dir / "state.jsonl")
    assert remaining == [first_entry, second_entry]


@pytest.mark.ai_generated
def test_submit_next_only_submits_entries_pending_in_state_jsonl(tmp_path: pathlib.Path) -> None:
    """_submit_next only submits entries listed as pending in state.jsonl."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "dandiset"
    _write_jsonl(
        queue_dir / "state.jsonl",
        [
            _make_state_entry(dandiset_id="000001", has_code=True, has_output=True, has_logs=False),
            _make_state_entry(dandiset_id="000002", subject="mouse02", has_code=True, has_output=False, has_logs=False),
        ],
    )
    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    eligible_attempt_dir = _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job") as mock_submit:
        submitted = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
        )

    assert submitted is True
    mock_submit.assert_called_once_with(script_file_path=eligible_attempt_dir / "code" / "submit.sh")
    remaining = _read_jsonl(queue_dir / "state.jsonl")
    assert len(remaining) == 2


@pytest.mark.ai_generated
def test_submit_next_submits_first_entry_directly(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first ordered state entry directly."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"

    # Create 2 failure attempt dirs for the dandiset (== max_fail_per_dandiset=2)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1, with_logs=True)
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 2, with_logs=True)

    # state.jsonl: first entry will be submitted directly.
    _write_jsonl(
        queue_dir / "state.jsonl",
        [
            _make_state_entry(dandiset_id="000001", version="v1.0"),
            _make_state_entry(dandiset_id="000002", version="v1.0"),
        ],
    )

    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        result = _submit_next(
            queue_directory=queue_dir,
            datalad_directory=dandiset_dir,
            dandiset_directory=dandiset_dir,
            max_submissions=1,
        )

    assert result is True


@pytest.mark.ai_generated
def test_submit_next_submits_first_entry_with_existing_failure_dirs(tmp_path: pathlib.Path) -> None:
    """_submit_next submits the first ordered state entry directly even if failure dirs exist."""
    queue_dir = _make_queue_dir(tmp_path)
    dandiset_dir = tmp_path / "001697"

    # Only 1 failure (< max_fail_per_dandiset=2) → entry should be submitted
    _make_attempt_dir(dandiset_dir, "000001", "v1.0", _FAKE_PARAMS_ID, _FAKE_CONFIG_ID, 1, with_logs=True)

    entry = _make_state_entry(
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )
    _write_jsonl(queue_dir / "state.jsonl", [entry])

    # Create the submit script so _submit_next can proceed
    _make_attempt_dir_with_script(
        dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        pipeline="test",
        version="v1.0",
        params="default",
        config="abc123",
        attempt=1,
    )

    with mock.patch("dandi_compute_code.queue._submit_next.submit_job"):
        result = _submit_next(
            queue_directory=queue_dir, datalad_directory=dandiset_dir, dandiset_directory=dandiset_dir
        )

    assert result is True
