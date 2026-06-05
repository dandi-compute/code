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


def _write_state_entries(queue_dir: pathlib.Path, entries: list[dict]) -> None:
    state_file = queue_dir / "state.jsonl"
    if entries:
        _write_jsonl(state_file, entries)
    else:
        state_file.write_text("")


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_raises_without_dandi_api_key(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules raises RuntimeError when DANDI_API_KEY is not set."""
    env_without_key = {k: v for k, v in os.environ.items() if k != "DANDI_API_KEY"}
    with mock.patch.dict(os.environ, env_without_key, clear=True):
        with pytest.raises(RuntimeError, match="DANDI_API_KEY"):
            clean_unsubmitted_capsules(dandiset_directory=tmp_path, queue_directory=tmp_path)


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_queued_directories(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes capsule dirs that are queued (code, no logs, no output)."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=False,
            )
        ],
    )

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    mock_run.assert_called_once_with(
        ["dandi", "delete", str(queued_dir)],
        input=b"y\n",
        check=True,
    )


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_output(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules does not remove capsules that already have output."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    completed_dir = _make_full_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
        with_output=True,
    )
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=True,
                has_logs=False,
            )
        ],
    )

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []
    assert completed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_logs(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules does not remove capsules that have logs (already run)."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    failed_dir = _make_full_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        with_code=True,
        with_logs=True,
    )
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=True,
            )
        ],
    )

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []
    assert failed_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_ignores_dataset_description_in_logs(
    tmp_path: pathlib.Path,
) -> None:
    """clean_unsubmitted_capsules removes a capsule whose logs/ dir contains only dataset_description.json.

    When prepare_job uploads an empty logs/ directory to DANDI, a dataset_description.json
    metadata file may be added inside it. That file must not be treated as evidence of
    actual job logs. The capsule should still qualify for cleaning.
    """
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    # Simulate a logs/ directory that contains only a dataset_description.json metadata file.
    logs_dir = queued_dir / "logs"
    logs_dir.mkdir()
    (logs_dir / "dataset_description.json").write_text("{}\n")
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=False,
            )
        ],
    )

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_skips_entries_with_submitted_marker(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules does not remove capsules with a code/submitted marker."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=False,
            )
        ],
    )

    (queued_dir / "code" / "submitted").touch()

    with mock.patch("subprocess.run") as mock_run, mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []
    assert queued_dir.exists()
    mock_run.assert_not_called()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_returns_empty_list_when_nothing_queued(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules returns an empty list when there are no queued capsules."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    _write_state_entries(queue_dir, [])

    with mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == []


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_handles_session_in_path(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules correctly handles attempt dirs with a session component."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        session="ses001",
        with_code=True,
    )
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                session="ses001",
                has_code=True,
                has_output=False,
                has_logs=False,
            )
        ],
    )

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_empty_parent_directories(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes empty pipeline/version dirs after last capsule removal."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir,
        "001371",
        "S25",
        "aind+ephys",
        "v1.1.1+b268fd2",
        "abc1234",
        "def5678",
        1,
        session="S25-210913",
        with_code=True,
    )
    version_dir = queued_dir.parent
    pipeline_dir = version_dir.parent
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                dandiset_id="001371",
                dandi_path="sub-S25/ses-S25-210913",
                pipeline="aind+ephys",
                version="v1.1.1+b268fd2",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=False,
            )
        ],
    )

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not version_dir.exists()
    assert not pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_keeps_non_empty_parent_directories(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules keeps pipeline/version dirs when a sibling attempt remains."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    remaining_dir = _make_full_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        2,
        with_code=True,
        with_output=True,
    )
    version_dir = queued_dir.parent
    pipeline_dir = version_dir.parent
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                attempt=1,
                has_code=True,
                has_output=False,
                has_logs=False,
            ),
            _make_state_entry(
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                attempt=2,
                has_code=True,
                has_output=True,
                has_logs=False,
            ),
        ],
    )

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert remaining_dir.exists()
    assert version_dir.exists()
    assert pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_legacy_nested_layout(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes queued capsules in the legacy nested layout."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    queued_dir = _make_full_attempt_dir_legacy_nested(
        base=dandiset_dir,
        dandiset_id="001371",
        subject="S25",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2",
        params="abc1234",
        config="def5678",
        attempt=1,
        session="S25-210913",
        with_code=True,
    )
    version_dir = queued_dir.parent
    pipeline_dir = version_dir.parent
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                dandiset_id="001371",
                dandi_path="sub-S25/ses-S25-210913",
                pipeline="aind+ephys",
                version="v1.1.1+b268fd2",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=False,
            )
        ],
    )

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert not version_dir.exists()
    assert not pipeline_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removes_only_queued_not_submitted(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules only removes queued capsules, leaving submitted ones intact."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()

    # Queued (should be removed)
    queued_dir = _make_full_attempt_dir(
        dandiset_dir, "000001", "mouse01", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    # Submitted marker exists – should be kept
    submitted_dir = _make_full_attempt_dir(
        dandiset_dir, "000002", "mouse02", "aind+ephys", "v1.0", "abc1234", "def5678", 1, with_code=True
    )
    (submitted_dir / "code" / "submitted").touch()
    _write_state_entries(
        queue_dir,
        [
            _make_state_entry(
                dandiset_id="000001",
                subject="mouse01",
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=False,
            ),
            _make_state_entry(
                dandiset_id="000002",
                subject="mouse02",
                pipeline="aind+ephys",
                version="v1.0",
                params="abc1234",
                config="def5678",
                has_code=True,
                has_output=False,
                has_logs=False,
            ),
        ],
    )

    with mock.patch("subprocess.run"), mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [queued_dir]
    assert not queued_dir.exists()
    assert submitted_dir.exists()


@pytest.mark.ai_generated
def test_clean_unsubmitted_capsules_removed_entry_via_fallback_attempt_resolution(tmp_path: pathlib.Path) -> None:
    """clean_unsubmitted_capsules removes queued entry when dandi_path differs from on-disk attempt path."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    attempt_dir = (
        dandiset_dir
        / "derivatives"
        / "dandiset-001849"
        / "sub-mouse01"
        / "pipeline-aind+ephys"
        / "version-v1.1.1+b268fd2+a66c8df_codebase-v0.3.0_params-4af6a25_config-0d4bf36_attempt-1"
    )
    (attempt_dir / "code").mkdir(parents=True)
    (attempt_dir / "code" / "submit.sh").write_text("#!/bin/bash\necho hello\n")
    state_entry = _make_state_entry(
        dandiset_id="001849",
        dandi_path="sourcedata",
        pipeline="aind+ephys",
        version="v1.1.1+b268fd2+a66c8df",
        params="4af6a25",
        config="0d4bf36",
        has_code=True,
        has_logs=False,
        has_output=False,
    )

    _write_state_entries(queue_dir, [state_entry])
    with (
        mock.patch("subprocess.run") as mock_run,
        mock.patch.dict(os.environ, {"DANDI_API_KEY": "test-key"}),
    ):
        removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_dir, queue_directory=queue_dir)

    assert removed == [attempt_dir]
    assert not attempt_dir.exists()
    mock_run.assert_called_once_with(["dandi", "delete", str(attempt_dir)], input=b"y\n", check=True)
