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
def test_cli_prepare_test_calls_prepare_queue_with_test_content_id(tmp_path: pathlib.Path) -> None:
    """dandicompute prepare aind --test calls prepare_queue with the known test content ID."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.prepare_queue") as mock_prepare_queue,
    ):
        result = runner.invoke(_dandicompute_group, ["prepare", "aind", "--test", "--queue", str(queue_dir)])

    assert result.exit_code == 0
    mock_prepare_queue.assert_called_once_with(
        queue_directory=queue_dir,
        content_ids=[TEST_QUEUE_CONTENT_ID],
        pipeline_directory=None,
        config_key="default",
    )


@pytest.mark.ai_generated
def test_cli_prepare_test_passes_config_key(tmp_path: pathlib.Path) -> None:
    """dandicompute prepare aind --test forwards --config to prepare_queue."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.prepare_queue") as mock_prepare_queue,
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["prepare", "aind", "--test", "--queue", str(queue_dir), "--config", "mit+engaging+revision-1"],
        )

    assert result.exit_code == 0
    mock_prepare_queue.assert_called_once_with(
        queue_directory=queue_dir,
        content_ids=[TEST_QUEUE_CONTENT_ID],
        pipeline_directory=None,
        config_key="mit+engaging+revision-1",
    )


@pytest.mark.ai_generated
def test_cli_prepare_test_required_queue_directory() -> None:
    """dandicompute prepare aind --test requires --queue."""
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, ["prepare", "aind", "--test"])
    assert result.exit_code != 0
    assert "--queue is required when using --test" in result.output


@pytest.mark.ai_generated
def test_cli_aind_prepare_passes_config_key() -> None:
    """dandicompute prepare aind forwards --config to prepare_aind_ephys_job."""
    runner = CliRunner()

    with (
        mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}),
        mock.patch("dandi_compute_code._cli._dandicompute_group.prepare_aind_ephys_job") as mock_prepare,
    ):
        mock_prepare.return_value = pathlib.Path("/tmp/submit.sh")
        result = runner.invoke(
            _dandicompute_group,
            [
                "prepare",
                "aind",
                "--id",
                "abc123",
                "--version",
                "v1.2.3",
                "--config",
                "mit+engaging+revision-1",
            ],
        )

    assert result.exit_code == 0
    assert mock_prepare.call_args.kwargs["config_key"] == "mit+engaging+revision-1"


@pytest.mark.ai_generated
def test_cli_queue_clean_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute queue clean delegates to clean_unsubmitted_capsules and reports removed paths."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    fake_removed = [dandiset_dir / "derivatives" / "dandiset-000001" / "sub-mouse01" / "attempt-1"]
    runner = CliRunner()

    with mock.patch(
        "dandi_compute_code._cli._dandicompute_group.clean_unsubmitted_capsules", return_value=fake_removed
    ) as mock_clean:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "clean",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
            ],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    mock_clean.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Cleaned 1 unsubmitted capsule" in result.output


@pytest.mark.ai_generated
def test_cli_queue_clean_reports_nothing_found(tmp_path: pathlib.Path) -> None:
    """dandicompute queue clean reports when no unsubmitted capsules are found."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()

    with mock.patch("dandi_compute_code._cli._dandicompute_group.clean_unsubmitted_capsules", return_value=[]):
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "clean",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
            ],
            env={"DANDI_API_KEY": "test-key"},
        )

    assert result.exit_code == 0, result.output
    assert "No unsubmitted capsules found" in result.output


@pytest.mark.ai_generated
def test_cli_queue_stats_calls_helper_and_reports_output(tmp_path: pathlib.Path) -> None:
    """dandicompute queue stats delegates to aggregate_queue_statistics."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch(
        "dandi_compute_code._cli._dandicompute_group.aggregate_queue_statistics",
        return_value={"successful_asset_bytes_total": 0},
    ) as mock_stats:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "stats",
                "--queue",
                str(queue_dir),
                "--dandiset",
                str(dandiset_dir),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_stats.assert_called_once_with(
        queue_directory=queue_dir,
        dandiset_directory=dandiset_dir,
        output_file_name="queue_stats.json",
    )
    assert "Wrote queue aggregate statistics" in result.output


@pytest.mark.ai_generated
def test_cli_issues_dump_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute issues dump delegates to dump_issues and reports output."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch("dandi_compute_code._cli._dandicompute_group.dump_issues", return_value=[]) as mock_dump:
        result = runner.invoke(
            _dandicompute_group,
            [
                "issues",
                "dump",
                "--directory",
                str(dandiset_dir),
                "--queue",
                str(queue_dir),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_dump.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Wrote issue dump" in result.output


@pytest.mark.ai_generated
def test_cli_issues_summarize_calls_helper(tmp_path: pathlib.Path) -> None:
    """dandicompute issues summarize delegates to summarize_issues and reports output."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch("dandi_compute_code._cli._dandicompute_group.summarize_issues", return_value={}) as mock_summarize:
        result = runner.invoke(
            _dandicompute_group,
            [
                "issues",
                "summarize",
                "--directory",
                str(dandiset_dir),
                "--queue",
                str(queue_dir),
            ],
        )

    assert result.exit_code == 0, result.output
    mock_summarize.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Wrote issue summary" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "subcommand",
    [
        "clean",
        "stats",
    ],
)
def test_cli_queue_subcommands_required_queue_directory(tmp_path: pathlib.Path, subcommand: str) -> None:
    """Queue clean/stats commands require --queue."""
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()
    args = ["queue", subcommand, "--dandiset", str(dandiset_dir)]
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, args)
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_required_queue_directory(tmp_path: pathlib.Path) -> None:
    """Queue process command requires --queue."""
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--processing", str(processing_dir)],
        )
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output


@pytest.mark.ai_generated
def test_cli_queue_prepare_required_queue_directory() -> None:
    """Queue prepare command requires --queue."""
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(_dandicompute_group, ["queue", "prepare"])
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_requires_processing_directory(tmp_path: pathlib.Path) -> None:
    """Queue process command requires --processing."""
    queue_dir = _make_queue_dir(tmp_path)
    runner = CliRunner()
    with mock.patch.dict("os.environ", {"DANDI_API_KEY": "test-key"}):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "process", "--queue", str(queue_dir)],
        )
    assert result.exit_code != 0
    assert "Missing option '--processing'" in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_passes_processing_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue process forwards --processing to process_queue."""
    queue_dir = _make_queue_dir(tmp_path)
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch("dandi_compute_code._cli._dandicompute_group.process_queue") as mock_process:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "process",
                "--queue",
                str(queue_dir),
                "--processing",
                str(processing_dir),
            ],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    mock_process.assert_called_once_with(
        queue_directory=queue_dir,
        processing_directory=processing_dir,
        test=False,
    )


@pytest.mark.ai_generated
def test_cli_queue_process_passes_test_flag(tmp_path: pathlib.Path) -> None:
    """dandicompute queue process forwards --test to process_queue."""
    queue_dir = _make_queue_dir(tmp_path)
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch("dandi_compute_code._cli._dandicompute_group.process_queue") as mock_process:
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "process",
                "--queue",
                str(queue_dir),
                "--processing",
                str(processing_dir),
                "--test",
            ],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    mock_process.assert_called_once_with(
        queue_directory=queue_dir,
        processing_directory=processing_dir,
        test=True,
    )


@pytest.mark.ai_generated
def test_cli_queue_process_reports_when_no_jobs_waiting(tmp_path: pathlib.Path) -> None:
    """dandicompute queue process reports when no jobs are waiting for submission."""
    queue_dir = _make_queue_dir(tmp_path)
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    with mock.patch("dandi_compute_code._cli._dandicompute_group.process_queue", return_value="no-pending"):
        result = runner.invoke(
            _dandicompute_group,
            [
                "queue",
                "process",
                "--queue",
                str(queue_dir),
                "--processing",
                str(processing_dir),
            ],
            env={"DANDI_API_KEY": "test-key", "DANDI_DEVEL": "1"},
        )

    assert result.exit_code == 0, result.output
    assert "No jobs were found waiting to be submitted." in result.output


@pytest.mark.ai_generated
def test_cli_queue_process_requires_dandi_devel(tmp_path: pathlib.Path) -> None:
    """Queue process command exits non-zero when DANDI_DEVEL is not set."""
    queue_dir = _make_queue_dir(tmp_path)
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        _dandicompute_group,
        [
            "queue",
            "process",
            "--queue",
            str(queue_dir),
            "--processing",
            str(processing_dir),
        ],
        env={"DANDI_API_KEY": "test-key"},
    )

    assert result.exit_code != 0
    assert "DANDI_DEVEL" in result.output
