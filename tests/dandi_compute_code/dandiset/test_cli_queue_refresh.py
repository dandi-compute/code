# ruff: noqa: F821
from . import _scan_dandiset_test_cases as _support

globals().update(
    {
        name: value
        for name, value in vars(_support).items()
        if not name.startswith("__") and not name.startswith("test_")
    }
)


@pytest.mark.ai_generated
def test_cli_queue_refresh_with_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh --dandiset scans and writes state.jsonl."""
    dandiset_dir = tmp_path / "dandiset"
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0+abc1234+def5678"],
                        "params_priority": ["default"],
                        "max_fail_per_dandiset": 3,
                    }
                }
            }
        )
    )
    content_id = "048d1ee9-83b7-491f-8f02-1ca615b1d455"
    _make_attempt_dir(
        dandiset_dir,
        "000001",
        "mouse01",
        "aind+ephys",
        "v1.0",
        "abc1234",
        "def5678",
        1,
        content_id=content_id,
    )
    runner = CliRunner()
    with mock.patch(
        "dandi_compute_code.dandiset._lookup_asset_size_bytes._load_content_id_to_unique_dandiset_path",
        return_value=_build_mapping_for_content_id(
            content_id=content_id,
            dandiset_id="000001",
            asset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
        ),
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
            env={"DANDI_API_KEY": "test-key"},
        )
    assert result.exit_code == 0, result.output
    assert (queue_dir / "state.jsonl").exists()
    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["dandiset_id"] == "000001"
    assert state_records[0]["content_id"] == content_id


@pytest.mark.ai_generated
def test_cli_queue_refresh_requires_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh requires --dandiset."""
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    entry = {
        "dandiset_id": "000001",
        "dandi_path": "sub-mouse01",
        "pipeline": "aind+ephys",
        "version": "v1.0",
        "params": "abc1234",
        "config": "def5678",
        "attempt": 1,
        "has_code": True,
        "has_output": False,
        "has_logs": False,
        "created_at": "2024-01-01T00:00:00+00:00",
    }
    (queue_dir / "state.jsonl").write_text(json.dumps(entry) + "\n")
    (queue_dir / "queue_config.json").write_text(
        json.dumps(
            {
                "pipelines": {
                    "aind+ephys": {
                        "version_priority": ["v1.0+abc1234+def5678"],
                        "params_priority": ["default"],
                        "max_fail_per_dandiset": 3,
                    }
                }
            }
        )
    )
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "refresh", "--queue", str(queue_dir)],
    )
    assert result.exit_code != 0
    assert "Missing option '--dandiset'" in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize("dandi_api_key", [None, ""])
def test_cli_queue_refresh_requires_dandi_api_key(tmp_path: pathlib.Path, dandi_api_key: str | None) -> None:
    """dandicompute queue refresh fails immediately when DANDI_API_KEY is missing."""
    dandiset_dir = tmp_path / "dandiset_directory"
    dandiset_dir.mkdir()
    queue_dir = tmp_path / "queue_directory"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))

    runner = CliRunner()
    with mock.patch.dict("os.environ", {}, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
            env={} if dandi_api_key is None else {"DANDI_API_KEY": dandi_api_key},
        )
    assert result.exit_code != 0
    assert "DANDI_API_KEY" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_fails_without_queue_config(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh --dandiset fails when queue_config.json is missing."""
    dandiset_dir = tmp_path / "dandiset_directory"
    dandiset_dir.mkdir()
    queue_dir = tmp_path / "queue_directory"
    queue_dir.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "refresh", "--queue", str(queue_dir), "--dandiset", str(dandiset_dir)],
        env={"DANDI_API_KEY": "test-key"},
    )
    assert result.exit_code != 0
    assert "queue_config.json" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_required_queue_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh requires --queue."""
    dandiset_dir = tmp_path / "dandiset_directory"
    dandiset_dir.mkdir()
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["queue", "refresh", "--dandiset", str(dandiset_dir)])
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output
