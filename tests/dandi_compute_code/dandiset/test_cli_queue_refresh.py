# ruff: noqa: F821
import importlib.util as _importlib_util
import pathlib as _pathlib

_spec = _importlib_util.spec_from_file_location(
    "_write_queue_state_test_cases",
    _pathlib.Path(__file__).with_name("_write_queue_state_test_cases.py"),
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
def test_cli_queue_refresh_with_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh writes state.jsonl from assets metadata."""
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
    runner = CliRunner()
    with mock.patch(
        "dandi_compute_code.queue._write_queue_state._load_assets_jsonld_metadata",
        return_value=_build_assets_metadata(
            content_id=content_id,
            asset_path="sub-mouse01/sub-mouse01_ecephys.nwb",
            content_size=1234,
        ),
    ):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir)],
            env={"DANDI_API_KEY": "test-key"},
        )
    assert result.exit_code == 0, result.output
    assert (queue_dir / "state.jsonl").exists()
    state_records = [json.loads(line) for line in (queue_dir / "state.jsonl").read_text().splitlines() if line.strip()]
    assert len(state_records) == 1
    assert state_records[0]["dandiset_id"] == "001697"
    assert state_records[0]["content_id"] == content_id


@pytest.mark.ai_generated
def test_cli_queue_refresh_does_not_require_dandiset_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh runs without --dandiset."""
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
    assert result.exit_code == 0, result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize("dandi_api_key", [None, ""])
def test_cli_queue_refresh_does_not_require_dandi_api_key(tmp_path: pathlib.Path, dandi_api_key: str | None) -> None:
    """dandicompute queue refresh runs when DANDI_API_KEY is missing."""
    queue_dir = tmp_path / "queue_directory"
    queue_dir.mkdir()
    (queue_dir / "queue_config.json").write_text(json.dumps({"pipelines": {}}))

    runner = CliRunner()
    with mock.patch.dict("os.environ", {}, clear=True):
        result = runner.invoke(
            _dandicompute_group,
            ["queue", "refresh", "--queue", str(queue_dir)],
            env={} if dandi_api_key is None else {"DANDI_API_KEY": dandi_api_key},
        )
    assert result.exit_code == 0, result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_fails_without_queue_config(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh fails when queue_config.json is missing."""
    queue_dir = tmp_path / "queue_directory"
    queue_dir.mkdir()
    runner = CliRunner()
    result = runner.invoke(
        _dandicompute_group,
        ["queue", "refresh", "--queue", str(queue_dir)],
        env={"DANDI_API_KEY": "test-key"},
    )
    assert result.exit_code != 0
    assert "queue_config.json" in result.output


@pytest.mark.ai_generated
def test_cli_queue_refresh_required_queue_directory(tmp_path: pathlib.Path) -> None:
    """dandicompute queue refresh requires --queue."""
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, ["queue", "refresh"])
    assert result.exit_code != 0
    assert "Missing option '--queue'" in result.output
