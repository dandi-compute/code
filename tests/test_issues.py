import json
import pathlib
from unittest import mock

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group
from dandi_compute_code.queue import dump_issues, summarize_issues


def _write_attempt_logs(
    *,
    dandiset_directory: pathlib.Path,
    dandiset_id: str,
    subject: str,
    attempt: int,
    nextflow_lines: list[str],
    slurm_file_to_lines: dict[str, list[str]],
) -> pathlib.Path:
    logs_dir = (
        dandiset_directory
        / "derivatives"
        / f"dandiset-{dandiset_id}"
        / f"sub-{subject}"
        / "pipeline-test"
        / f"version-v1.0_params-default_config-abc123_attempt-{attempt}"
        / "logs"
    )
    logs_dir.mkdir(parents=True)
    (logs_dir / "nextflow.log").write_text("\n".join(nextflow_lines) + "\n")
    for file_name, lines in slurm_file_to_lines.items():
        (logs_dir / file_name).write_text("\n".join(lines) + "\n")
    return logs_dir


@pytest.mark.ai_generated
def test_dump_issues_writes_per_capsule_records(tmp_path: pathlib.Path) -> None:
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    _write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        attempt=1,
        nextflow_lines=["INFO start", "ERROR ~ Process failed"],
        slurm_file_to_lines={"job-123_slurm.log": ["slurm ok", "srun: error: node failure"]},
    )
    _write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000001",
        subject="mouse02",
        attempt=1,
        nextflow_lines=["INFO only"],
        slurm_file_to_lines={"job-456_slurm.log": ["all good"]},
    )

    records = dump_issues(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    dump_payload = json.loads((queue_dir / "issues_dump.json").read_text())

    assert len(records) == 1
    assert dump_payload["capsule_count"] == 1
    assert dump_payload["records"][0]["capsule_path"].endswith("_attempt-1")
    assert dump_payload["records"][0]["nextflow_errors"] == ["ERROR ~ Process failed"]
    assert dump_payload["records"][0]["slurm_errors"] == {"job-123_slurm.log": ["srun: error: node failure"]}


@pytest.mark.ai_generated
def test_summarize_issues_writes_descending_frequency(tmp_path: pathlib.Path) -> None:
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    _write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        attempt=1,
        nextflow_lines=["error: common failure", "error: unique failure"],
        slurm_file_to_lines={"job-001_slurm.log": ["error: common failure"]},
    )
    _write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        attempt=1,
        nextflow_lines=["error: common failure"],
        slurm_file_to_lines={"job-002_slurm.log": ["done"]},
    )

    summary = summarize_issues(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    summary_payload = json.loads((queue_dir / "issues_summary.json").read_text())

    assert summary == {"3": ["error: common failure"], "1": ["error: unique failure"]}
    assert summary_payload["summary"] == {"1": ["error: unique failure"], "3": ["error: common failure"]}


@pytest.mark.ai_generated
def test_cli_issues_dump_calls_helper(tmp_path: pathlib.Path) -> None:
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch("dandi_compute_code._cli.dump_issues", return_value=[] ) as mock_dump:
        result = runner.invoke(
            _dandicompute_group,
            ["issues", "dump", "--directory", str(dandiset_dir), "--queue", str(queue_dir)],
        )

    assert result.exit_code == 0, result.output
    mock_dump.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Wrote issue dump" in result.output


@pytest.mark.ai_generated
def test_cli_issues_summarize_calls_helper(tmp_path: pathlib.Path) -> None:
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    runner = CliRunner()
    with mock.patch("dandi_compute_code._cli.summarize_issues", return_value={}) as mock_summarize:
        result = runner.invoke(
            _dandicompute_group,
            ["issues", "summarize", "--directory", str(dandiset_dir), "--queue", str(queue_dir)],
        )

    assert result.exit_code == 0, result.output
    mock_summarize.assert_called_once_with(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    assert "Wrote issue summary" in result.output
