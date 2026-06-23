import json
import pathlib

import pytest

from dandi_compute_code.queue import QueueState
from model.testing_utilities import write_attempt_logs


@pytest.mark.ai_generated
def test_dump_issues_writes_per_capsule_records(tmp_path: pathlib.Path) -> None:
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    dandiset_dir = tmp_path / "dandiset"
    dandiset_dir.mkdir()

    write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        attempt=1,
        nextflow_lines=["INFO start", "ERROR ~ Process failed"],
        slurm_lines_by_file={"job-123_slurm.log": ["slurm ok", "srun: error: node failure"]},
    )
    write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000001",
        subject="mouse02",
        attempt=1,
        nextflow_lines=["INFO only"],
        slurm_lines_by_file={"job-456_slurm.log": ["all good"]},
    )

    records = QueueState.dump_issues(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
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

    write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000001",
        subject="mouse01",
        attempt=1,
        nextflow_lines=["error: common failure", "error: unique failure"],
        slurm_lines_by_file={"job-001_slurm.log": ["error: common failure"]},
    )
    write_attempt_logs(
        dandiset_directory=dandiset_dir,
        dandiset_id="000002",
        subject="mouse02",
        attempt=1,
        nextflow_lines=["error: common failure"],
        slurm_lines_by_file={"job-002_slurm.log": ["done"]},
    )

    summary = QueueState.summarize_issues(dandiset_directory=dandiset_dir, queue_directory=queue_dir)
    summary_payload = json.loads((queue_dir / "issues_summary.json").read_text())

    assert summary == {"3": ["error: common failure"], "1": ["error: unique failure"]}
    assert summary_payload["summary"] == {"3": ["error: common failure"], "1": ["error: unique failure"]}
