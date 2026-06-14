"""CLI help text tests for top-level groups and commands."""

import pytest
from click.testing import CliRunner

from dandi_compute_code._cli import _dandicompute_group


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("args", "expected_text"),
    [
        (["--help"], "Run compute workflows and queue management tasks for DANDI assets."),
        (["prepare", "--help"], "Run preparation workflows that generate queue entries or scripts."),
        (["prepare", "aind", "--help"], "Prepare an AIND ephys job, or prepare test queue entries with --test."),
        (["submit", "--help"], "Submit a previously prepared pipeline script via sbatch."),
        (["queue", "--help"], "Manage queue ordering, preparation, and execution."),
        (["queue", "refresh", "--help"], "Regenerate state.jsonl from DANDI assets metadata."),
        (["queue", "clean", "--help"], "Delete unsubmitted capsules that are no longer present in the queue."),
        (["queue", "stats", "--help"], "Write aggregate queue statistics from state.jsonl and timeline reports."),
        (["queue", "pending", "--help"], "Report whether any queued jobs are awaiting submission."),
        (["issues", "--help"], "Scan logs and write per-capsule and aggregate issue reports."),
        (["issues", "dump", "--help"], "Scan nextflow and slurm logs and write per-capsule issue records."),
        (["issues", "summarize", "--help"], "Summarize discovered issue lines by descending occurrence count."),
        (["delete", "--help"], "Delete remote and local derivatives for specific version patterns."),
    ],
)
def test_cli_help_includes_descriptions(args: list[str], expected_text: str) -> None:
    """CLI help output includes the configured command/group description text."""
    runner = CliRunner()
    result = runner.invoke(_dandicompute_group, args)

    assert result.exit_code == 0
    assert expected_text in result.output
