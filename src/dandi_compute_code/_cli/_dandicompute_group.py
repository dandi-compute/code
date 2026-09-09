import logging
import os
import pathlib

import click

from ._clean_work_directory import clean_work_directory
from ._styled_echo import _styled_echo
from .._configure_logging import _configure_logging
from ..aind_ephys_pipeline import prepare_aind_ephys_job, submit_job
from ..dandiset import (
    delete_dandiset_version,
    move_job_capsule,
    scan_version_directories,
)
from ..dandiset._globals import _FAILED_RUNS_ARCHIVE_DANDISET_ID, _JOB_CAPSULES_DANDISET_ID
from ..queue import TEST_QUEUE_CONTENT_ID, QueueState

logging.basicConfig(level=logging.INFO)


def _require_dandi_api_key() -> None:
    """
    Verify that the ``DANDI_API_KEY`` environment variable is set and non-empty.

    Raises
    ------
    click.ClickException
        If ``DANDI_API_KEY`` is unset or empty.  The CLI catches this and exits
        with a user-facing error message.
    """
    if "DANDI_API_KEY" not in os.environ or not os.environ["DANDI_API_KEY"]:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")


def _require_dandi_devel() -> None:
    """
    Verify that the ``DANDI_DEVEL`` environment variable is set and non-empty.

    Raises
    ------
    click.ClickException
        If ``DANDI_DEVEL`` is unset or empty.
    """
    if "DANDI_DEVEL" not in os.environ or not os.environ["DANDI_DEVEL"]:
        raise click.ClickException("`DANDI_DEVEL` environment variable is not set.")


# dandicompute
@click.group(name="dandicompute")
def _dandicompute_group():
    """Run compute workflows and queue management tasks for DANDI assets."""
    pass


# dandicompute clean [OPTIONS]
@_dandicompute_group.command(name="clean")
@click.option(
    "--directory",
    "directory",
    help="Path to the directory to clean (all contents except 'apptainer_cache' will be deleted).",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _clean_command(directory: pathlib.Path, silent: bool = False) -> None:
    """Remove all files and directories under a work directory except apptainer cache."""
    _configure_logging(silent=silent)
    clean_work_directory(directory=directory)
    if not silent:
        _styled_echo(text="\nWork directory cleaned!", color="green")


# dandicompute submit [OPTIONS]
@_dandicompute_group.command(name="submit")
@click.option(
    "--script",
    "script_file_path",
    help="Path to the submission script file.",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _submit_command(script_file_path: pathlib.Path, silent: bool = False) -> None:
    """Submit a previously prepared pipeline script via sbatch."""
    _configure_logging(silent=silent)
    submit_job(script_file_path=script_file_path)


# dandicompute prepare
@_dandicompute_group.group(name="prepare")
def _prepare_group() -> None:
    """Run preparation workflows that generate queue entries or scripts."""
    pass


# dandicompute prepare aind [OPTIONS]
@_prepare_group.command(name="aind")
@click.option(
    "--test",
    "test",
    help="Prepare test queue entries for all configured AIND ephys version/params combinations.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--id",
    "content_id",
    help="The content ID for the data to be processed. Required if --dandiset and --dandipath are not provided.",
    required=False,
    type=None,
)
@click.option(
    "--dandiset",
    "dandiset_id",
    help="The Dandiset ID for the data to be processed (e.g., '000409'). Required if --id is not provided.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--dandipath",
    "dandiset_path",
    help="The local path to the Dandiset data to be processed. Required if --id is not provided (ignored with --test).",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--config",
    "config_key",
    help="Registered configuration key to use.",
    required=False,
    type=str,
    default="default",
)
@click.option(
    "--pipeline",
    "pipeline_directory",
    help="Local path to the AIND pipeline repository.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--version",
    "pipeline_version",
    help="The version of the pipeline to use (ignored with --test).",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--params",
    "parameters_key",
    help="The name of the parameters to use (ignored with --test).",
    required=False,
    type=str,
    default="default",
)
@click.option(
    "--submit",
    help="Automatically submit the job after preparation (ignored with --test).",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress output messages (ignored with --test).",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory containing queue_config.json (only used with --test).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
def _prepare_aind_command(
    test: bool = False,
    pipeline_version: str | None = None,
    content_id: str | None = None,
    dandiset_id: str | None = None,
    dandiset_path: pathlib.Path | None = None,
    config_key: str = "default",
    pipeline_directory: pathlib.Path | None = None,
    parameters_key: str = "default",
    submit: bool = False,
    silent: bool = False,
    queue_directory: pathlib.Path | None = None,
) -> None:
    """Prepare an AIND ephys job, or prepare test queue entries with --test."""
    _configure_logging(silent=silent)
    if "DANDI_API_KEY" not in os.environ:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")

    if test:
        if queue_directory is None:
            raise click.UsageError("--queue is required when using --test.")

        QueueState.prepare(
            queue_directory=queue_directory,
            content_ids=[TEST_QUEUE_CONTENT_ID],
            pipeline_directory=pipeline_directory,
            config_key=config_key,
        )
        return

    if pipeline_version is None:
        raise click.UsageError("--version is required when not using --test.")

    script_file_path = prepare_aind_ephys_job(
        content_id=content_id,
        dandiset_id=dandiset_id,
        dandiset_path=dandiset_path,
        config_key=config_key,
        pipeline_directory=pipeline_directory,
        pipeline_version=pipeline_version,
        parameters_key=parameters_key,
        silent=silent,
    )

    if submit:
        submit_job(script_file_path=script_file_path)

    if silent:
        return

    _styled_echo(text="\nPreparation complete!", color="green")

    if submit and not silent:
        _styled_echo(text=f"\n\nProcessing script at: {script_file_path}\n\n", color="yellow")
        return

    _styled_echo(
        text=f"\n\nTo submit the job, run:\n\n\tdandicompute submit --script {script_file_path}\n\n",
        color="yellow",
    )


# dandicompute queue
@_dandicompute_group.group(name="queue")
def _queue_group() -> None:
    """Manage queue ordering, preparation, and execution."""
    pass


# dandicompute queue refresh [OPTIONS]
@_queue_group.command(name="refresh")
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Source (job capsules) Dandiset ID whose state is refreshed.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--archive-dandiset-id",
    "archive_dandiset_id",
    help="Archived (failed runs archive) Dandiset ID whose state is refreshed.",
    required=False,
    type=str,
    default=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--processing",
    "processing_directory",
    help="Directory for the temporary working tree used to publish each state.tsv "
    "(defaults to the system temporary location).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--test",
    "test",
    help="Preserve the temporary working tree used to publish each state.tsv instead of cleaning it up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _queue_refresh_command(
    queue_directory: pathlib.Path,
    dandiset_id: str,
    archive_dandiset_id: str,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
    silent: bool = False,
) -> None:
    """
    Regenerate state.tsv/archive_state.tsv and republish state.tsv.

    Writes state.tsv and archive_state.tsv under --queue (see QueueState.write_state /
    write_archive_state), then ephemerally rebuilds and republishes derivatives/state.tsv within
    both the source and archived Dandisets themselves, so each always reflects its current state.
    """
    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    try:
        QueueState.write_state(queue_directory=queue_directory, dandiset_id=dandiset_id)
        QueueState.write_state(
            queue_directory=queue_directory,
            dandiset_id=archive_dandiset_id,
            state_file_name="archive_state.tsv",
        )
    except FileNotFoundError as error:
        raise click.ClickException(str(error)) from error

    for target_dandiset_id in (dandiset_id, archive_dandiset_id):
        QueueState.write_dandiset_state_table(
            dandiset_id=target_dandiset_id,
            processing_directory=processing_directory,
            test=test,
        )
        if not silent:
            _styled_echo(text=f"\nPublished derivatives/state.tsv to Dandiset {target_dandiset_id}.", color="green")


# dandicompute queue clean [OPTIONS]
@_queue_group.command(name="clean")
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--dandiset",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan for queued capsules.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _queue_clean_command(
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    silent: bool = False,
) -> None:
    """Delete unsubmitted capsules that are no longer present in the queue."""
    _configure_logging(silent=silent)
    _require_dandi_api_key()

    state = QueueState.from_tsv(queue_directory / "state.tsv")
    removed = state.clean_unsubmitted_capsules(dandiset_directory=dandiset_directory)
    if removed:
        if not silent:
            for path in removed:
                _styled_echo(text=f"  Removed: {path}", color="yellow")
            noun = "capsule" if len(removed) == 1 else "capsules"
            _styled_echo(text=f"\nCleaned {len(removed)} unsubmitted {noun}.", color="green")
    elif not silent:
        _styled_echo(text="\nNo unsubmitted capsules found.", color="yellow")


# dandicompute queue stats [OPTIONS]
@_queue_group.command(name="stats")
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--dandiset",
    "dandiset_directory",
    help="Path to a local dandiset clone used to locate Nextflow timeline reports.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--output-file",
    "output_file_name",
    help="Name of the aggregate statistics JSON file written under --queue.",
    required=False,
    type=str,
    default="queue_stats.json",
    show_default=True,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _queue_stats_command(
    queue_directory: pathlib.Path,
    dandiset_directory: pathlib.Path,
    output_file_name: str = "queue_stats.json",
    silent: bool = False,
) -> None:
    """Write aggregate queue statistics from state.tsv and timeline reports."""
    _configure_logging(silent=silent)

    state = QueueState.from_tsv(queue_directory / "state.tsv")
    state.aggregate_statistics(
        queue_directory=queue_directory,
        dandiset_directory=dandiset_directory,
        output_file_name=output_file_name,
    )
    if not silent:
        _styled_echo(text=f"\nWrote queue aggregate statistics: {queue_directory / output_file_name}", color="green")


# dandicompute queue pending [OPTIONS]
@_queue_group.command(name="pending")
@click.option(
    "--silent",
    help="Suppress informational log output and the printed result.",
    required=False,
    is_flag=True,
    default=False,
)
@click.pass_context
def _queue_pending_command(context: click.Context, silent: bool = False) -> None:
    """Report whether any queued jobs are awaiting submission.

    Prints ``true`` and exits with code 0 when at least one job is pending.
    Prints ``false`` and exits with code 1 when nothing is pending. This lets a
    crontab skip the dispatch entirely when there is no work, for example:

        dandicompute queue pending --silent && dandicompute queue process ...
    """
    _configure_logging(silent=silent)
    pending = QueueState.has_pending_jobs()
    if not silent:
        _styled_echo(text="true" if pending else "false", color="green" if pending else "yellow")
    context.exit(0 if pending else 1)


# dandicompute queue process [OPTIONS]
@_queue_group.command(name="process")
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--processing",
    "processing_directory",
    help="Path to the directory used for temporary working trees during job submission.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--max",
    "max_concurrent_aind_jobs",
    help="Maximum number of AIND jobs allowed to be running before submission is skipped.",
    required=False,
    type=click.IntRange(min=1),
    default=2,
    show_default=True,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--test",
    "test",
    help="Preserve temporary processing directories instead of cleaning them up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--jitter",
    "jitter_seconds",
    help="Maximum seconds of random sleep applied before processing to spread concurrent invocations.",
    required=False,
    type=click.FloatRange(min=0),
    default=30.0,
    show_default=True,
)
def _queue_process_command(
    queue_directory: pathlib.Path,
    processing_directory: pathlib.Path,
    max_concurrent_aind_jobs: int = 2,
    silent: bool = False,
    test: bool = False,
    jitter_seconds: float = 30.0,
) -> None:
    """Submit queued jobs when no active dandicompute jobs are running."""
    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    queue_status = QueueState.process_queue(
        queue_directory=queue_directory,
        processing_directory=processing_directory,
        max_concurrent_aind_jobs=max_concurrent_aind_jobs,
        jitter_seconds=jitter_seconds,
        test=test,
    )
    if not silent and queue_status == "no-pending":
        _styled_echo(text="\nNo jobs were found waiting to be submitted.", color="yellow")


# dandicompute queue prepare [OPTIONS]
@_queue_group.command(name="prepare")
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--pipeline",
    "pipeline_directory",
    help="Local path to the AIND pipeline repository.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--config",
    "config_key",
    help="Registered configuration key to use.",
    required=False,
    type=str,
    default="default",
)
@click.option(
    "--limit",
    "limit",
    help="Stop after preparing N assets in total. Useful for testing.",
    required=False,
    type=click.IntRange(min=1),
    default=None,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _queue_prepare_command(
    queue_directory: pathlib.Path,
    pipeline_directory: pathlib.Path | None = None,
    config_key: str = "default",
    limit: int | None = None,
    silent: bool = False,
) -> None:
    """Prepare queued jobs across configured pipelines without submitting them."""
    _configure_logging(silent=silent)
    if "DANDI_API_KEY" not in os.environ:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")

    QueueState.prepare(
        queue_directory=queue_directory,
        pipeline_directory=pipeline_directory,
        config_key=config_key,
        limit=limit,
    )


# dandicompute issues
@_dandicompute_group.group(name="issues")
def _issues_group() -> None:
    """Scan logs and write per-capsule and aggregate issue reports."""
    pass


# dandicompute issues dump [OPTIONS]
@_issues_group.command(name="dump")
@click.option(
    "--directory",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan for logs.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _issues_dump_command(
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
    silent: bool = False,
) -> None:
    """Scan nextflow and slurm logs and write per-capsule issue records."""
    _configure_logging(silent=silent)

    QueueState.dump_issues(dandiset_directory=dandiset_directory, queue_directory=queue_directory)
    if not silent:
        _styled_echo(text=f"\nWrote issue dump: {queue_directory / 'issues_dump.json'}", color="green")


# dandicompute issues summarize [OPTIONS]
@_issues_group.command(name="summarize")
@click.option(
    "--directory",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan for logs.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _issues_summarize_command(
    dandiset_directory: pathlib.Path,
    queue_directory: pathlib.Path,
    silent: bool = False,
) -> None:
    """Summarize discovered issue lines by descending occurrence count."""
    _configure_logging(silent=silent)

    QueueState.summarize_issues(dandiset_directory=dandiset_directory, queue_directory=queue_directory)
    if not silent:
        _styled_echo(text=f"\nWrote issue summary: {queue_directory / 'issues_summary.json'}", color="green")


# dandicompute delete
@_dandicompute_group.group(name="delete")
def _delete_group() -> None:
    """Delete remote and local derivatives for specific version patterns."""
    pass


# dandicompute delete version [OPTIONS]
@_delete_group.command(name="version")
@click.option(
    "--directory",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--version",
    "version",
    help=(
        "The base version string to delete (e.g., 'v1.0.0'). "
        "Matches the exact directory 'version-v1.0.0' as well as any hash-suffixed variants "
        "such as 'v1.0.0+fixes+20abeb6' or 'v1.1.2+abcd123+def4567'."
    ),
    required=True,
    type=str,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _delete_version_command(dandiset_directory: pathlib.Path, version: str, silent: bool = False) -> None:
    """Delete version-matching derivatives from the archive and local filesystem."""
    _configure_logging(silent=silent)
    if not os.environ.get("DANDI_API_KEY", "").strip():
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set or is blank.")
    version_dirs = scan_version_directories(dandiset_directory=dandiset_directory, version=version)
    if not version_dirs:
        if not silent:
            _styled_echo(text=f"\nNo 'version-{version}' directories found.", color="yellow")
        return

    count = len(version_dirs)
    noun = "directory" if count == 1 else "directories"
    examples = version_dirs[:3]
    example_lines = "\n".join(f"  {p}" for p in examples)
    suffix = f"\n  ... and {count - 3} more" if count > 3 else ""
    click.confirm(
        f"This will permanently delete {count} 'version-{version}' {noun} "
        f"from the DANDI archive and the local filesystem under '{dandiset_directory}'.\n"
        f"Directories to be deleted:\n{example_lines}{suffix}\n\nContinue?",
        abort=True,
    )
    deleted = delete_dandiset_version(dandiset_directory=dandiset_directory, version=version)
    if not silent:
        _styled_echo(text=f"\nDeleted {len(deleted)} version {noun}.", color="green")


# dandicompute archive [OPTIONS]
@_dandicompute_group.command(name="archive")
@click.option(
    "--status",
    "status",
    help="Archive every job capsule with this status. Mutually exclusive with --job.",
    required=False,
    type=click.Choice(["failed", "pending"]),
    default=None,
)
@click.option(
    "--job",
    "capsule_path",
    help="Path of a single job capsule folder (relative to the source Dandiset root) to archive directly. "
    "Mutually exclusive with --status.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--queue",
    "queue_directory",
    help="Path to the queue root directory (containing state.tsv). Required with --status.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--dandiset-id",
    "dandiset_id",
    help="Dandiset ID capsules are archived from.",
    required=False,
    type=str,
    default=_JOB_CAPSULES_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--archive-dandiset-id",
    "archive_dandiset_id",
    help="Dandiset ID capsules are archived to.",
    required=False,
    type=str,
    default=_FAILED_RUNS_ARCHIVE_DANDISET_ID,
    show_default=True,
)
@click.option(
    "--processing",
    "processing_directory",
    help="Directory for the temporary working tree (defaults to the system temporary location).",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--test",
    "test",
    help="Preserve the temporary working tree instead of cleaning it up.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress informational log output.",
    required=False,
    is_flag=True,
    default=False,
)
def _archive_command(
    status: str | None,
    capsule_path: str | None,
    queue_directory: pathlib.Path | None,
    dandiset_id: str,
    archive_dandiset_id: str,
    processing_directory: pathlib.Path | None = None,
    test: bool = False,
    silent: bool = False,
) -> None:
    """Archive one job capsule (--job) or every capsule with a --status."""
    if (status is None) == (capsule_path is None):
        message = "Provide exactly one of --status (failed|pending) or --job PATH."
        raise click.UsageError(message)

    _configure_logging(silent=silent)
    _require_dandi_api_key()
    _require_dandi_devel()

    if capsule_path is not None:
        move_job_capsule(
            capsule_path=capsule_path,
            source_dandiset_id=dandiset_id,
            target_dandiset_id=archive_dandiset_id,
            processing_directory=processing_directory,
            test=test,
        )
        if not silent:
            _styled_echo(text=f"\nArchived job capsule: {capsule_path}", color="green")
        return

    if queue_directory is None:
        message = "--queue is required when archiving by --status."
        raise click.UsageError(message)

    state = QueueState.from_tsv(queue_directory / "state.tsv")
    archived = state.archive_by_status(
        status=status,
        dandiset_id=dandiset_id,
        archive_dandiset_id=archive_dandiset_id,
        processing_directory=processing_directory,
        test=test,
    )

    if not silent:
        if archived:
            _styled_echo(text=f"\nArchived {len(archived)} {status} job capsule(s):", color="green")
            for capsule_path in archived:
                _styled_echo(text=f"  {capsule_path}", color="green")
        else:
            _styled_echo(text=f"\nNo {status} job capsules to archive.", color="yellow")
