import json
import os
import pathlib
import sys

import click

from ._utils import _styled_echo, clean_work_directory
from .aind_ephys_pipeline import prepare_aind_ephys_job, submit_aind_ephys_job
from .dandiset import (
    delete_dandiset_version,
    scan_dandiset_directory,
    scan_version_directories,
    write_state_and_waiting_jsonl,
)
from .queue import clean_unsubmitted_capsules, prepare_queue, prepare_test_queue, process_queue, refresh_waiting_queue


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
def _clean_command(directory: pathlib.Path) -> None:
    """Remove all files and directories under a work directory except apptainer cache."""
    clean_work_directory(directory=directory)
    _styled_echo(text="\nWork directory cleaned!", color="green")


# dandicompute aind
@_dandicompute_group.group(name="aind")
def _aind_group() -> None:
    """Prepare and submit AIND ephys pipeline jobs."""
    pass


# dandicompute aind prepare [OPTIONS]
@_aind_group.command(name="prepare")
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
    help="The local path to the Dandiset data to be processed. Required if --id is not provided.",
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
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--version",
    "pipeline_version",
    help="The version of the pipeline to use, which will be used to checkout a branch of the pipeline repository.",
    required=True,
    type=str,
)
@click.option(
    "--params",
    "parameters_key",
    help="The name of the parameters to use.",
    required=False,
    type=str,
    default="default",
)
@click.option(
    "--submit",
    help="Automatically submit the job.",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "--silent",
    help="Suppress output messages.",
    required=False,
    is_flag=True,
    default=False,
)
def _aind_prepare_command(
    pipeline_version: str,
    content_id: str | None = None,
    dandiset_id: str | None = None,
    dandiset_path: pathlib.Path | None = None,
    config_key: str = "default",
    pipeline_directory: pathlib.Path | None = None,
    parameters_key: str = "default",
    submit: bool = False,
    silent: bool = False,
) -> None:
    """Prepare an AIND ephys submission script, and optionally submit it."""
    if submit and "DANDI_API_KEY" not in os.environ:
        message = "`DANDI_API_KEY` environment variable is not set."
        raise RuntimeError(message)

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
        submit_aind_ephys_job(script_file_path=script_file_path)

    if silent:
        return

    _styled_echo(text="\nPreparation complete!", color="green")

    if submit and not silent:
        _styled_echo(text=f"\n\nProcessing script at: {script_file_path}\n\n", color="yellow")
        return

    _styled_echo(
        text=f"\n\nTo submit the job, run:\n\n\tdandicompute aind submit --script {script_file_path}\n\n",
        color="yellow",
    )


# dandicompute aind submit [OPTIONS]
@_aind_group.command(name="submit")
@click.option(
    "--script",
    "script_file_path",
    help="Path to the submission script file.",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
)
def _aind_submit_command(script_file_path: pathlib.Path) -> None:
    """Submit a previously prepared AIND ephys script."""
    submit_aind_ephys_job(script_file_path=script_file_path)


# dandicompute queue
@_dandicompute_group.group(name="queue")
def _queue_group() -> None:
    """Manage queue ordering, preparation, and execution."""
    pass


# dandicompute queue refresh [OPTIONS]
@_queue_group.command(name="refresh")
@click.option(
    "--queue-directory",
    "directory",
    help="Path to the queue root directory. Defaults to the current working directory.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--limit",
    "limit",
    help="Truncate waiting.jsonl to the first N entries. Useful for testing.",
    required=False,
    type=click.IntRange(min=1),
    default=None,
)
def _queue_refresh_command(directory: pathlib.Path | None = None, limit: int | None = None) -> None:
    """Regenerate waiting.jsonl from state.jsonl and optional queue limits."""
    cwd = directory if directory is not None else pathlib.Path.cwd()
    refresh_waiting_queue(cwd=cwd, limit=limit)


# dandicompute queue clean [OPTIONS]
@_queue_group.command(name="clean")
@click.option(
    "--queue-directory",
    "directory",
    help="Path to the queue root directory. Defaults to the current working directory.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--dandiset-directory",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan for queued capsules.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
def _queue_clean_command(
    directory: pathlib.Path | None = None,
    dandiset_directory: pathlib.Path = pathlib.Path("."),
) -> None:
    """Delete unsubmitted capsules that are no longer present in the queue."""
    cwd = directory if directory is not None else pathlib.Path.cwd()
    removed = clean_unsubmitted_capsules(dandiset_directory=dandiset_directory, queue_directory=cwd)
    if removed:
        for path in removed:
            _styled_echo(text=f"  Removed: {path}", color="yellow")
        noun = "capsule" if len(removed) == 1 else "capsules"
        _styled_echo(text=f"\nCleaned {len(removed)} unsubmitted {noun}.", color="green")
    else:
        _styled_echo(text="\nNo unsubmitted capsules found.", color="yellow")


# dandicompute prepare
@_dandicompute_group.group(name="prepare")
def _prepare_group() -> None:
    """Run preparation workflows that generate queue entries or scripts."""
    pass


# dandicompute prepare test [OPTIONS]
@_prepare_group.command(name="test")
@click.option(
    "--queue-directory",
    "directory",
    help="Path to the queue root directory containing queue_config.json. Defaults to the current working directory.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
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
def _prepare_test_command(
    directory: pathlib.Path | None = None,
    pipeline_directory: pathlib.Path | None = None,
    config_key: str = "default",
) -> None:
    """Prepare test queue entries for configured AIND ephys versions and params."""
    if "DANDI_API_KEY" not in os.environ:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")
    cwd = directory if directory is not None else pathlib.Path.cwd()
    prepare_test_queue(
        cwd=cwd,
        pipeline_directory=pipeline_directory,
        config_key=config_key,
    )


# dandicompute queue process [OPTIONS]
@_queue_group.command(name="process")
@click.option(
    "--queue-directory",
    "directory",
    help="Path to the queue root directory. Defaults to the current working directory.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--dandiset-directory",
    "dandiset_directory",
    help="Path to a local clone of the 001697 dandiset repository, used to count failures per dandiset.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
def _queue_process_command(
    directory: pathlib.Path | None = None,
    dandiset_directory: pathlib.Path = pathlib.Path("."),
) -> None:
    """Submit queued jobs when no active dandicompute jobs are running."""
    cwd = directory if directory is not None else pathlib.Path.cwd()
    process_queue(cwd=cwd, dandiset_directory=dandiset_directory)


# dandicompute queue prepare [OPTIONS]
@_queue_group.command(name="prepare")
@click.option(
    "--queue-directory",
    "directory",
    help="Path to the queue root directory. Defaults to the current working directory.",
    required=False,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
    default=None,
)
@click.option(
    "--dandiset-directory",
    "dandiset_directory",
    help="Path to a local clone of the 001697 dandiset repository, used to count failures per dandiset.",
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
def _queue_prepare_command(
    directory: pathlib.Path | None = None,
    dandiset_directory: pathlib.Path = pathlib.Path("."),
    pipeline_directory: pathlib.Path | None = None,
    config_key: str = "default",
    limit: int | None = None,
) -> None:
    """Prepare queued jobs across configured pipelines without submitting them."""
    if "DANDI_API_KEY" not in os.environ:
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set.")
    cwd = directory if directory is not None else pathlib.Path.cwd()
    prepare_queue(
        cwd=cwd,
        dandiset_directory=dandiset_directory,
        pipeline_directory=pipeline_directory,
        config_key=config_key,
        limit=limit,
    )


# dandicompute dandiset
@_dandicompute_group.group(name="dandiset")
def _dandiset_group() -> None:
    """Inspect dandiset derivatives and write queue state files."""
    pass


# dandicompute dandiset scan [OPTIONS]
@_dandiset_group.command(name="scan")
@click.option(
    "--directory",
    "dandiset_directory",
    help="Path to a local clone of the dandiset repository to scan.",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path),
)
@click.option(
    "--output",
    "output_file",
    help="Path to the output JSONL file. Defaults to stdout if not provided.",
    required=False,
    type=click.Path(dir_okay=False, path_type=pathlib.Path),
    default=None,
)
def _dandiset_scan_command(
    dandiset_directory: pathlib.Path,
    output_file: pathlib.Path | None = None,
) -> None:
    """Scan a local dandiset clone and emit attempt records as JSONL."""
    if output_file is None:
        records = scan_dandiset_directory(dandiset_directory=dandiset_directory)
        for record in records:
            sys.stdout.write(json.dumps(record) + "\n")
    else:
        if output_file.name == "state.jsonl":
            try:
                write_state_and_waiting_jsonl(
                    dandiset_directory=dandiset_directory,
                    queue_directory=output_file.parent,
                )
            except FileNotFoundError as error:
                message = (
                    f"{error} When writing to state.jsonl, ensure " "queue_config.json exists in the parent directory."
                )
                raise click.ClickException(message) from error
        else:
            records = scan_dandiset_directory(dandiset_directory=dandiset_directory)
            with output_file.open(mode="w") as file_stream:
                for record in records:
                    file_stream.write(json.dumps(record) + "\n")
        _styled_echo(text=f"\nScan complete! Output written to: {output_file}", color="green")


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
def _delete_version_command(dandiset_directory: pathlib.Path, version: str) -> None:
    """Delete version-matching derivatives from the archive and local filesystem."""
    if not os.environ.get("DANDI_API_KEY", "").strip():
        raise click.ClickException("`DANDI_API_KEY` environment variable is not set or is blank.")
    version_dirs = scan_version_directories(dandiset_directory=dandiset_directory, version=version)
    if not version_dirs:
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
    _styled_echo(text=f"\nDeleted {len(deleted)} version {noun}.", color="green")
